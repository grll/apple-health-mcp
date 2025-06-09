import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple
from zoneinfo import ZoneInfo
from collections import defaultdict

from lxml import etree  # type: ignore[import-untyped]
from sqlmodel import Session, SQLModel, create_engine, select, text
from tqdm import tqdm

from .models import (
    ActivitySummary,
    Audiogram,
    ClinicalRecord,
    Correlation,
    CorrelationRecord,
    EyePrescription,
    EyeSide,
    HealthData,
    HeartRateVariabilityMetadataList,
    InstantaneousBeatsPerMinute,
    MetadataEntry,
    Record,
    SensitivityPoint,
    VisionAttachment,
    VisionPrescription,
    Workout,
    WorkoutEvent,
    WorkoutRoute,
    WorkoutStatistics,
)


class OptimizedAppleHealthParser:
    """Optimized parser for Apple Health export XML files with bulk processing."""

    def __init__(self, db_path: str = "data/sqlite.db"):
        """Initialize parser with database connection."""
        # Create data directory if it doesn't exist
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        # Create database engine with optimizations
        self.engine = create_engine(
            f"sqlite:///{db_path}",
            echo=False,
            pool_pre_ping=True,
            connect_args={
                "timeout": 300,  # 5 minute timeout
                "check_same_thread": False,
            }
        )
        SQLModel.metadata.create_all(self.engine)

        # Create optimized indexes
        self._create_optimized_indexes()

        # Optimized batch processing settings
        self.batch_size = 10000  # 10x larger batches
        self.commit_frequency = 50000  # Commit every 50k records
        self.duplicate_check_batch_size = 5000  # Check duplicates in batches
        
        # Batch containers
        self.current_batch: List[Any] = []
        self.records_batch: List[Record] = []
        self.workouts_batch: List[Workout] = []
        self.correlations_batch: List[Correlation] = []
        self.activity_summaries_batch: List[ActivitySummary] = []
        
        # Duplicate tracking sets (in-memory for speed)
        self.existing_record_keys: Set[Tuple] = set()
        self.existing_workout_keys: Set[Tuple] = set()
        self.existing_correlation_keys: Set[Tuple] = set()
        self.existing_activity_summary_keys: Set[Tuple] = set()
        
        # Processing counters
        self.processed_count = 0
        self.last_commit_count = 0
        
        self.stats = {
            "records": 0,
            "workouts": 0,
            "correlations": 0,
            "activity_summaries": 0,
            "clinical_records": 0,
            "audiograms": 0,
            "vision_prescriptions": 0,
            "metadata_entries": 0,
            "hrv_lists": 0,
            "correlation_records": 0,
            "errors": 0,
            "duplicates": 0,
            "bulk_inserts": 0,
        }

    def _create_optimized_indexes(self) -> None:
        """Create optimized database indexes for duplicate checking and performance."""
        with Session(self.engine) as session:
            # Composite indexes for efficient duplicate checking
            indexes = [
                # Records
                "CREATE INDEX IF NOT EXISTS idx_record_duplicate ON record (type, start_date, end_date, health_data_id, value)",
                "CREATE INDEX IF NOT EXISTS idx_record_type_date ON record (type, start_date)",
                
                # Workouts
                "CREATE INDEX IF NOT EXISTS idx_workout_duplicate ON workout (workout_activity_type, start_date, end_date, health_data_id)",
                "CREATE INDEX IF NOT EXISTS idx_workout_type_date ON workout (workout_activity_type, start_date)",
                
                # Correlations
                "CREATE INDEX IF NOT EXISTS idx_correlation_duplicate ON correlation (type, start_date, end_date, health_data_id)",
                
                # Activity Summaries
                "CREATE INDEX IF NOT EXISTS idx_activity_summary_unique ON activitysummary (date_components, health_data_id)",
                
                # Foreign key performance
                "CREATE INDEX IF NOT EXISTS idx_metadata_parent ON metadataentry (parent_type, parent_id)",
                "CREATE INDEX IF NOT EXISTS idx_correlation_record_ids ON correlationrecord (correlation_id, record_id)",
            ]
            
            for index_sql in indexes:
                try:
                    session.exec(text(index_sql))
                except Exception as e:
                    print(f"Warning: Could not create index: {e}")
            
            session.commit()

    def _load_existing_keys(self, session: Session, health_data_id: int) -> None:
        """Load existing record keys into memory for fast duplicate checking."""
        print("Loading existing records for duplicate checking...")
        
        # Load existing record keys
        record_query = text("""
            SELECT type, start_date, end_date, COALESCE(value, '') as value
            FROM record 
            WHERE health_data_id = :health_data_id
        """)
        
        for row in session.execute(record_query, {"health_data_id": health_data_id}):
            key = (row.type, row.start_date, row.end_date, row.value)
            self.existing_record_keys.add(key)
        
        # Load existing workout keys
        workout_query = text("""
            SELECT workout_activity_type, start_date, end_date
            FROM workout 
            WHERE health_data_id = :health_data_id
        """)
        
        for row in session.execute(workout_query, {"health_data_id": health_data_id}):
            key = (row.workout_activity_type, row.start_date, row.end_date)
            self.existing_workout_keys.add(key)
        
        # Load existing correlation keys
        correlation_query = text("""
            SELECT type, start_date, end_date
            FROM correlation 
            WHERE health_data_id = :health_data_id
        """)
        
        for row in session.execute(correlation_query, {"health_data_id": health_data_id}):
            key = (row.type, row.start_date, row.end_date)
            self.existing_correlation_keys.add(key)
        
        # Load existing activity summary keys
        activity_query = text("""
            SELECT date_components
            FROM activitysummary 
            WHERE health_data_id = :health_data_id
        """)
        
        for row in session.execute(activity_query, {"health_data_id": health_data_id}):
            key = (row.date_components,)
            self.existing_activity_summary_keys.add(key)
        
        print(f"Loaded {len(self.existing_record_keys)} existing records")
        print(f"Loaded {len(self.existing_workout_keys)} existing workouts")
        print(f"Loaded {len(self.existing_correlation_keys)} existing correlations")
        print(f"Loaded {len(self.existing_activity_summary_keys)} existing activity summaries")

    def parse_file(self, xml_path: str) -> None:
        """Parse Apple Health export XML file using optimized streaming."""
        print(f"Starting optimized parsing: {xml_path}")

        # Check if file exists
        if not os.path.exists(xml_path):
            raise FileNotFoundError(f"XML file not found: {xml_path}")

        # Get file size for progress tracking
        file_size = os.path.getsize(xml_path)
        print(f"File size: {file_size / (1024**3):.2f} GB")
        
        # Configure SQLite for better performance
        with Session(self.engine) as session:
            # SQLite performance optimizations
            session.exec(text("PRAGMA journal_mode=WAL"))
            session.exec(text("PRAGMA synchronous=NORMAL"))
            session.exec(text("PRAGMA cache_size=1000000"))  # 1GB cache
            session.exec(text("PRAGMA temp_store=MEMORY"))
            session.exec(text("PRAGMA mmap_size=268435456"))  # 256MB mmap
            session.commit()
        
        # Clear events to free memory during parsing
        context = etree.iterparse(
            xml_path,
            events=("start", "end"),
            tag=None,  # Process all tags
            huge_tree=True,  # Enable parsing of large files
        )

        # Make iterator return start-end event pairs
        context = iter(context)

        # Skip to root element
        event, root = next(context)

        # Current element being processed
        health_data = None
        current_correlation = None
        current_workout = None
        current_audiogram = None
        current_vision_prescription = None
        current_record = None
        current_hrv_list = None

        # Track parent elements for metadata
        current_parent_type = None
        current_parent_id = None

        with Session(self.engine) as session:
            try:
                # Process root element first
                if root.tag == "HealthData":
                    # Check if HealthData already exists
                    existing_health_data = session.exec(select(HealthData)).first()
                    if existing_health_data:
                        health_data = existing_health_data
                        print(f"Using existing HealthData record with ID: {health_data.id}")
                    else:
                        health_data = self._parse_health_data(root)
                        session.add(health_data)
                        session.commit()
                        print(f"Created HealthData record with ID: {health_data.id}")

                # Load existing keys for duplicate checking
                if health_data and health_data.id:
                    self._load_existing_keys(session, health_data.id)

                # Create progress bar
                pbar = tqdm(desc="Processing", unit=" elements", miniters=5000)
                
                for event, elem in context:
                    if event == "start":
                        # Update progress bar
                        pbar.update(1)
                        self.processed_count += 1
                        
                        # Update description with current stats every 10000 records
                        if self.processed_count % 10000 == 0:
                            pbar.set_description(
                                f"Records: {self.stats['records']:,} | "
                                f"Duplicates: {self.stats['duplicates']:,} | "
                                f"Bulk Inserts: {self.stats['bulk_inserts']:,}"
                            )
                        
                        # Commit batches periodically
                        if self.processed_count - self.last_commit_count >= self.commit_frequency:
                            self._flush_all_batches(session)
                            self.last_commit_count = self.processed_count
                        
                        try:
                            if elem.tag == "HealthData" and not health_data:
                                # Check if HealthData already exists
                                existing_health_data = session.exec(select(HealthData)).first()
                                if existing_health_data:
                                    health_data = existing_health_data
                                else:
                                    health_data = self._parse_health_data(elem)
                                    session.add(health_data)
                                    session.commit()

                            elif elem.tag == "ExportDate" and health_data:
                                # Update health_data with export date
                                export_date_str = elem.get("value")
                                if export_date_str:
                                    health_data.export_date = self._parse_datetime(export_date_str)
                                    session.add(health_data)
                                    session.commit()

                            elif elem.tag == "Me" and health_data:
                                # Update health_data with personal info
                                health_data.date_of_birth = elem.get("HKCharacteristicTypeIdentifierDateOfBirth", "")
                                health_data.biological_sex = elem.get("HKCharacteristicTypeIdentifierBiologicalSex", "")
                                health_data.blood_type = elem.get("HKCharacteristicTypeIdentifierBloodType", "")
                                health_data.fitzpatrick_skin_type = elem.get("HKCharacteristicTypeIdentifierFitzpatrickSkinType", "")
                                health_data.cardio_fitness_medications_use = elem.get("HKCharacteristicTypeIdentifierCardioFitnessMedicationsUse", "")
                                session.add(health_data)
                                session.commit()

                            elif elem.tag == "Record" and health_data and health_data.id:
                                record = self._parse_record(elem, health_data.id)
                                
                                # Fast duplicate check using in-memory set
                                record_key = (record.type, record.start_date, record.end_date, record.value or "")
                                if record_key in self.existing_record_keys:
                                    self.stats["duplicates"] += 1
                                else:
                                    self.records_batch.append(record)
                                    self.existing_record_keys.add(record_key)
                                    self.stats["records"] += 1
                                    
                                    # Process batch when full
                                    if len(self.records_batch) >= self.batch_size:
                                        self._flush_records_batch(session)

                            elif elem.tag == "Workout" and health_data and health_data.id:
                                workout = self._parse_workout(elem, health_data.id)
                                
                                # Fast duplicate check
                                workout_key = (workout.workout_activity_type, workout.start_date, workout.end_date)
                                if workout_key in self.existing_workout_keys:
                                    self.stats["duplicates"] += 1
                                    current_workout = None  # Don't process child elements
                                else:
                                    self.workouts_batch.append(workout)
                                    self.existing_workout_keys.add(workout_key)
                                    current_workout = workout
                                    self.stats["workouts"] += 1
                                    
                                    # Process batch when full
                                    if len(self.workouts_batch) >= self.batch_size:
                                        self._flush_workouts_batch(session)

                            elif elem.tag == "Correlation" and health_data and health_data.id:
                                correlation = self._parse_correlation(elem, health_data.id)
                                
                                # Fast duplicate check
                                correlation_key = (correlation.type, correlation.start_date, correlation.end_date)
                                if correlation_key in self.existing_correlation_keys:
                                    self.stats["duplicates"] += 1
                                    current_correlation = None
                                else:
                                    self.correlations_batch.append(correlation)
                                    self.existing_correlation_keys.add(correlation_key)
                                    current_correlation = correlation
                                    self.stats["correlations"] += 1
                                    
                                    # Process batch when full
                                    if len(self.correlations_batch) >= self.batch_size:
                                        self._flush_correlations_batch(session)

                            elif elem.tag == "ActivitySummary" and health_data and health_data.id:
                                summary = self._parse_activity_summary(elem, health_data.id)
                                
                                # Fast duplicate check
                                summary_key = (summary.date_components,)
                                if summary_key in self.existing_activity_summary_keys:
                                    self.stats["duplicates"] += 1
                                else:
                                    self.activity_summaries_batch.append(summary)
                                    self.existing_activity_summary_keys.add(summary_key)
                                    self.stats["activity_summaries"] += 1
                                    
                                    # Process batch when full
                                    if len(self.activity_summaries_batch) >= self.batch_size:
                                        self._flush_activity_summaries_batch(session)

                            # Other elements processed with smaller batches for simplicity
                            elif elem.tag == "ClinicalRecord" and health_data and health_data.id:
                                clinical = self._parse_clinical_record(elem, health_data.id)
                                existing = self._check_duplicate_clinical_record(session, clinical)
                                if existing:
                                    self.stats["duplicates"] += 1
                                else:
                                    self._add_to_batch(session, clinical)
                                    self.stats["clinical_records"] += 1

                            elif elem.tag == "Audiogram" and health_data and health_data.id:
                                audiogram = self._parse_audiogram(elem, health_data.id)
                                existing = self._check_duplicate_audiogram(session, audiogram)
                                if existing:
                                    self.stats["duplicates"] += 1
                                    current_audiogram = existing
                                else:
                                    session.add(audiogram)
                                    session.commit()
                                    current_audiogram = audiogram
                                    self.stats["audiograms"] += 1

                            elif elem.tag == "VisionPrescription" and health_data and health_data.id:
                                prescription = self._parse_vision_prescription(elem, health_data.id)
                                existing = self._check_duplicate_vision_prescription(session, prescription)
                                if existing:
                                    self.stats["duplicates"] += 1
                                    current_vision_prescription = existing
                                else:
                                    session.add(prescription)
                                    session.commit()
                                    current_vision_prescription = prescription
                                    self.stats["vision_prescriptions"] += 1

                            elif (
                                elem.tag == "MetadataEntry"
                                and current_parent_type
                                and current_parent_id
                            ):
                                metadata = self._parse_metadata_entry(
                                    elem, current_parent_type, current_parent_id
                                )
                                self._add_to_batch(session, metadata)
                                self.stats["metadata_entries"] += 1

                            elif (
                                elem.tag == "HeartRateVariabilityMetadataList"
                                and current_record
                                and current_record.id
                            ):
                                current_hrv_list = self._parse_hrv_list(current_record.id)
                                session.add(current_hrv_list)
                                session.commit()  # Need ID for relationships
                                self.stats["hrv_lists"] += 1

                            # Handle nested elements (only if parent wasn't a duplicate)
                            elif elem.tag == "WorkoutEvent" and current_workout:
                                if hasattr(current_workout, 'id') and current_workout.id:
                                    event_obj = self._parse_workout_event(elem, current_workout.id)
                                    self._add_to_batch(session, event_obj)

                            elif elem.tag == "WorkoutStatistics" and current_workout:
                                if hasattr(current_workout, 'id') and current_workout.id:
                                    stat = self._parse_workout_statistics(elem, current_workout.id)
                                    self._add_to_batch(session, stat)

                            elif elem.tag == "WorkoutRoute" and current_workout:
                                if hasattr(current_workout, 'id') and current_workout.id:
                                    route = self._parse_workout_route(elem, current_workout.id)
                                    self._add_to_batch(session, route)

                            elif elem.tag == "SensitivityPoint" and current_audiogram and current_audiogram.id:
                                point = self._parse_sensitivity_point(elem, current_audiogram.id)
                                self._add_to_batch(session, point)

                            elif (
                                elem.tag == "Prescription"
                                and current_vision_prescription
                                and current_vision_prescription.id
                            ):
                                prescription = self._parse_eye_prescription(
                                    elem, current_vision_prescription.id
                                )
                                self._add_to_batch(session, prescription)

                            elif (
                                elem.tag == "Attachment" 
                                and current_vision_prescription
                                and current_vision_prescription.id
                            ):
                                attachment = self._parse_vision_attachment(
                                    elem, current_vision_prescription.id
                                )
                                self._add_to_batch(session, attachment)

                            elif (
                                elem.tag == "InstantaneousBeatsPerMinute"
                                and current_hrv_list
                                and current_hrv_list.id
                            ):
                                bpm = self._parse_instantaneous_bpm(elem, current_hrv_list.id)
                                self._add_to_batch(session, bpm)

                        except Exception as e:
                            self.stats["errors"] += 1
                            if self.stats["errors"] <= 10:  # Only print first 10 errors
                                print(f"Error parsing {elem.tag}: {e}")

                    elif event == "end":
                        # Clear completed elements
                        if elem.tag == "Correlation":
                            current_correlation = None
                            current_parent_type = None
                            current_parent_id = None
                        elif elem.tag == "Workout":
                            current_workout = None
                            current_parent_type = None
                            current_parent_id = None
                        elif elem.tag == "Audiogram":
                            current_audiogram = None
                        elif elem.tag == "VisionPrescription":
                            current_vision_prescription = None
                        elif elem.tag == "Record" and not current_correlation:
                            current_record = None
                            current_parent_type = None
                            current_parent_id = None
                        elif elem.tag == "HeartRateVariabilityMetadataList":
                            current_hrv_list = None

                        # Clear the element to free memory
                        elem.clear()
                        # Also remove preceding siblings
                        while elem.getprevious() is not None:
                            del elem.getparent()[0]

                # Flush any remaining batches
                self._flush_all_batches(session)
                pbar.close()

            except Exception as e:
                pbar.close()
                print(f"Fatal error during parsing: {e}")
                raise

        # Final statistics
        self._print_progress()
        print("Optimized parsing complete!")

    def _flush_records_batch(self, session: Session) -> None:
        """Flush records batch using bulk insert."""
        if self.records_batch:
            # Use bulk insert for better performance
            session.add_all(self.records_batch)
            session.commit()
            self.stats["bulk_inserts"] += 1
            self.records_batch = []

    def _flush_workouts_batch(self, session: Session) -> None:
        """Flush workouts batch using bulk insert."""
        if self.workouts_batch:
            session.add_all(self.workouts_batch)
            session.commit()
            self.stats["bulk_inserts"] += 1
            self.workouts_batch = []

    def _flush_correlations_batch(self, session: Session) -> None:
        """Flush correlations batch using bulk insert."""
        if self.correlations_batch:
            session.add_all(self.correlations_batch)
            session.commit()
            self.stats["bulk_inserts"] += 1
            self.correlations_batch = []

    def _flush_activity_summaries_batch(self, session: Session) -> None:
        """Flush activity summaries batch using bulk insert."""
        if self.activity_summaries_batch:
            session.add_all(self.activity_summaries_batch)
            session.commit()
            self.stats["bulk_inserts"] += 1
            self.activity_summaries_batch = []

    def _flush_all_batches(self, session: Session) -> None:
        """Flush all pending batches."""
        self._flush_records_batch(session)
        self._flush_workouts_batch(session)
        self._flush_correlations_batch(session)
        self._flush_activity_summaries_batch(session)
        self._flush_batch(session)  # Regular batch

    def _add_to_batch(self, session: Session, obj: Any) -> None:
        """Add object to batch and flush if necessary."""
        self.current_batch.append(obj)
        if len(self.current_batch) >= self.batch_size:
            self._flush_batch(session)

    def _flush_batch(self, session: Session) -> None:
        """Flush current batch to database."""
        if self.current_batch:
            session.add_all(self.current_batch)
            session.commit()
            self.stats["bulk_inserts"] += 1
            self.current_batch = []

    def _print_progress(self) -> None:
        """Print current parsing progress."""
        print(f"\nFinal Statistics:")
        for key, value in self.stats.items():
            print(f"  {key}: {value:,}")

    # Duplicate checking methods (keeping existing logic for less common items)
    def _check_duplicate_clinical_record(self, session: Session, record: ClinicalRecord) -> ClinicalRecord | None:
        """Check if a clinical record already exists."""
        return session.exec(
            select(ClinicalRecord).where(
                ClinicalRecord.identifier == record.identifier,
                ClinicalRecord.health_data_id == record.health_data_id,
            )
        ).first()

    def _check_duplicate_audiogram(self, session: Session, audiogram: Audiogram) -> Audiogram | None:
        """Check if an audiogram already exists."""
        return session.exec(
            select(Audiogram).where(
                Audiogram.type == audiogram.type,
                Audiogram.start_date == audiogram.start_date,
                Audiogram.end_date == audiogram.end_date,
                Audiogram.health_data_id == audiogram.health_data_id,
            )
        ).first()

    def _check_duplicate_vision_prescription(self, session: Session, prescription: VisionPrescription) -> VisionPrescription | None:
        """Check if a vision prescription already exists."""
        return session.exec(
            select(VisionPrescription).where(
                VisionPrescription.type == prescription.type,
                VisionPrescription.date_issued == prescription.date_issued,
                VisionPrescription.health_data_id == prescription.health_data_id,
            )
        ).first()

    # Parsing methods remain the same as the original parser
    def _parse_datetime(self, date_str: str) -> datetime:
        """Parse datetime string from Apple Health format."""
        # Apple Health uses format: "2023-12-31 23:59:59 +0000"
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S %z")
        # Convert to preferred timezone
        return dt.astimezone(ZoneInfo("Europe/Zurich"))

    def _parse_health_data(self, elem: Any) -> HealthData:
        """Parse HealthData root element."""
        return HealthData(
            locale=elem.get("locale", ""),
            export_date=datetime.now(ZoneInfo("Europe/Zurich")),
            date_of_birth="",
            biological_sex="",
            blood_type="",
            fitzpatrick_skin_type="",
            cardio_fitness_medications_use="",
        )

    def _parse_record(self, elem: Any, health_data_id: int) -> Record:
        """Parse Record element."""
        return Record(
            type=elem.get("type"),
            source_name=elem.get("sourceName"),
            source_version=elem.get("sourceVersion"),
            device=elem.get("device"),
            unit=elem.get("unit"),
            value=elem.get("value"),
            creation_date=self._parse_datetime(elem.get("creationDate"))
            if elem.get("creationDate")
            else None,
            start_date=self._parse_datetime(elem.get("startDate")),
            end_date=self._parse_datetime(elem.get("endDate")),
            health_data_id=health_data_id,
        )

    def _parse_correlation(self, elem: Any, health_data_id: int) -> Correlation:
        """Parse Correlation element."""
        return Correlation(
            type=elem.get("type"),
            source_name=elem.get("sourceName"),
            source_version=elem.get("sourceVersion"),
            device=elem.get("device"),
            creation_date=self._parse_datetime(elem.get("creationDate"))
            if elem.get("creationDate")
            else None,
            start_date=self._parse_datetime(elem.get("startDate")),
            end_date=self._parse_datetime(elem.get("endDate")),
            health_data_id=health_data_id,
        )

    def _parse_workout(self, elem: Any, health_data_id: int) -> Workout:
        """Parse Workout element."""
        return Workout(
            workout_activity_type=elem.get("workoutActivityType"),
            duration=float(elem.get("duration")) if elem.get("duration") else None,
            duration_unit=elem.get("durationUnit"),
            total_distance=float(elem.get("totalDistance"))
            if elem.get("totalDistance")
            else None,
            total_distance_unit=elem.get("totalDistanceUnit"),
            total_energy_burned=float(elem.get("totalEnergyBurned"))
            if elem.get("totalEnergyBurned")
            else None,
            total_energy_burned_unit=elem.get("totalEnergyBurnedUnit"),
            source_name=elem.get("sourceName"),
            source_version=elem.get("sourceVersion"),
            device=elem.get("device"),
            creation_date=self._parse_datetime(elem.get("creationDate"))
            if elem.get("creationDate")
            else None,
            start_date=self._parse_datetime(elem.get("startDate")),
            end_date=self._parse_datetime(elem.get("endDate")),
            health_data_id=health_data_id,
        )

    def _parse_activity_summary(self, elem: Any, health_data_id: int) -> ActivitySummary:
        """Parse ActivitySummary element."""
        return ActivitySummary(
            date_components=elem.get("dateComponents"),
            active_energy_burned=float(elem.get("activeEnergyBurned"))
            if elem.get("activeEnergyBurned")
            else None,
            active_energy_burned_goal=float(elem.get("activeEnergyBurnedGoal"))
            if elem.get("activeEnergyBurnedGoal")
            else None,
            active_energy_burned_unit=elem.get("activeEnergyBurnedUnit"),
            apple_move_time=float(elem.get("appleMoveTime"))
            if elem.get("appleMoveTime")
            else None,
            apple_move_time_goal=float(elem.get("appleMoveTimeGoal"))
            if elem.get("appleMoveTimeGoal")
            else None,
            apple_exercise_time=float(elem.get("appleExerciseTime"))
            if elem.get("appleExerciseTime")
            else None,
            apple_exercise_time_goal=float(elem.get("appleExerciseTimeGoal"))
            if elem.get("appleExerciseTimeGoal")
            else None,
            apple_stand_hours=int(elem.get("appleStandHours"))
            if elem.get("appleStandHours")
            else None,
            apple_stand_hours_goal=int(elem.get("appleStandHoursGoal"))
            if elem.get("appleStandHoursGoal")
            else None,
            health_data_id=health_data_id,
        )

    def _parse_clinical_record(self, elem: Any, health_data_id: int) -> ClinicalRecord:
        """Parse ClinicalRecord element."""
        return ClinicalRecord(
            type=elem.get("type"),
            identifier=elem.get("identifier"),
            source_name=elem.get("sourceName"),
            source_url=elem.get("sourceURL"),
            fhir_version=elem.get("fhirVersion"),
            received_date=self._parse_datetime(elem.get("receivedDate")),
            resource_file_path=elem.get("resourceFilePath"),
            health_data_id=health_data_id,
        )

    def _parse_audiogram(self, elem: Any, health_data_id: int) -> Audiogram:
        """Parse Audiogram element."""
        return Audiogram(
            type=elem.get("type"),
            source_name=elem.get("sourceName"),
            source_version=elem.get("sourceVersion"),
            device=elem.get("device"),
            creation_date=self._parse_datetime(elem.get("creationDate"))
            if elem.get("creationDate")
            else None,
            start_date=self._parse_datetime(elem.get("startDate")),
            end_date=self._parse_datetime(elem.get("endDate")),
            health_data_id=health_data_id,
        )

    def _parse_vision_prescription(self, elem: Any, health_data_id: int) -> VisionPrescription:
        """Parse VisionPrescription element."""
        return VisionPrescription(
            type=elem.get("type"),
            date_issued=self._parse_datetime(elem.get("dateIssued")),
            expiration_date=self._parse_datetime(elem.get("expirationDate"))
            if elem.get("expirationDate")
            else None,
            brand=elem.get("brand"),
            health_data_id=health_data_id,
        )

    def _parse_workout_event(self, elem: Any, workout_id: int) -> WorkoutEvent:
        """Parse WorkoutEvent element."""
        return WorkoutEvent(
            type=elem.get("type"),
            date=self._parse_datetime(elem.get("date")),
            duration=float(elem.get("duration")) if elem.get("duration") else None,
            duration_unit=elem.get("durationUnit"),
            workout_id=workout_id,
        )

    def _parse_workout_statistics(self, elem: Any, workout_id: int) -> WorkoutStatistics:
        """Parse WorkoutStatistics element."""
        return WorkoutStatistics(
            type=elem.get("type"),
            start_date=self._parse_datetime(elem.get("startDate")),
            end_date=self._parse_datetime(elem.get("endDate")),
            average=float(elem.get("average")) if elem.get("average") else None,
            minimum=float(elem.get("minimum")) if elem.get("minimum") else None,
            maximum=float(elem.get("maximum")) if elem.get("maximum") else None,
            sum=float(elem.get("sum")) if elem.get("sum") else None,
            unit=elem.get("unit"),
            workout_id=workout_id,
        )

    def _parse_workout_route(self, elem: Any, workout_id: int) -> WorkoutRoute:
        """Parse WorkoutRoute element."""
        return WorkoutRoute(
            source_name=elem.get("sourceName"),
            source_version=elem.get("sourceVersion"),
            device=elem.get("device"),
            creation_date=self._parse_datetime(elem.get("creationDate"))
            if elem.get("creationDate")
            else None,
            start_date=self._parse_datetime(elem.get("startDate")),
            end_date=self._parse_datetime(elem.get("endDate")),
            file_path=elem.get("filePath"),
            workout_id=workout_id,
        )

    def _parse_sensitivity_point(self, elem: Any, audiogram_id: int) -> SensitivityPoint:
        """Parse SensitivityPoint element."""
        return SensitivityPoint(
            frequency_value=float(elem.get("frequencyValue")),
            frequency_unit=elem.get("frequencyUnit"),
            left_ear_value=float(elem.get("leftEarValue"))
            if elem.get("leftEarValue")
            else None,
            left_ear_unit=elem.get("leftEarUnit"),
            left_ear_masked=elem.get("leftEarMasked") == "true"
            if elem.get("leftEarMasked")
            else None,
            left_ear_clamping_range_lower_bound=float(
                elem.get("leftEarClampingRangeLowerBound")
            )
            if elem.get("leftEarClampingRangeLowerBound")
            else None,
            left_ear_clamping_range_upper_bound=float(
                elem.get("leftEarClampingRangeUpperBound")
            )
            if elem.get("leftEarClampingRangeUpperBound")
            else None,
            right_ear_value=float(elem.get("rightEarValue"))
            if elem.get("rightEarValue")
            else None,
            right_ear_unit=elem.get("rightEarUnit"),
            right_ear_masked=elem.get("rightEarMasked") == "true"
            if elem.get("rightEarMasked")
            else None,
            right_ear_clamping_range_lower_bound=float(
                elem.get("rightEarClampingRangeLowerBound")
            )
            if elem.get("rightEarClampingRangeLowerBound")
            else None,
            right_ear_clamping_range_upper_bound=float(
                elem.get("rightEarClampingRangeUpperBound")
            )
            if elem.get("rightEarClampingRangeUpperBound")
            else None,
            audiogram_id=audiogram_id,
        )

    def _parse_eye_prescription(self, elem: Any, vision_prescription_id: int) -> EyePrescription:
        """Parse Prescription (eye) element."""
        eye_side = EyeSide.LEFT if elem.get("eye") == "left" else EyeSide.RIGHT

        return EyePrescription(
            eye_side=eye_side,
            sphere=float(elem.get("sphere")) if elem.get("sphere") else None,
            sphere_unit=elem.get("sphereUnit"),
            cylinder=float(elem.get("cylinder")) if elem.get("cylinder") else None,
            cylinder_unit=elem.get("cylinderUnit"),
            axis=float(elem.get("axis")) if elem.get("axis") else None,
            axis_unit=elem.get("axisUnit"),
            add=float(elem.get("add")) if elem.get("add") else None,
            add_unit=elem.get("addUnit"),
            vertex=float(elem.get("vertex")) if elem.get("vertex") else None,
            vertex_unit=elem.get("vertexUnit"),
            prism_amount=float(elem.get("prismAmount"))
            if elem.get("prismAmount")
            else None,
            prism_amount_unit=elem.get("prismAmountUnit"),
            prism_angle=float(elem.get("prismAngle"))
            if elem.get("prismAngle")
            else None,
            prism_angle_unit=elem.get("prismAngleUnit"),
            far_pd=float(elem.get("farPD")) if elem.get("farPD") else None,
            far_pd_unit=elem.get("farPDUnit"),
            near_pd=float(elem.get("nearPD")) if elem.get("nearPD") else None,
            near_pd_unit=elem.get("nearPDUnit"),
            base_curve=float(elem.get("baseCurve")) if elem.get("baseCurve") else None,
            base_curve_unit=elem.get("baseCurveUnit"),
            diameter=float(elem.get("diameter")) if elem.get("diameter") else None,
            diameter_unit=elem.get("diameterUnit"),
            vision_prescription_id=vision_prescription_id,
        )

    def _parse_vision_attachment(self, elem: Any, vision_prescription_id: int) -> VisionAttachment:
        """Parse Attachment element."""
        return VisionAttachment(
            identifier=elem.get("identifier"),
            vision_prescription_id=vision_prescription_id,
        )

    def _parse_metadata_entry(self, elem: Any, parent_type: str, parent_id: int) -> MetadataEntry:
        """Parse MetadataEntry element."""
        return MetadataEntry(
            key=elem.get("key"),
            value=elem.get("value"),
            parent_type=parent_type,
            parent_id=parent_id,
        )

    def _parse_hrv_list(self, record_id: int) -> HeartRateVariabilityMetadataList:
        """Parse HeartRateVariabilityMetadataList element."""
        return HeartRateVariabilityMetadataList(record_id=record_id)

    def _parse_instantaneous_bpm(self, elem: Any, hrv_list_id: int) -> InstantaneousBeatsPerMinute:
        """Parse InstantaneousBeatsPerMinute element."""
        return InstantaneousBeatsPerMinute(
            bpm=int(elem.get("bpm")),
            time=self._parse_datetime(elem.get("time")),
            hrv_list_id=hrv_list_id,
        )


if __name__ == "__main__":
    # Example usage
    parser = OptimizedAppleHealthParser()
    parser.parse_file("data/export/apple_health_export/export.xml")