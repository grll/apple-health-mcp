import os
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from lxml import etree  # type: ignore[import-untyped]
from sqlmodel import Session, SQLModel, create_engine
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


class AppleHealthParser:
    """Parser for Apple Health export XML files with streaming support."""

    def __init__(self, db_path: str = "data/sqlite.db"):
        """Initialize parser with database connection."""
        # Create data directory if it doesn't exist
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        # Create database engine
        self.engine = create_engine(f"sqlite:///{db_path}")
        SQLModel.metadata.create_all(self.engine)

        # Batch processing settings
        self.batch_size = 1000
        self.current_batch: list[Any] = []
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
        }

    def parse_file(self, xml_path: str) -> None:
        """Parse Apple Health export XML file using streaming."""
        print(f"Starting to parse: {xml_path}")

        # Check if file exists
        if not os.path.exists(xml_path):
            raise FileNotFoundError(f"XML file not found: {xml_path}")

        # Get file size for progress tracking
        file_size = os.path.getsize(xml_path)
        print(f"File size: {file_size / (1024**3):.2f} GB")
        
        # For large files, we'll use a file position based progress bar
        # rather than counting all elements first (which would take too long)

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
                        print(
                            f"Using existing HealthData record with ID: {health_data.id}"
                        )
                    else:
                        health_data = self._parse_health_data(root)
                        session.add(health_data)
                        session.commit()
                        print(f"Created HealthData record with ID: {health_data.id}")

                # Create progress bar based on number of elements processed
                with tqdm(desc="Processing", unit=" elements") as pbar:
                    for event, elem in context:
                        if event == "start":
                            try:
                            if elem.tag == "HealthData" and not health_data:
                                # Check if HealthData already exists
                                existing_health_data = session.exec(
                                    select(HealthData)
                                ).first()
                                if existing_health_data:
                                    health_data = existing_health_data
                                else:
                                    health_data = self._parse_health_data(elem)
                                    session.add(health_data)
                                    session.commit()  # Commit to get ID

                            elif elem.tag == "ExportDate" and health_data:
                                # Update health_data with export date
                                export_date_str = elem.get("value")
                                if export_date_str:
                                    health_data.export_date = self._parse_datetime(
                                        export_date_str
                                    )
                                    session.add(health_data)
                                    session.commit()

                            elif elem.tag == "Me" and health_data:
                                # Update health_data with personal info
                                health_data.date_of_birth = elem.get(
                                    "HKCharacteristicTypeIdentifierDateOfBirth", ""
                                )
                                health_data.biological_sex = elem.get(
                                    "HKCharacteristicTypeIdentifierBiologicalSex", ""
                                )
                                health_data.blood_type = elem.get(
                                    "HKCharacteristicTypeIdentifierBloodType", ""
                                )
                                health_data.fitzpatrick_skin_type = elem.get(
                                    "HKCharacteristicTypeIdentifierFitzpatrickSkinType",
                                    "",
                                )
                                health_data.cardio_fitness_medications_use = elem.get(
                                    "HKCharacteristicTypeIdentifierCardioFitnessMedicationsUse",
                                    "",
                                )
                                session.add(health_data)
                                session.commit()

                            elif (
                                elem.tag == "Record" and health_data and health_data.id
                            ):
                                # Check if inside a correlation
                                if current_correlation and current_correlation.id:
                                    # Parse record but don't add to batch yet
                                    record = self._parse_record(elem, health_data.id)
                                    existing_record = self._get_existing_record(
                                        session, record
                                    )

                                    if existing_record:
                                        record = existing_record
                                        self.stats["duplicates"] += 1
                                    else:
                                        session.add(record)
                                        session.commit()  # Need ID for relationship
                                        self.stats["records"] += 1

                                    # Create correlation-record link if not exists
                                    if (
                                        record.id
                                        and not self._correlation_record_exists(
                                            session, current_correlation.id, record.id
                                        )
                                    ):
                                        link = CorrelationRecord(
                                            correlation_id=current_correlation.id,
                                            record_id=record.id,
                                        )
                                        self._add_to_batch(session, link)
                                        self.stats["correlation_records"] += 1
                                else:
                                    record = self._parse_record(elem, health_data.id)
                                    existing_record = self._get_existing_record(
                                        session, record
                                    )

                                    if existing_record:
                                        record = existing_record
                                        self.stats["duplicates"] += 1
                                    else:
                                        session.add(record)
                                        session.commit()  # Need ID for potential metadata
                                        self.stats["records"] += 1

                                    current_record = record
                                    current_parent_type = "record"
                                    current_parent_id = record.id

                            elif (
                                elem.tag == "Correlation"
                                and health_data
                                and health_data.id
                            ):
                                current_correlation = self._parse_correlation(
                                    elem, health_data.id
                                )
                                existing_correlation = self._get_existing_correlation(
                                    session, current_correlation
                                )

                                if existing_correlation:
                                    current_correlation = existing_correlation
                                    self.stats["duplicates"] += 1
                                else:
                                    session.add(current_correlation)
                                    session.commit()  # Need ID for relationships
                                    self.stats["correlations"] += 1

                                current_parent_type = "correlation"
                                current_parent_id = current_correlation.id

                            elif (
                                elem.tag == "Workout" and health_data and health_data.id
                            ):
                                current_workout = self._parse_workout(
                                    elem, health_data.id
                                )
                                existing_workout = self._get_existing_workout(
                                    session, current_workout
                                )

                                if existing_workout:
                                    current_workout = existing_workout
                                    self.stats["duplicates"] += 1
                                else:
                                    session.add(current_workout)
                                    session.commit()  # Need ID for relationships
                                    self.stats["workouts"] += 1

                                current_parent_type = "workout"
                                current_parent_id = current_workout.id

                            elif (
                                elem.tag == "ActivitySummary"
                                and health_data
                                and health_data.id
                            ):
                                summary = self._parse_activity_summary(
                                    elem, health_data.id
                                )
                                # Check for existing activity summary by date_components
                                existing_summary = self._get_existing_activity_summary(
                                    session, summary
                                )

                                if existing_summary:
                                    self.stats["duplicates"] += 1
                                else:
                                    self._add_to_batch(session, summary)
                                    self.stats["activity_summaries"] += 1

                            elif (
                                elem.tag == "ClinicalRecord"
                                and health_data
                                and health_data.id
                            ):
                                clinical = self._parse_clinical_record(
                                    elem, health_data.id
                                )
                                # Check for existing clinical record by identifier
                                existing_clinical = self._get_existing_clinical_record(
                                    session, clinical
                                )

                                if existing_clinical:
                                    self.stats["duplicates"] += 1
                                else:
                                    self._add_to_batch(session, clinical)
                                    self.stats["clinical_records"] += 1

                            elif (
                                elem.tag == "Audiogram"
                                and health_data
                                and health_data.id
                            ):
                                current_audiogram = self._parse_audiogram(
                                    elem, health_data.id
                                )
                                existing_audiogram = self._get_existing_audiogram(
                                    session, current_audiogram
                                )

                                if existing_audiogram:
                                    current_audiogram = existing_audiogram
                                    self.stats["duplicates"] += 1
                                else:
                                    session.add(current_audiogram)
                                    session.commit()  # Need ID for relationships
                                    self.stats["audiograms"] += 1

                            elif (
                                elem.tag == "VisionPrescription"
                                and health_data
                                and health_data.id
                            ):
                                current_vision_prescription = (
                                    self._parse_vision_prescription(
                                        elem, health_data.id
                                    )
                                )
                                existing_vision = (
                                    self._get_existing_vision_prescription(
                                        session, current_vision_prescription
                                    )
                                )

                                if existing_vision:
                                    current_vision_prescription = existing_vision
                                    self.stats["duplicates"] += 1
                                else:
                                    session.add(current_vision_prescription)
                                    session.commit()  # Need ID for relationships
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
                                current_hrv_list = self._parse_hrv_list(
                                    current_record.id
                                )
                                session.add(current_hrv_list)
                                session.commit()  # Need ID for relationships
                                self.stats["hrv_lists"] += 1

                            # Handle nested elements
                            elif (
                                elem.tag == "WorkoutEvent"
                                and current_workout
                                and current_workout.id
                            ):
                                event_obj = self._parse_workout_event(
                                    elem, current_workout.id
                                )
                                self._add_to_batch(session, event_obj)

                            elif (
                                elem.tag == "WorkoutStatistics"
                                and current_workout
                                and current_workout.id
                            ):
                                stat = self._parse_workout_statistics(
                                    elem, current_workout.id
                                )
                                self._add_to_batch(session, stat)

                            elif (
                                elem.tag == "WorkoutRoute"
                                and current_workout
                                and current_workout.id
                            ):
                                route = self._parse_workout_route(
                                    elem, current_workout.id
                                )
                                self._add_to_batch(session, route)

                            elif (
                                elem.tag == "SensitivityPoint"
                                and current_audiogram
                                and current_audiogram.id
                            ):
                                point = self._parse_sensitivity_point(
                                    elem, current_audiogram.id
                                )
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
                                bpm = self._parse_instantaneous_bpm(
                                    elem, current_hrv_list.id
                                )
                                self._add_to_batch(session, bpm)

                        except Exception as e:
                            self.stats["errors"] += 1
                            print(f"Error parsing {elem.tag}: {e}")
                            # Continue processing

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

                    # Print progress every 5000 records
                    if self.stats["records"] % 5000 == 0 and self.stats["records"] > 0:
                        self._print_progress()

                # Flush any remaining batch
                self._flush_batch(session)

            except Exception as e:
                print(f"Fatal error during parsing: {e}")
                raise

        # Final statistics
        self._print_progress()
        print("Parsing complete!")

    def _add_to_batch(self, session: Session, obj: Any) -> None:
        """Add object to batch and flush if necessary."""
        self.current_batch.append(obj)
        if len(self.current_batch) >= self.batch_size:
            self._flush_batch(session)

    def _flush_batch(self, session: Session) -> None:
        """Flush current batch to database with duplicate checking."""
        if self.current_batch:
            # Group objects by type for efficient duplicate checking
            objects_to_add = []

            for obj in self.current_batch:
                # Skip duplicate checking for certain types that are already checked
                if isinstance(
                    obj,
                    (
                        MetadataEntry,
                        WorkoutEvent,
                        WorkoutStatistics,
                        WorkoutRoute,
                        SensitivityPoint,
                        EyePrescription,
                        VisionAttachment,
                        InstantaneousBeatsPerMinute,
                    ),
                ):
                    objects_to_add.append(obj)
                else:
                    # For other types, they should already be checked before batching
                    objects_to_add.append(obj)

            if objects_to_add:
                session.add_all(objects_to_add)
                session.commit()

            self.current_batch = []

    def _print_progress(self) -> None:
        """Print current parsing progress."""
        total_processed = sum(
            [
                self.stats["records"],
                self.stats["workouts"],
                self.stats["correlations"],
                self.stats["activity_summaries"],
                self.stats["clinical_records"],
                self.stats["audiograms"],
                self.stats["vision_prescriptions"],
            ]
        )
        print(
            f"Progress - Total: {total_processed:,} | Duplicates: {self.stats['duplicates']:,} | {self.stats}"
        )

    def _parse_datetime(self, date_str: str) -> datetime:
        """Parse datetime string from Apple Health format."""
        # Apple Health uses format: "2023-12-31 23:59:59 +0000"
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S %z")
        # Convert to preferred timezone
        return dt.astimezone(ZoneInfo("Europe/Zurich"))

    def _parse_health_data(self, elem: Any) -> HealthData:
        """Parse HealthData root element."""
        # Default values
        export_date = datetime.now(ZoneInfo("Europe/Zurich"))

        return HealthData(
            locale=elem.get("locale", ""),
            export_date=export_date,
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

    def _parse_activity_summary(
        self, elem: Any, health_data_id: int
    ) -> ActivitySummary:
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

    def _parse_vision_prescription(
        self, elem: Any, health_data_id: int
    ) -> VisionPrescription:
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

    def _parse_workout_statistics(
        self, elem: Any, workout_id: int
    ) -> WorkoutStatistics:
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

    def _parse_sensitivity_point(
        self, elem: Any, audiogram_id: int
    ) -> SensitivityPoint:
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

    def _parse_eye_prescription(
        self, elem: Any, vision_prescription_id: int
    ) -> EyePrescription:
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

    def _parse_vision_attachment(
        self, elem: Any, vision_prescription_id: int
    ) -> VisionAttachment:
        """Parse Attachment element."""
        return VisionAttachment(
            identifier=elem.get("identifier"),
            vision_prescription_id=vision_prescription_id,
        )

    def _parse_metadata_entry(
        self, elem: Any, parent_type: str, parent_id: int
    ) -> MetadataEntry:
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

    def _parse_instantaneous_bpm(
        self, elem: Any, hrv_list_id: int
    ) -> InstantaneousBeatsPerMinute:
        """Parse InstantaneousBeatsPerMinute element."""
        return InstantaneousBeatsPerMinute(
            bpm=int(elem.get("bpm")),
            time=self._parse_datetime(elem.get("time")),
            hrv_list_id=hrv_list_id,
        )

    def _get_existing_record(self, session: Session, record: Record) -> Record | None:
        """Check if a record already exists based on type, dates, and value."""
        # Build query to check for existing record
        stmt = select(Record).where(
            Record.type == record.type,
            Record.start_date == record.start_date,
            Record.end_date == record.end_date,
            Record.health_data_id == record.health_data_id,
        )

        # Add value comparison - handle None values
        if record.value is not None:
            stmt = stmt.where(Record.value == record.value)
        else:
            stmt = stmt.where(Record.value == None)

        # Execute query
        result = session.exec(stmt).first()
        return result

    def _correlation_record_exists(
        self, session: Session, correlation_id: int, record_id: int
    ) -> bool:
        """Check if a correlation-record link already exists."""
        stmt = select(CorrelationRecord).where(
            CorrelationRecord.correlation_id == correlation_id,
            CorrelationRecord.record_id == record_id,
        )
        result = session.exec(stmt).first()
        return result is not None

    def _get_existing_correlation(
        self, session: Session, correlation: Correlation
    ) -> Correlation | None:
        """Check if a correlation already exists based on type and dates."""
        stmt = select(Correlation).where(
            Correlation.type == correlation.type,
            Correlation.start_date == correlation.start_date,
            Correlation.end_date == correlation.end_date,
            Correlation.health_data_id == correlation.health_data_id,
        )
        result = session.exec(stmt).first()
        return result

    def _get_existing_workout(
        self, session: Session, workout: Workout
    ) -> Workout | None:
        """Check if a workout already exists based on type and dates."""
        stmt = select(Workout).where(
            Workout.workout_activity_type == workout.workout_activity_type,
            Workout.start_date == workout.start_date,
            Workout.end_date == workout.end_date,
            Workout.health_data_id == workout.health_data_id,
        )
        result = session.exec(stmt).first()
        return result

    def _get_existing_activity_summary(
        self, session: Session, summary: ActivitySummary
    ) -> ActivitySummary | None:
        """Check if an activity summary already exists for the given date."""
        stmt = select(ActivitySummary).where(
            ActivitySummary.date_components == summary.date_components,
            ActivitySummary.health_data_id == summary.health_data_id,
        )
        result = session.exec(stmt).first()
        return result

    def _get_existing_clinical_record(
        self, session: Session, clinical: ClinicalRecord
    ) -> ClinicalRecord | None:
        """Check if a clinical record already exists based on identifier."""
        stmt = select(ClinicalRecord).where(
            ClinicalRecord.identifier == clinical.identifier,
            ClinicalRecord.health_data_id == clinical.health_data_id,
        )
        result = session.exec(stmt).first()
        return result

    def _get_existing_audiogram(
        self, session: Session, audiogram: Audiogram
    ) -> Audiogram | None:
        """Check if an audiogram already exists based on type and dates."""
        stmt = select(Audiogram).where(
            Audiogram.type == audiogram.type,
            Audiogram.start_date == audiogram.start_date,
            Audiogram.end_date == audiogram.end_date,
            Audiogram.health_data_id == audiogram.health_data_id,
        )
        result = session.exec(stmt).first()
        return result

    def _get_existing_vision_prescription(
        self, session: Session, vision: VisionPrescription
    ) -> VisionPrescription | None:
        """Check if a vision prescription already exists based on date issued."""
        stmt = select(VisionPrescription).where(
            VisionPrescription.type == vision.type,
            VisionPrescription.date_issued == vision.date_issued,
            VisionPrescription.health_data_id == vision.health_data_id,
        )
        result = session.exec(stmt).first()
        return result


if __name__ == "__main__":
    # Example usage
    parser = AppleHealthParser()
    parser.parse_file("data/export/apple_health_export/export.xml")
