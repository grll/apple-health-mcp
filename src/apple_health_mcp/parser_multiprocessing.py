"""
Multiprocessing Apple Health Parser for maximum performance.
"""
import multiprocessing as mp
import os
import queue
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple, Optional
from zoneinfo import ZoneInfo
import tempfile
import mmap

from lxml import etree
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


class ProcessingResult:
    """Results from processing a chunk."""
    
    def __init__(self, chunk_id: int):
        self.chunk_id = chunk_id
        self.records: List[Record] = []
        self.workouts: List[Workout] = []
        self.correlations: List[Correlation] = []
        self.activity_summaries: List[ActivitySummary] = []
        self.metadata_entries: List[MetadataEntry] = []
        self.stats = defaultdict(int)
        self.errors: List[str] = []


def extract_elements_from_xml(xml_path: str, num_chunks: int) -> List[List[str]]:
    """Extract XML elements and distribute them among chunks."""
    print("Extracting XML elements for parallel processing...")
    
    elements_by_type = {
        'Record': [],
        'Workout': [],
        'Correlation': [],
        'ActivitySummary': []
    }
    
    # Parse XML and extract elements as strings
    context = etree.iterparse(xml_path, events=("start", "end"), huge_tree=True)
    context = iter(context)
    
    # Skip to root
    event, root = next(context)
    
    for event, elem in context:
        if event == "end":
            if elem.tag in elements_by_type:
                # Convert element to string
                elem_str = etree.tostring(elem, encoding='unicode')
                elements_by_type[elem.tag].append(elem_str)
                
                # Clear element to free memory
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]
    
    # Distribute elements among chunks
    chunks = [[] for _ in range(num_chunks)]
    
    # Round-robin distribution
    chunk_idx = 0
    for element_type, elements in elements_by_type.items():
        for element in elements:
            chunks[chunk_idx].append(element)
            chunk_idx = (chunk_idx + 1) % num_chunks
    
    print(f"Distributed {sum(len(elements) for elements in elements_by_type.values())} elements among {num_chunks} chunks")
    return chunks


def process_element_chunk(chunk_id: int, element_strings: List[str], health_data_id: int, existing_keys: Dict[str, Set]) -> ProcessingResult:
    """Process a chunk of XML element strings in a separate process."""
    result = ProcessingResult(chunk_id)
    
    for elem_str in element_strings:
        try:
            # Parse individual element
            elem = etree.fromstring(elem_str)
            
            if elem.tag == "Record":
                record = _parse_record_element(elem, health_data_id)
                if record:
                    # Check for duplicates using shared keys
                    record_key = (record.type, str(record.start_date), str(record.end_date), record.value or "")
                    if record_key not in existing_keys.get('records', {}):
                        result.records.append(record)
                        existing_keys['records'][record_key] = True
                    else:
                        result.stats['duplicates'] += 1
                    
                    result.stats['records'] += 1
            
            elif elem.tag == "Workout":
                workout = _parse_workout_element(elem, health_data_id)
                if workout:
                    # Check for duplicates
                    workout_key = (workout.workout_activity_type, str(workout.start_date), str(workout.end_date))
                    if workout_key not in existing_keys.get('workouts', {}):
                        result.workouts.append(workout)
                        existing_keys['workouts'][workout_key] = True
                    else:
                        result.stats['duplicates'] += 1
                    
                    result.stats['workouts'] += 1
            
            elif elem.tag == "Correlation":
                correlation = _parse_correlation_element(elem, health_data_id)
                if correlation:
                    # Check for duplicates
                    correlation_key = (correlation.type, str(correlation.start_date), str(correlation.end_date))
                    if correlation_key not in existing_keys.get('correlations', {}):
                        result.correlations.append(correlation)
                        existing_keys['correlations'][correlation_key] = True
                    else:
                        result.stats['duplicates'] += 1
                    
                    result.stats['correlations'] += 1
            
            elif elem.tag == "ActivitySummary":
                activity_summary = _parse_activity_summary_element(elem, health_data_id)
                if activity_summary:
                    result.activity_summaries.append(activity_summary)
                    result.stats['activity_summaries'] += 1
                
        except Exception as e:
            result.errors.append(f"Error processing element: {str(e)}")
            result.stats['errors'] += 1
    
    return result


def _parse_datetime(date_str: str) -> datetime:
    """Parse datetime string from Apple Health format."""
    if not date_str:
        return None
    # Apple Health uses format: "2023-12-31 23:59:59 +0000"
    dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S %z")
    # Convert to preferred timezone
    return dt.astimezone(ZoneInfo("Europe/Zurich"))


def _parse_record_element(elem, health_data_id: int) -> Optional[Record]:
    """Parse a Record element."""
    try:
        return Record(
            type=elem.get("type", ""),
            source_name=elem.get("sourceName", ""),
            source_version=elem.get("sourceVersion", ""),
            device=elem.get("device"),
            unit=elem.get("unit", ""),
            value=elem.get("value"),
            creation_date=_parse_datetime(elem.get("creationDate")) if elem.get("creationDate") else None,
            start_date=_parse_datetime(elem.get("startDate")),
            end_date=_parse_datetime(elem.get("endDate")),
            health_data_id=health_data_id
        )
    except Exception:
        return None


def _parse_workout_element(elem, health_data_id: int) -> Optional[Workout]:
    """Parse a Workout element."""
    try:
        return Workout(
            workout_activity_type=elem.get("workoutActivityType", ""),
            duration=float(elem.get("duration", 0)) if elem.get("duration") else None,
            duration_unit=elem.get("durationUnit", ""),
            total_distance=float(elem.get("totalDistance", 0)) if elem.get("totalDistance") else None,
            total_distance_unit=elem.get("totalDistanceUnit", ""),
            total_energy_burned=float(elem.get("totalEnergyBurned", 0)) if elem.get("totalEnergyBurned") else None,
            total_energy_burned_unit=elem.get("totalEnergyBurnedUnit", ""),
            source_name=elem.get("sourceName", ""),
            source_version=elem.get("sourceVersion", ""),
            device=elem.get("device"),
            creation_date=_parse_datetime(elem.get("creationDate")) if elem.get("creationDate") else None,
            start_date=_parse_datetime(elem.get("startDate")),
            end_date=_parse_datetime(elem.get("endDate")),
            health_data_id=health_data_id
        )
    except Exception:
        return None


def _parse_correlation_element(elem, health_data_id: int) -> Optional[Correlation]:
    """Parse a Correlation element."""
    try:
        return Correlation(
            type=elem.get("type", ""),
            source_name=elem.get("sourceName", ""),
            source_version=elem.get("sourceVersion", ""),
            device=elem.get("device"),
            creation_date=_parse_datetime(elem.get("creationDate")) if elem.get("creationDate") else None,
            start_date=_parse_datetime(elem.get("startDate")),
            end_date=_parse_datetime(elem.get("endDate")),
            health_data_id=health_data_id
        )
    except Exception:
        return None


def _parse_activity_summary_element(elem, health_data_id: int) -> Optional[ActivitySummary]:
    """Parse an ActivitySummary element."""
    try:
        return ActivitySummary(
            date_components=elem.get("dateComponents", ""),
            active_energy_burned=float(elem.get("activeEnergyBurned", 0)) if elem.get("activeEnergyBurned") else None,
            active_energy_burned_goal=float(elem.get("activeEnergyBurnedGoal", 0)) if elem.get("activeEnergyBurnedGoal") else None,
            active_energy_burned_unit=elem.get("activeEnergyBurnedUnit", ""),
            apple_exercise_time=float(elem.get("appleExerciseTime", 0)) if elem.get("appleExerciseTime") else None,
            apple_exercise_time_goal=float(elem.get("appleExerciseTimeGoal", 0)) if elem.get("appleExerciseTimeGoal") else None,
            apple_stand_hours=float(elem.get("appleStandHours", 0)) if elem.get("appleStandHours") else None,
            apple_stand_hours_goal=float(elem.get("appleStandHoursGoal", 0)) if elem.get("appleStandHoursGoal") else None,
            health_data_id=health_data_id
        )
    except Exception:
        return None


class MultiprocessingAppleHealthParser:
    """High-performance multiprocessing Apple Health parser."""
    
    def __init__(self, db_path: str = "data/sqlite_multiprocessing.db", num_processes: int = None):
        """Initialize multiprocessing parser."""
        # Create data directory if it doesn't exist
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        self.db_path = db_path
        self.num_processes = num_processes or mp.cpu_count()
        
        # Create database engine
        self.engine = create_engine(
            f"sqlite:///{db_path}",
            echo=False,
            pool_pre_ping=True,
            connect_args={
                "timeout": 300,
                "check_same_thread": False,
            }
        )
        SQLModel.metadata.create_all(self.engine)
        
        # Create optimized indexes
        self._create_optimized_indexes()
        
        # Statistics
        self.stats = defaultdict(int)
        
        print(f"Initialized multiprocessing parser with {self.num_processes} processes")
    
    def _create_optimized_indexes(self) -> None:
        """Create optimized database indexes."""
        with Session(self.engine) as session:
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_record_duplicate ON record (type, start_date, end_date, health_data_id, value)",
                "CREATE INDEX IF NOT EXISTS idx_workout_duplicate ON workout (workout_activity_type, start_date, end_date, health_data_id)",
                "CREATE INDEX IF NOT EXISTS idx_correlation_duplicate ON correlation (type, start_date, end_date, health_data_id)",
                "CREATE INDEX IF NOT EXISTS idx_activity_summary_unique ON activitysummary (date_components, health_data_id)",
            ]
            
            for index_sql in indexes:
                try:
                    session.exec(text(index_sql))
                except Exception as e:
                    print(f"Warning: Could not create index: {e}")
            
            session.commit()
    
    def _load_existing_keys(self, session: Session, health_data_id: int) -> Dict[str, Set]:
        """Load existing keys for duplicate checking."""
        existing_keys = {
            'records': set(),
            'workouts': set(),
            'correlations': set(),
            'activity_summaries': set()
        }
        
        # Load existing record keys
        record_query = text("""
            SELECT type, start_date, end_date, COALESCE(value, '') as value
            FROM record WHERE health_data_id = :health_data_id
        """)
        for row in session.execute(record_query, {"health_data_id": health_data_id}):
            key = (row.type, str(row.start_date), str(row.end_date), row.value)
            existing_keys['records'].add(key)
        
        # Load existing workout keys
        workout_query = text("""
            SELECT workout_activity_type, start_date, end_date
            FROM workout WHERE health_data_id = :health_data_id
        """)
        for row in session.execute(workout_query, {"health_data_id": health_data_id}):
            key = (row.workout_activity_type, str(row.start_date), str(row.end_date))
            existing_keys['workouts'].add(key)
        
        # Load existing correlation keys
        correlation_query = text("""
            SELECT type, start_date, end_date
            FROM correlation WHERE health_data_id = :health_data_id
        """)
        for row in session.execute(correlation_query, {"health_data_id": health_data_id}):
            key = (row.type, str(row.start_date), str(row.end_date))
            existing_keys['correlations'].add(key)
        
        return existing_keys
    
    def parse_file(self, xml_path: str) -> None:
        """Parse XML file using multiprocessing."""
        print(f"Starting multiprocessing parse of: {xml_path}")
        print(f"Using {self.num_processes} processes")
        
        # Get file size for progress tracking
        file_size = Path(xml_path).stat().st_size
        print(f"File size: {file_size / (1024**3):.2f} GB")
        
        start_time = time.time()
        
        with Session(self.engine) as session:
            # Set up database optimizations
            session.exec(text("PRAGMA journal_mode=WAL"))
            session.exec(text("PRAGMA synchronous=NORMAL"))
            session.exec(text("PRAGMA cache_size=1000000"))
            session.exec(text("PRAGMA temp_store=MEMORY"))
            session.exec(text("PRAGMA mmap_size=268435456"))
            session.commit()
            
            # Get or create HealthData record
            existing_health_data = session.exec(select(HealthData)).first()
            if existing_health_data:
                health_data = existing_health_data
                print(f"Using existing HealthData record with ID: {health_data.id}")
            else:
                # Parse HealthData from XML root
                with open(xml_path, 'rb') as f:
                    for event, elem in etree.iterparse(f, events=('start',)):
                        if elem.tag == "HealthData":
                            health_data = self._parse_health_data(elem)
                            session.add(health_data)
                            session.commit()
                            print(f"Created HealthData record with ID: {health_data.id}")
                            break
            
            # Load existing keys for duplicate checking
            print("Loading existing keys...")
            existing_keys = self._load_existing_keys(session, health_data.id)
            print(f"Loaded {len(existing_keys['records'])} existing records")
            print(f"Loaded {len(existing_keys['workouts'])} existing workouts")
            print(f"Loaded {len(existing_keys['correlations'])} existing correlations")
        
        # Extract and distribute elements
        print(f"Extracting and distributing elements among {self.num_processes} processes...")
        element_chunks = extract_elements_from_xml(xml_path, self.num_processes)
        
        # Use multiprocessing Manager for shared state
        with mp.Manager() as manager:
            # Create shared dictionaries instead of sets for better multiprocessing support
            shared_keys = {
                'records': manager.dict({k: True for k in existing_keys['records']}),
                'workouts': manager.dict({k: True for k in existing_keys['workouts']}),
                'correlations': manager.dict({k: True for k in existing_keys['correlations']}),
                'activity_summaries': manager.dict({k: True for k in existing_keys['activity_summaries']})
            }
            
            # Process chunks in parallel
            print("Processing element chunks in parallel...")
            with mp.Pool(processes=self.num_processes) as pool:
                # Create arguments for each process
                args = [(i, chunk, health_data.id, shared_keys) for i, chunk in enumerate(element_chunks)]
                
                # Process chunks and collect results
                results = []
                with tqdm(total=len(element_chunks), desc="Processing chunks") as pbar:
                    for result in pool.starmap(process_element_chunk, args):
                        results.append(result)
                        pbar.update(1)
        
        # Aggregate results and bulk insert
        print("Aggregating results...")
        self._bulk_insert_results(results)
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Calculate performance metrics
        total_processed = sum(self.stats[key] for key in ['records', 'workouts', 'correlations', 'activity_summaries'])
        if duration > 0:
            rate = total_processed / duration
            print(f"\nMultiprocessing Performance:")
            print(f"Total time: {duration:.1f} seconds ({duration/60:.1f} minutes)")
            print(f"Total elements processed: {total_processed:,}")
            print(f"Processing rate: {rate:.1f} elements/second")
            print(f"Speedup with {self.num_processes} processes: ~{self.num_processes}x theoretical")
        
        print("\nFinal Statistics:")
        for key, value in self.stats.items():
            print(f"  {key}: {value:,}")
    
    def _parse_health_data(self, root) -> HealthData:
        """Parse HealthData from XML root element."""
        # Get Me element for personal info
        me_elem = root.find(".//Me")
        
        return HealthData(
            locale=root.get("locale", ""),
            export_date=datetime.now(ZoneInfo("Europe/Zurich")),
            date_of_birth=me_elem.get("HKCharacteristicTypeIdentifierDateOfBirth", "") if me_elem is not None else "",
            biological_sex=me_elem.get("HKCharacteristicTypeIdentifierBiologicalSex", "") if me_elem is not None else "",
            blood_type=me_elem.get("HKCharacteristicTypeIdentifierBloodType", "") if me_elem is not None else "",
            fitzpatrick_skin_type=me_elem.get("HKCharacteristicTypeIdentifierFitzpatrickSkinType", "") if me_elem is not None else "",
            cardio_fitness_medications_use=me_elem.get("HKCharacteristicTypeIdentifierCardioFitnessMedicationsUse", "") if me_elem is not None else ""
        )
    
    def _bulk_insert_results(self, results: List[ProcessingResult]) -> None:
        """Bulk insert all processing results to database."""
        print("Performing bulk database inserts...")
        
        # Aggregate all results
        all_records = []
        all_workouts = []
        all_correlations = []
        all_activity_summaries = []
        
        for result in results:
            all_records.extend(result.records)
            all_workouts.extend(result.workouts)
            all_correlations.extend(result.correlations)
            all_activity_summaries.extend(result.activity_summaries)
            
            # Update statistics
            for key, value in result.stats.items():
                self.stats[key] += value
        
        # Bulk insert with batching
        batch_size = 10000
        
        with Session(self.engine) as session:
            # Insert records in batches
            if all_records:
                print(f"Inserting {len(all_records):,} records...")
                for i in range(0, len(all_records), batch_size):
                    batch = all_records[i:i + batch_size]
                    session.add_all(batch)
                    session.commit()
                    self.stats['bulk_inserts'] += 1
            
            # Insert workouts in batches
            if all_workouts:
                print(f"Inserting {len(all_workouts):,} workouts...")
                for i in range(0, len(all_workouts), batch_size):
                    batch = all_workouts[i:i + batch_size]
                    session.add_all(batch)
                    session.commit()
                    self.stats['bulk_inserts'] += 1
            
            # Insert correlations in batches
            if all_correlations:
                print(f"Inserting {len(all_correlations):,} correlations...")
                for i in range(0, len(all_correlations), batch_size):
                    batch = all_correlations[i:i + batch_size]
                    session.add_all(batch)
                    session.commit()
                    self.stats['bulk_inserts'] += 1
            
            # Insert activity summaries in batches  
            if all_activity_summaries:
                print(f"Inserting {len(all_activity_summaries):,} activity summaries...")
                for i in range(0, len(all_activity_summaries), batch_size):
                    batch = all_activity_summaries[i:i + batch_size]
                    session.add_all(batch)
                    session.commit()
                    self.stats['bulk_inserts'] += 1
        
        print("Bulk insert complete!")