"""Test the parser with the sample export.xml file."""

import os
import tempfile
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from sqlmodel import Session, create_engine, select

from apple_health_mcp.models import (
    ActivitySummary,
    Correlation,
    CorrelationRecord,
    HealthData,
    MetadataEntry,
    Record,
    Workout,
    WorkoutEvent,
    WorkoutRoute,
    WorkoutStatistics,
)
from apple_health_mcp.parser import AppleHealthParser


@pytest.fixture
def sample_xml_path():
    """Path to the sample export.xml file."""
    return Path(__file__).parent.parent / "data" / "sample" / "export.xml"


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    yield db_path
    # Cleanup
    os.unlink(db_path)


class TestParserSample:
    """Test parsing of sample export.xml file."""

    def test_parse_sample_file(self, sample_xml_path, temp_db):
        """Test parsing the complete sample file."""
        # Create parser with temp database
        parser = AppleHealthParser(db_path=temp_db)

        # Parse the sample file
        parser.parse_file(str(sample_xml_path))

        # Create engine for verification
        engine = create_engine(f"sqlite:///{temp_db}")

        with Session(engine) as session:
            # Verify HealthData
            health_data = session.exec(select(HealthData)).first()
            assert health_data is not None
            assert health_data.locale == "en_US"
            assert health_data.date_of_birth == "1995-01-15"
            assert health_data.biological_sex == "HKBiologicalSexMale"
            assert health_data.blood_type == "HKBloodTypeNotSet"
            assert health_data.fitzpatrick_skin_type == "HKFitzpatrickSkinTypeNotSet"
            assert health_data.cardio_fitness_medications_use == "None"

            # Verify export date
            expected_export_date = datetime(
                2025, 1, 14, 12, 0, 0, tzinfo=ZoneInfo("Europe/Zurich")
            )
            # SQLite might not preserve timezone info, so compare without timezone
            assert (
                health_data.export_date.replace(tzinfo=ZoneInfo("Europe/Zurich"))
                == expected_export_date
            )

    def test_blood_glucose_records(self, sample_xml_path, temp_db):
        """Test parsing of blood glucose records."""
        parser = AppleHealthParser(db_path=temp_db)
        parser.parse_file(str(sample_xml_path))

        engine = create_engine(f"sqlite:///{temp_db}")
        with Session(engine) as session:
            # Get blood glucose records
            glucose_records = session.exec(
                select(Record).where(
                    Record.type == "HKQuantityTypeIdentifierBloodGlucose"
                )
            ).all()

            assert len(glucose_records) == 2

            # Check first record
            record1 = next(r for r in glucose_records if r.value == "4.59")
            assert record1.source_name == "Health"
            assert record1.source_version == "17.3"
            assert record1.unit == "mmol<180.1558800000541>/L"

            # Check metadata for first record
            metadata = session.exec(
                select(MetadataEntry).where(
                    MetadataEntry.parent_type == "record",
                    MetadataEntry.parent_id == record1.id,
                )
            ).all()
            assert len(metadata) == 2
            assert any(m.key == "HKWasUserEntered" and m.value == "1" for m in metadata)
            assert any(
                m.key == "HKBloodGlucoseMealTime" and m.value == "1" for m in metadata
            )

    def test_heart_rate_records(self, sample_xml_path, temp_db):
        """Test parsing of heart rate records."""
        parser = AppleHealthParser(db_path=temp_db)
        parser.parse_file(str(sample_xml_path))

        engine = create_engine(f"sqlite:///{temp_db}")
        with Session(engine) as session:
            # Get heart rate records
            hr_records = session.exec(
                select(Record).where(Record.type == "HKQuantityTypeIdentifierHeartRate")
            ).all()

            assert len(hr_records) == 3

            # Check values
            values = sorted([float(r.value) for r in hr_records])
            assert values == [56.0, 96.0, 150.0]

            # Check device info
            assert all("Apple Watch" in r.device for r in hr_records)
            assert all(r.unit == "count/min" for r in hr_records)

    def test_blood_pressure_correlation(self, sample_xml_path, temp_db):
        """Test parsing of blood pressure correlation."""
        parser = AppleHealthParser(db_path=temp_db)
        parser.parse_file(str(sample_xml_path))

        engine = create_engine(f"sqlite:///{temp_db}")
        with Session(engine) as session:
            # Get blood pressure correlation
            bp_correlation = session.exec(
                select(Correlation).where(
                    Correlation.type == "HKCorrelationTypeIdentifierBloodPressure"
                )
            ).first()

            assert bp_correlation is not None
            assert bp_correlation.source_name == "Health"

            # Get linked records through CorrelationRecord
            linked_records = session.exec(
                select(Record)
                .join(CorrelationRecord)
                .where(CorrelationRecord.correlation_id == bp_correlation.id)
            ).all()

            assert len(linked_records) == 2

            # Check systolic and diastolic
            systolic = next(r for r in linked_records if "Systolic" in r.type)
            diastolic = next(r for r in linked_records if "Diastolic" in r.type)

            assert systolic.value == "136"
            assert diastolic.value == "69"
            assert systolic.unit == "mmHg"
            assert diastolic.unit == "mmHg"

    def test_workout_records(self, sample_xml_path, temp_db):
        """Test parsing of workout records."""
        parser = AppleHealthParser(db_path=temp_db)
        parser.parse_file(str(sample_xml_path))

        engine = create_engine(f"sqlite:///{temp_db}")
        with Session(engine) as session:
            # Get workouts
            workouts = session.exec(select(Workout)).all()
            assert len(workouts) == 2

            # Check walking workout
            walking = next(
                w
                for w in workouts
                if w.workout_activity_type == "HKWorkoutActivityTypeWalking"
            )
            assert walking.duration == pytest.approx(88.23378186623255)
            assert walking.duration_unit == "min"
            assert walking.total_distance == 5.234
            assert walking.total_distance_unit == "km"
            assert walking.total_energy_burned == 342.5
            assert walking.total_energy_burned_unit == "Cal"

            # Check workout events
            events = session.exec(
                select(WorkoutEvent).where(WorkoutEvent.workout_id == walking.id)
            ).all()
            assert len(events) == 2

            pause_event = next(e for e in events if e.type == "HKWorkoutEventTypePause")
            resume_event = next(
                e for e in events if e.type == "HKWorkoutEventTypeResume"
            )
            assert pause_event is not None
            assert resume_event is not None

            # Check workout statistics
            stats = session.exec(
                select(WorkoutStatistics).where(
                    WorkoutStatistics.workout_id == walking.id
                )
            ).all()
            assert len(stats) == 2

            hr_stats = next(
                s for s in stats if s.type == "HKQuantityTypeIdentifierHeartRate"
            )
            assert hr_stats.average == 92.5
            assert hr_stats.minimum == 65
            assert hr_stats.maximum == 125
            assert hr_stats.unit == "count/min"

            # Check WorkoutRoute (should have only one despite duplicate entries)
            routes = session.exec(
                select(WorkoutRoute).where(WorkoutRoute.workout_id == walking.id)
            ).all()
            assert len(routes) == 1  # Only one route should exist despite duplicates
            
            route = routes[0]
            assert route.source_name == "Guillaume's Apple Watch"
            assert route.source_version == "11.2"

    def test_activity_summaries(self, sample_xml_path, temp_db):
        """Test parsing of activity summaries."""
        parser = AppleHealthParser(db_path=temp_db)
        parser.parse_file(str(sample_xml_path))

        engine = create_engine(f"sqlite:///{temp_db}")
        with Session(engine) as session:
            # Get activity summaries
            summaries = session.exec(select(ActivitySummary)).all()
            assert len(summaries) == 3

            # Check specific summary
            may_27 = next(s for s in summaries if s.date_components == "2020-05-27")
            assert may_27.active_energy_burned == 547.163
            assert may_27.active_energy_burned_goal == 680
            assert may_27.active_energy_burned_unit == "Cal"
            assert may_27.apple_exercise_time == 44
            assert may_27.apple_exercise_time_goal == 30
            assert may_27.apple_stand_hours == 10
            assert may_27.apple_stand_hours_goal == 12

    def test_sleep_analysis_records(self, sample_xml_path, temp_db):
        """Test parsing of sleep analysis records."""
        parser = AppleHealthParser(db_path=temp_db)
        parser.parse_file(str(sample_xml_path))

        engine = create_engine(f"sqlite:///{temp_db}")
        with Session(engine) as session:
            # Get sleep records
            sleep_records = session.exec(
                select(Record).where(
                    Record.type == "HKCategoryTypeIdentifierSleepAnalysis"
                )
            ).all()

            assert len(sleep_records) == 2

            # Check values
            in_bed = next(
                r
                for r in sleep_records
                if r.value == "HKCategoryValueSleepAnalysisInBed"
            )
            asleep = next(
                r
                for r in sleep_records
                if r.value == "HKCategoryValueSleepAnalysisAsleepUnspecified"
            )

            assert in_bed.source_name == "AutoSleep"
            assert asleep.source_name == "AutoSleep"

    def test_various_record_types(self, sample_xml_path, temp_db):
        """Test parsing of various other record types."""
        parser = AppleHealthParser(db_path=temp_db)
        parser.parse_file(str(sample_xml_path))

        engine = create_engine(f"sqlite:///{temp_db}")
        with Session(engine) as session:
            # Test BMI records
            bmi_records = session.exec(
                select(Record).where(
                    Record.type == "HKQuantityTypeIdentifierBodyMassIndex"
                )
            ).all()
            assert len(bmi_records) == 2

            # Test body mass records
            mass_records = session.exec(
                select(Record).where(Record.type == "HKQuantityTypeIdentifierBodyMass")
            ).all()
            assert len(mass_records) == 2
            assert all(r.unit == "kg" for r in mass_records)

            # Test step count records
            step_records = session.exec(
                select(Record).where(Record.type == "HKQuantityTypeIdentifierStepCount")
            ).all()
            assert len(step_records) == 2

            # Test stand hour records
            stand_records = session.exec(
                select(Record).where(
                    Record.type == "HKCategoryTypeIdentifierAppleStandHour"
                )
            ).all()
            assert len(stand_records) == 1
            assert stand_records[0].value == "HKCategoryValueAppleStandHourStood"

            # Test VO2 Max record
            vo2_records = session.exec(
                select(Record).where(Record.type == "HKQuantityTypeIdentifierVO2Max")
            ).all()
            assert len(vo2_records) == 1
            assert vo2_records[0].value == "45.2"
            assert vo2_records[0].unit == "mL/kg·min"

    def test_parser_statistics(self, sample_xml_path, temp_db):
        """Test parser statistics after parsing."""
        parser = AppleHealthParser(db_path=temp_db)
        parser.parse_file(str(sample_xml_path))

        # Check statistics
        assert parser.stats["records"] > 0
        assert parser.stats["workouts"] == 2
        assert parser.stats["correlations"] == 1
        assert parser.stats["activity_summaries"] == 3
        assert parser.stats["metadata_entries"] > 0
        assert (
            parser.stats["correlation_records"] == 2
        )  # 2 records in blood pressure correlation
        assert parser.stats["errors"] == 0

    def test_metadata_entries(self, sample_xml_path, temp_db):
        """Test parsing of metadata entries."""
        parser = AppleHealthParser(db_path=temp_db)
        parser.parse_file(str(sample_xml_path))

        engine = create_engine(f"sqlite:///{temp_db}")
        with Session(engine) as session:
            # Get all metadata entries
            metadata_entries = session.exec(select(MetadataEntry)).all()
            assert len(metadata_entries) > 0

            # Check metadata for different parent types
            record_metadata = [m for m in metadata_entries if m.parent_type == "record"]
            correlation_metadata = [
                m for m in metadata_entries if m.parent_type == "correlation"
            ]
            workout_metadata = [
                m for m in metadata_entries if m.parent_type == "workout"
            ]

            assert len(record_metadata) > 0
            assert len(correlation_metadata) > 0
            assert len(workout_metadata) > 0

            # Check specific metadata values
            user_entered = [m for m in metadata_entries if m.key == "HKWasUserEntered"]
            assert len(user_entered) > 0
            assert all(m.value == "1" for m in user_entered)

    def test_duplicate_handling(self, sample_xml_path, temp_db):
        """Test that parsing the same file twice doesn't create duplicates."""
        parser = AppleHealthParser(db_path=temp_db)

        # Parse once
        parser.parse_file(str(sample_xml_path))

        # Parse again
        parser = AppleHealthParser(db_path=temp_db)
        parser.parse_file(str(sample_xml_path))
        second_stats = parser.stats.copy()

        # Check that second parse found duplicates
        assert second_stats["duplicates"] > 0
        assert second_stats["records"] == 0  # No new records added
        assert second_stats["workouts"] == 0  # No new workouts added
        assert second_stats["correlations"] == 0  # No new correlations added

        # Verify database has same number of records
        engine = create_engine(f"sqlite:///{temp_db}")
        with Session(engine) as session:
            health_data_count = session.exec(select(HealthData)).all()
            assert len(health_data_count) == 1  # Only one HealthData record

    def test_bulk_mode_performance(self, sample_xml_path, temp_db):
        """Test that bulk mode is faster than legacy mode."""
        # Test legacy mode (bulk_mode=False)
        legacy_parser = AppleHealthParser(db_path=temp_db + "_legacy", bulk_mode=False)
        start_time = time.time()
        legacy_parser.parse_file(str(sample_xml_path))
        legacy_time = time.time() - start_time
        legacy_stats = legacy_parser.stats.copy()

        # Test bulk mode (bulk_mode=True)
        bulk_parser = AppleHealthParser(db_path=temp_db + "_bulk", bulk_mode=True)
        start_time = time.time()
        bulk_parser.parse_file(str(sample_xml_path))
        bulk_time = time.time() - start_time
        bulk_stats = bulk_parser.stats.copy()

        # Both should parse same number of records
        assert bulk_stats["records"] == legacy_stats["records"]
        assert bulk_stats["workouts"] == legacy_stats["workouts"]
        assert bulk_stats["correlations"] == legacy_stats["correlations"]
        assert bulk_stats["activity_summaries"] == legacy_stats["activity_summaries"]
        assert bulk_stats["metadata_entries"] == legacy_stats["metadata_entries"]
        assert bulk_stats["errors"] == legacy_stats["errors"]

        # Bulk mode should be faster or at least not slower
        # (For small files, the difference might be minimal)
        print(f"Legacy mode: {legacy_time:.3f}s, Bulk mode: {bulk_time:.3f}s")
        assert bulk_time <= legacy_time * 1.5  # Allow some variance for small files

        # Verify both databases have same content
        legacy_engine = create_engine(f"sqlite:///{temp_db}_legacy")
        bulk_engine = create_engine(f"sqlite:///{temp_db}_bulk")

        with (
            Session(legacy_engine) as legacy_session,
            Session(bulk_engine) as bulk_session,
        ):
            # Compare record counts
            legacy_record_count = len(legacy_session.exec(select(Record)).all())
            bulk_record_count = len(bulk_session.exec(select(Record)).all())
            assert legacy_record_count == bulk_record_count

            # Compare workout counts
            legacy_workout_count = len(legacy_session.exec(select(Workout)).all())
            bulk_workout_count = len(bulk_session.exec(select(Workout)).all())
            assert legacy_workout_count == bulk_workout_count

    def test_bulk_mode_configuration(self, sample_xml_path, temp_db):
        """Test bulk mode configuration options."""
        # Test with custom batch sizes
        parser = AppleHealthParser(db_path=temp_db, bulk_mode=True)

        # Verify configuration
        assert parser.bulk_mode is True
        assert parser.batch_size == 5000  # bulk mode default
        assert parser.transaction_batch_size == 100

        # Parse and verify it works
        parser.parse_file(str(sample_xml_path))
        assert parser.stats["records"] > 0
        assert parser.stats["errors"] == 0

        # Test legacy mode configuration
        legacy_parser = AppleHealthParser(db_path=temp_db + "_config", bulk_mode=False)

        assert legacy_parser.bulk_mode is False
        assert legacy_parser.batch_size == 1000  # legacy mode default
        assert legacy_parser.transaction_batch_size == 100
