#!/usr/bin/env python3
"""Check the parsed data in the database."""

from sqlmodel import Session, create_engine, select
from src.apple_health_mcp.models import Record, Workout, Correlation, ActivitySummary, HealthData

# Create engine
engine = create_engine("sqlite:///data/sqlite.db")

with Session(engine) as session:
    # Count records
    record_count = session.exec(select(Record).limit(1)).first()
    if record_count:
        total_records = session.query(Record).count()
        print(f"Records: {total_records}")
        
        # Show sample record
        sample = session.exec(select(Record).limit(1)).first()
        if sample:
            print(f"Sample record: type={sample.type}, value={sample.value}, date={sample.start_date}")
    else:
        print("No records found")
    
    # Count other entities
    print(f"Workouts: {session.query(Workout).count()}")
    print(f"Correlations: {session.query(Correlation).count()}")
    print(f"Activity Summaries: {session.query(ActivitySummary).count()}")
    
    # Check HealthData
    health_data = session.exec(select(HealthData).limit(1)).first()
    if health_data:
        print(f"\nHealthData: locale={health_data.locale}, export_date={health_data.export_date}")
        print(f"  DOB: {health_data.date_of_birth}")
        print(f"  Sex: {health_data.biological_sex}")