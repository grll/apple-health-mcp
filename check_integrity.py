#!/usr/bin/env python3
"""Check data integrity between XML file and database."""

import xml.etree.ElementTree as ET
from datetime import datetime
from zoneinfo import ZoneInfo
from sqlmodel import Session, create_engine, select, func
from src.apple_health_mcp.models import Record, HealthData, MetadataEntry, Workout, Correlation, ActivitySummary

# Create engine
engine = create_engine("sqlite:///data/sqlite.db")

def count_xml_elements(xml_path: str):
    """Count elements in XML file using streaming."""
    counts = {
        'Record': 0,
        'Workout': 0,
        'Correlation': 0,
        'ActivitySummary': 0,
        'MetadataEntry': 0,
        'WorkoutEvent': 0,
        'ClinicalRecord': 0,
        'Audiogram': 0,
        'VisionPrescription': 0
    }
    
    print("Counting XML elements (this may take a while)...")
    
    # Use iterparse for memory efficiency
    context = ET.iterparse(xml_path, events=('start', 'end'))
    context = iter(context)
    
    # Skip root
    event, root = next(context)
    
    for event, elem in context:
        if event == 'start' and elem.tag in counts:
            counts[elem.tag] += 1
        
        # Clear element to save memory
        if event == 'end':
            elem.clear()
            if hasattr(elem, '_parent'):
                elem._parent = None
    
    return counts

def check_sample_records(xml_path: str, session: Session):
    """Check a few sample records for data integrity."""
    print("\nChecking sample records for data integrity...")
    
    # Parse first few records from XML
    context = ET.iterparse(xml_path, events=('start', 'end'))
    context = iter(context)
    
    # Skip to first records
    samples_checked = 0
    max_samples = 5
    
    for event, elem in context:
        if event == 'start' and elem.tag == 'Record' and samples_checked < max_samples:
            # Get record attributes
            record_type = elem.get('type')
            value = elem.get('value')
            unit = elem.get('unit')
            start_date = elem.get('startDate')
            source_name = elem.get('sourceName')
            
            # Convert unit (unescape XML entities)
            if unit:
                unit = unit.replace('&lt;', '<').replace('&gt;', '>')
            
            print(f"\nXML Record {samples_checked + 1}:")
            print(f"  Type: {record_type}")
            print(f"  Value: {value}")
            print(f"  Unit: {unit}")
            print(f"  Start Date: {start_date}")
            print(f"  Source: {source_name}")
            
            # Find corresponding database record
            if start_date and record_type:
                # Parse date
                dt = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S %z")
                dt_zurich = dt.astimezone(ZoneInfo("Europe/Zurich"))
                
                # Query database
                stmt = select(Record).where(
                    Record.type == record_type,
                    Record.start_date == dt_zurich
                )
                if value:
                    stmt = stmt.where(Record.value == value)
                
                db_record = session.exec(stmt).first()
                
                if db_record:
                    print(f"  ✓ Found in DB: value={db_record.value}, unit={db_record.unit}")
                    
                    # Check metadata
                    metadata_count = 0
                    for child in elem:
                        if child.tag == 'MetadataEntry':
                            metadata_count += 1
                    
                    if metadata_count > 0:
                        db_metadata_count = session.exec(
                            select(func.count(MetadataEntry.id)).where(
                                MetadataEntry.parent_type == "record",
                                MetadataEntry.parent_id == db_record.id
                            )
                        ).one()
                        print(f"  Metadata: XML={metadata_count}, DB={db_metadata_count}")
                else:
                    print(f"  ✗ NOT FOUND in database!")
            
            samples_checked += 1
        
        # Clear element
        if event == 'end':
            elem.clear()
        
        if samples_checked >= max_samples:
            break

def check_health_data(xml_path: str, session: Session):
    """Check HealthData and Me element."""
    print("\nChecking HealthData integrity...")
    
    # Parse XML to get HealthData and Me
    tree = ET.parse(xml_path)
    root = tree.getroot()
    
    locale = root.get('locale')
    print(f"XML HealthData locale: {locale}")
    
    # Find ExportDate and Me elements
    export_date = None
    me_data = {}
    
    for child in root:
        if child.tag == 'ExportDate':
            export_date = child.get('value')
            print(f"XML ExportDate: {export_date}")
        elif child.tag == 'Me':
            me_data = {
                'dob': child.get('HKCharacteristicTypeIdentifierDateOfBirth'),
                'sex': child.get('HKCharacteristicTypeIdentifierBiologicalSex'),
                'blood_type': child.get('HKCharacteristicTypeIdentifierBloodType'),
                'skin_type': child.get('HKCharacteristicTypeIdentifierFitzpatrickSkinType'),
                'cardio_meds': child.get('HKCharacteristicTypeIdentifierCardioFitnessMedicationsUse')
            }
            print(f"XML Me data: {me_data}")
            break
    
    # Check database
    health_data = session.exec(select(HealthData)).first()
    if health_data:
        print(f"\nDB HealthData:")
        print(f"  Locale: {health_data.locale} {'✓' if health_data.locale == locale else '✗'}")
        print(f"  Export Date: {health_data.export_date}")
        print(f"  DOB: {health_data.date_of_birth} {'✓' if health_data.date_of_birth == me_data.get('dob') else '✗'}")
        print(f"  Sex: {health_data.biological_sex} {'✓' if health_data.biological_sex == me_data.get('sex') else '✗'}")
        print(f"  Blood Type: {health_data.blood_type} {'✓' if health_data.blood_type == me_data.get('blood_type') else '✗'}")
    else:
        print("✗ No HealthData found in database!")

def main():
    xml_path = "data/export/apple_health_export/export.xml"
    
    # Count XML elements
    print("=" * 60)
    print("DATA INTEGRITY CHECK")
    print("=" * 60)
    
    xml_counts = count_xml_elements(xml_path)
    
    print("\nXML Element Counts:")
    for tag, count in xml_counts.items():
        if count > 0:
            print(f"  {tag}: {count:,}")
    
    # Check database counts
    with Session(engine) as session:
        print("\nDatabase Record Counts:")
        
        db_records = session.exec(select(func.count(Record.id))).one()
        print(f"  Records: {db_records:,}")
        
        db_workouts = session.exec(select(func.count(Workout.id))).one()
        print(f"  Workouts: {db_workouts:,}")
        
        db_correlations = session.exec(select(func.count(Correlation.id))).one()
        print(f"  Correlations: {db_correlations:,}")
        
        db_activities = session.exec(select(func.count(ActivitySummary.id))).one()
        print(f"  Activity Summaries: {db_activities:,}")
        
        db_metadata = session.exec(select(func.count(MetadataEntry.id))).one()
        print(f"  Metadata Entries: {db_metadata:,}")
        
        # Compare counts
        print("\nIntegrity Check Results:")
        print(f"  Records: XML={xml_counts['Record']:,}, DB={db_records:,} {'✓' if xml_counts['Record'] == db_records else '✗ MISMATCH!'}")
        print(f"  Workouts: XML={xml_counts['Workout']:,}, DB={db_workouts:,} {'✓' if xml_counts['Workout'] == db_workouts else '✗ MISMATCH!'}")
        print(f"  Correlations: XML={xml_counts['Correlation']:,}, DB={db_correlations:,} {'✓' if xml_counts['Correlation'] == db_correlations else '✗ MISMATCH!'}")
        print(f"  Activities: XML={xml_counts['ActivitySummary']:,}, DB={db_activities:,} {'✓' if xml_counts['ActivitySummary'] == db_activities else '✗ MISMATCH!'}")
        
        # Check HealthData
        check_health_data(xml_path, session)
        
        # Check sample records
        check_sample_records(xml_path, session)
        
        # Check date ranges
        print("\nDate Range Check:")
        oldest_record = session.exec(select(Record).order_by(Record.start_date).limit(1)).first()
        newest_record = session.exec(select(Record).order_by(Record.start_date.desc()).limit(1)).first()
        
        if oldest_record and newest_record:
            print(f"  Oldest record: {oldest_record.start_date.strftime('%Y-%m-%d')} ({oldest_record.type})")
            print(f"  Newest record: {newest_record.start_date.strftime('%Y-%m-%d')} ({newest_record.type})")
            
            # Calculate span
            span = newest_record.start_date - oldest_record.start_date
            print(f"  Date span: {span.days} days ({span.days // 365} years)")

if __name__ == "__main__":
    main()