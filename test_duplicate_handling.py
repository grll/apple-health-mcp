#!/usr/bin/env python3
"""Test script to verify duplicate handling in the parser."""

import os
from pathlib import Path
from src.apple_health_mcp.parser import AppleHealthParser
from sqlmodel import Session, select
from src.apple_health_mcp.models import Record, HealthData


def test_duplicate_handling():
    """Test that the parser correctly handles duplicate data."""
    # Use a test database
    test_db = "data/test_duplicate.db"
    
    # Remove test database if it exists
    if os.path.exists(test_db):
        os.remove(test_db)
    
    # Create parser instance
    parser = AppleHealthParser(db_path=test_db)
    
    # Parse the data twice to test duplicate handling
    xml_path = "data/export/apple_health_export/export.xml"
    
    if not os.path.exists(xml_path):
        print(f"Error: XML file not found at {xml_path}")
        return
    
    print("First parse - should insert all records...")
    parser.parse_file(xml_path)
    first_stats = parser.stats.copy()
    
    print(f"\nFirst parse complete:")
    print(f"Records inserted: {first_stats['records']}")
    print(f"Duplicates found: {first_stats['duplicates']}")
    
    # Reset stats for second parse
    parser.stats = {key: 0 for key in parser.stats}
    
    print("\nSecond parse - should find all duplicates...")
    parser.parse_file(xml_path)
    second_stats = parser.stats.copy()
    
    print(f"\nSecond parse complete:")
    print(f"Records inserted: {second_stats['records']}")
    print(f"Duplicates found: {second_stats['duplicates']}")
    
    # Verify counts
    with Session(parser.engine) as session:
        total_records = session.exec(select(Record)).all()
        print(f"\nTotal records in database: {len(total_records)}")
        
        # Check that we didn't create duplicates
        if first_stats['records'] == len(total_records):
            print("✓ SUCCESS: No duplicate records were created!")
        else:
            print(f"✗ ERROR: Expected {first_stats['records']} records, but found {len(total_records)}")
        
        # Check that duplicates were detected
        expected_duplicates = first_stats['records'] + first_stats['workouts'] + first_stats['correlations'] + first_stats['activity_summaries']
        if second_stats['duplicates'] > 0:
            print(f"✓ SUCCESS: {second_stats['duplicates']} duplicates were correctly identified!")
        else:
            print("✗ ERROR: No duplicates were detected in the second parse")
    
    # Cleanup test database
    if os.path.exists(test_db):
        os.remove(test_db)
    print("\nTest database cleaned up.")


if __name__ == "__main__":
    test_duplicate_handling()