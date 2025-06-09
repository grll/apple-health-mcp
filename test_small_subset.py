#!/usr/bin/env python3
"""Test optimized parser on a small subset."""

import sys
import time
from pathlib import Path

# Add src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from apple_health_mcp.parser_optimized import OptimizedAppleHealthParser


def create_small_xml_sample():
    """Create a small XML sample for testing."""
    xml_content = '''<?xml version="1.0" encoding="UTF-8"?>
<HealthData locale="en_US">
 <ExportDate value="2025-05-31 18:21:36 +0200"/>
 <Me HKCharacteristicTypeIdentifierDateOfBirth="1995-01-15" HKCharacteristicTypeIdentifierBiologicalSex="HKBiologicalSexMale" HKCharacteristicTypeIdentifierBloodType="HKBloodTypeNotSet" HKCharacteristicTypeIdentifierFitzpatrickSkinType="HKFitzpatrickSkinTypeNotSet" HKCharacteristicTypeIdentifierCardioFitnessMedicationsUse="None"/>
 <Record type="HKQuantityTypeIdentifierBloodGlucose" sourceName="Health" sourceVersion="17.3" unit="mmol&lt;180.1558800000541&gt;/L" creationDate="2024-01-09 22:32:54 +0200" startDate="2024-01-09 12:30:00 +0200" endDate="2024-01-09 12:30:00 +0200" value="4.59">
  <MetadataEntry key="HKWasUserEntered" value="1"/>
  <MetadataEntry key="HKBloodGlucoseMealTime" value="1"/>
 </Record>
 <Record type="HKQuantityTypeIdentifierBodyMassIndex" sourceName="Zepp Life" sourceVersion="202311211629" unit="count" creationDate="2023-12-03 22:41:05 +0200" startDate="2023-12-03 22:38:45 +0200" endDate="2023-12-03 22:38:45 +0200" value="21.3294"/>
 <Record type="HKQuantityTypeIdentifierBodyMassIndex" sourceName="Zepp Life" sourceVersion="202311211629" unit="count" creationDate="2023-12-03 22:41:05 +0200" startDate="2023-12-03 22:39:44 +0200" endDate="2023-12-03 22:39:44 +0200" value="22.4"/>
 <Record type="HKQuantityTypeIdentifierHeartRate" sourceName="Guillaume's Apple Watch" sourceVersion="10.1" unit="count/min" creationDate="2024-05-01 10:15:00 +0200" startDate="2024-05-01 10:15:00 +0200" endDate="2024-05-01 10:15:00 +0200" value="75"/>
 <Record type="HKQuantityTypeIdentifierHeartRate" sourceName="Guillaume's Apple Watch" sourceVersion="10.1" unit="count/min" creationDate="2024-05-01 10:16:00 +0200" startDate="2024-05-01 10:16:00 +0200" endDate="2024-05-01 10:16:00 +0200" value="78"/>
</HealthData>'''
    
    with open("test_sample.xml", "w") as f:
        f.write(xml_content)


def main():
    """Test the optimized parser on a small sample."""
    print("Creating test XML sample...")
    create_small_xml_sample()
    
    print("Testing optimized parser...")
    start_time = time.time()
    
    # Use a clean test database
    parser = OptimizedAppleHealthParser("test_sample.db")
    parser.parse_file("test_sample.xml")
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"\nTest completed in {duration:.2f} seconds")
    print("\nFinal statistics:")
    for key, value in parser.stats.items():
        print(f"  {key}: {value:,}")
    
    # Test duplicate handling by running again
    print("\n" + "="*50)
    print("Testing duplicate handling (running again)...")
    start_time = time.time()
    
    parser2 = OptimizedAppleHealthParser("test_sample.db")
    parser2.parse_file("test_sample.xml")
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"\nSecond run completed in {duration:.2f} seconds")
    print("\nSecond run statistics:")
    for key, value in parser2.stats.items():
        print(f"  {key}: {value:,}")
    
    # Clean up
    import os
    os.remove("test_sample.xml")
    os.remove("test_sample.db")


if __name__ == "__main__":
    main()