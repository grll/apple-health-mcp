#!/usr/bin/env python3
"""Test multiprocessing parser on a small subset."""

import sys
import time
import os
from pathlib import Path

# Add src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from apple_health_mcp.parser_multiprocessing import MultiprocessingAppleHealthParser


def create_test_xml_sample():
    """Create a larger XML sample for multiprocessing testing."""
    xml_content = '''<?xml version="1.0" encoding="UTF-8"?>
<HealthData locale="en_US">
 <ExportDate value="2025-05-31 18:21:36 +0200"/>
 <Me HKCharacteristicTypeIdentifierDateOfBirth="1995-01-15" HKCharacteristicTypeIdentifierBiologicalSex="HKBiologicalSexMale" HKCharacteristicTypeIdentifierBloodType="HKBloodTypeNotSet" HKCharacteristicTypeIdentifierFitzpatrickSkinType="HKFitzpatrickSkinTypeNotSet" HKCharacteristicTypeIdentifierCardioFitnessMedicationsUse="None"/>'''
    
    # Add many records to test multiprocessing effectively
    for i in range(1000):  # 1000 records for testing
        xml_content += f'''
 <Record type="HKQuantityTypeIdentifierStepCount" sourceName="Health" sourceVersion="17.3" unit="count" creationDate="2024-01-{(i % 28) + 1:02d} 12:{i % 60:02d}:00 +0200" startDate="2024-01-{(i % 28) + 1:02d} 12:{i % 60:02d}:00 +0200" endDate="2024-01-{(i % 28) + 1:02d} 12:{i % 60:02d}:00 +0200" value="{i % 10000}"/>'''
    
    # Add some workouts
    for i in range(50):  # 50 workouts
        xml_content += f'''
 <Workout workoutActivityType="HKWorkoutActivityTypeRunning" duration="30" durationUnit="min" totalDistance="5.2" totalDistanceUnit="km" totalEnergyBurned="300" totalEnergyBurnedUnit="kcal" sourceName="Health" sourceVersion="17.3" creationDate="2024-01-{(i % 28) + 1:02d} 14:00:00 +0200" startDate="2024-01-{(i % 28) + 1:02d} 14:00:00 +0200" endDate="2024-01-{(i % 28) + 1:02d} 14:30:00 +0200"/>'''
    
    # Add some correlations
    for i in range(20):  # 20 correlations
        xml_content += f'''
 <Correlation type="HKCorrelationTypeIdentifierBloodPressure" sourceName="Health" sourceVersion="17.3" creationDate="2024-01-{(i % 28) + 1:02d} 16:00:00 +0200" startDate="2024-01-{(i % 28) + 1:02d} 16:00:00 +0200" endDate="2024-01-{(i % 28) + 1:02d} 16:00:00 +0200"/>'''
    
    xml_content += '''
</HealthData>'''
    
    with open("test_multiprocessing_sample.xml", "w") as f:
        f.write(xml_content)


def main():
    """Test the multiprocessing parser on a larger sample."""
    print("Creating test XML sample for multiprocessing...")
    create_test_xml_sample()
    
    # Test with different numbers of processes
    test_configs = [
        {"processes": 1, "name": "Single Process"},
        {"processes": 2, "name": "Dual Process"},
        {"processes": 4, "name": "Quad Process"},
    ]
    
    results = {}
    
    for config in test_configs:
        print(f"\n{'='*60}")
        print(f"Testing {config['name']} ({config['processes']} processes)")
        print(f"{'='*60}")
        
        # Clean up previous test database
        db_path = f"test_multiprocessing_{config['processes']}.db"
        if os.path.exists(db_path):
            os.remove(db_path)
        
        try:
            start_time = time.time()
            
            # Create parser with specified number of processes
            parser = MultiprocessingAppleHealthParser(
                db_path=db_path, 
                num_processes=config['processes']
            )
            parser.parse_file("test_multiprocessing_sample.xml")
            
            end_time = time.time()
            duration = end_time - start_time
            
            total_processed = (parser.stats["records"] + parser.stats["workouts"] + 
                             parser.stats["correlations"] + parser.stats["activity_summaries"])
            
            rate = total_processed / duration if duration > 0 else 0
            
            results[config['processes']] = {
                'duration': duration,
                'total_processed': total_processed,
                'rate': rate,
                'stats': dict(parser.stats)
            }
            
            print(f"Duration: {duration:.2f} seconds")
            print(f"Records processed: {total_processed:,}")
            print(f"Processing rate: {rate:.1f} records/second")
            print("Statistics:")
            for key, value in parser.stats.items():
                print(f"  {key}: {value:,}")
                
        except Exception as e:
            print(f"Error testing {config['name']}: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            # Cleanup
            if os.path.exists(db_path):
                os.remove(db_path)
    
    # Compare results
    if len(results) > 1:
        print(f"\n{'='*60}")
        print("PERFORMANCE COMPARISON")
        print(f"{'='*60}")
        
        base_rate = results[1]['rate']  # Single process as baseline
        
        for processes, result in results.items():
            speedup = result['rate'] / base_rate if base_rate > 0 else 1
            efficiency = speedup / processes * 100
            
            print(f"{processes} Process(es):")
            print(f"  Rate: {result['rate']:.1f} records/sec")
            print(f"  Speedup: {speedup:.2f}x")
            print(f"  Efficiency: {efficiency:.1f}%")
        
        print(f"\n✅ Multiprocessing test completed successfully!")
        print(f"Expected: Higher process counts should show improved performance")
        print(f"Note: Efficiency may decrease due to coordination overhead")
    
    # Test duplicate handling by running again
    print(f"\n{'='*60}")
    print("Testing Duplicate Handling")
    print(f"{'='*60}")
    
    db_path = "test_duplicate_handling.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    
    try:
        # First run
        print("First run...")
        parser1 = MultiprocessingAppleHealthParser(db_path=db_path, num_processes=2)
        parser1.parse_file("test_multiprocessing_sample.xml")
        
        first_stats = dict(parser1.stats)
        
        # Second run (should detect duplicates)
        print("Second run (testing duplicate detection)...")
        parser2 = MultiprocessingAppleHealthParser(db_path=db_path, num_processes=2)
        parser2.parse_file("test_multiprocessing_sample.xml")
        
        second_stats = dict(parser2.stats)
        
        print(f"First run - Total records: {first_stats['records']:,}")
        print(f"Second run - Duplicates detected: {second_stats['duplicates']:,}")
        print(f"Second run - New records: {second_stats['records']:,}")
        
        if second_stats['duplicates'] > 0:
            print("✅ Duplicate detection working correctly!")
        else:
            print("⚠️  No duplicates detected - may need investigation")
            
    except Exception as e:
        print(f"Error testing duplicate handling: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Cleanup
        if os.path.exists(db_path):
            os.remove(db_path)
        if os.path.exists("test_multiprocessing_sample.xml"):
            os.remove("test_multiprocessing_sample.xml")
    
    print(f"\n🎉 Multiprocessing parser test complete!")


if __name__ == "__main__":
    main()