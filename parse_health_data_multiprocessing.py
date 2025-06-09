#!/usr/bin/env python3
"""Script to parse Apple Health export data using multiprocessing for maximum performance."""

import sys
import time
from pathlib import Path

# Add src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from apple_health_mcp.parser_multiprocessing import MultiprocessingAppleHealthParser


def main():
    """Main function to run the multiprocessing parser."""
    # Default paths
    xml_path = "data/export/apple_health_export/export.xml"
    db_path = "data/sqlite_multiprocessing.db"

    # Check if XML file exists
    if not Path(xml_path).exists():
        print(f"Error: XML file not found at {xml_path}")
        print("Please ensure the Apple Health export is extracted to data/export/")
        return 1

    print("Apple Health Data Parser - MULTIPROCESSING VERSION")
    print("=" * 70)
    print("🚀 MAXIMUM PERFORMANCE OPTIMIZATIONS:")
    print("- Parallel XML processing using multiple CPU cores")
    print("- Shared memory for duplicate checking across processes")
    print("- Memory-mapped file I/O for efficient chunk splitting")
    print("- Bulk database operations with coordinated writes")
    print("- Advanced SQLite performance tuning")
    print("=" * 70)

    try:
        # Create multiprocessing parser
        parser = MultiprocessingAppleHealthParser(db_path=db_path)
        
        # Start timing
        start_time = time.time()
        
        # Parse the file
        parser.parse_file(xml_path)

        # End timing
        end_time = time.time()
        duration = end_time - start_time
        
        # Calculate performance metrics
        total_records = (parser.stats["records"] + parser.stats["workouts"] + 
                        parser.stats["correlations"] + parser.stats["activity_summaries"])
        
        print(f"\n🎉 MULTIPROCESSING PERFORMANCE SUMMARY:")
        print(f"=" * 50)
        if duration > 0:
            records_per_second = total_records / duration
            print(f"Total time: {duration:.1f} seconds ({duration/60:.1f} minutes)")
            print(f"Total records processed: {total_records:,}")
            print(f"Processing rate: {records_per_second:.1f} records/second")
            print(f"CPU cores utilized: {parser.num_processes}")
            
            # Compare with previous optimizations
            optimized_rate = 8000  # From previous optimized parser
            if records_per_second > optimized_rate:
                improvement = records_per_second / optimized_rate
                print(f"Improvement over optimized parser: {improvement:.1f}x faster!")
            
            original_rate = 80  # Original parser rate
            total_improvement = records_per_second / original_rate
            print(f"Total improvement over original: {total_improvement:.1f}x faster!")
            
        print(f"\nMultiprocessing database created at: {db_path}")
        
        # Show final statistics
        print("\nDetailed Statistics:")
        for key, value in parser.stats.items():
            print(f"  {key}: {value:,}")
            
        return 0

    except KeyboardInterrupt:
        print("\nParsing interrupted by user")
        print("Current progress:")
        for key, value in parser.stats.items():
            print(f"  {key}: {value:,}")
        return 1
    except Exception as e:
        print(f"\nError during parsing: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())