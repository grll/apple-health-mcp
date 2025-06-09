#!/usr/bin/env python3
"""Script to parse Apple Health export data using the optimized parser."""

import sys
import time
from pathlib import Path

# Add src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from apple_health_mcp.parser_optimized import OptimizedAppleHealthParser


def main():
    """Main function to run the optimized parser."""
    # Default paths
    xml_path = "data/export/apple_health_export/export.xml"
    db_path = "data/sqlite_optimized.db"

    # Check if XML file exists
    if not Path(xml_path).exists():
        print(f"Error: XML file not found at {xml_path}")
        print("Please ensure the Apple Health export is extracted to data/export/")
        return 1

    print("Apple Health Data Parser - OPTIMIZED VERSION")
    print("=" * 60)
    print("Key optimizations:")
    print("- Bulk duplicate checking using in-memory sets")
    print("- 10x larger batch sizes (10,000 vs 1,000)")
    print("- Optimized database indexes")
    print("- Reduced commit frequency")
    print("- SQLite performance tuning")
    print("=" * 60)

    try:
        # Create optimized parser
        parser = OptimizedAppleHealthParser(db_path=db_path)
        
        # Start timing
        start_time = time.time()
        
        # Parse the file
        parser.parse_file(xml_path)

        # End timing
        end_time = time.time()
        duration = end_time - start_time
        
        # Calculate performance metrics
        total_records = parser.stats["records"] + parser.stats["workouts"] + parser.stats["correlations"]
        if duration > 0:
            records_per_second = total_records / duration
            print(f"\nPerformance Summary:")
            print(f"Total time: {duration:.1f} seconds ({duration/60:.1f} minutes)")
            print(f"Total records processed: {total_records:,}")
            print(f"Processing rate: {records_per_second:.1f} records/second")
            print(f"Bulk inserts performed: {parser.stats['bulk_inserts']:,}")
            
            # Compare with original performance (80 records/second)
            if records_per_second > 80:
                improvement = records_per_second / 80
                print(f"Performance improvement: {improvement:.1f}x faster than original!")
            
        print(f"\nOptimized database created at: {db_path}")
        
        # Show final statistics
        print("\nFinal Statistics:")
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