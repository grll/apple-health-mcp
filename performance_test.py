#!/usr/bin/env python3
"""Performance test script to compare original vs optimized parser."""

import sys
import time
import os
from pathlib import Path

# Add src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from apple_health_mcp.parser import AppleHealthParser
from apple_health_mcp.parser_optimized import OptimizedAppleHealthParser


def test_parser_performance(parser_class, db_path: str, test_name: str, sample_size: int = 10000):
    """Test parser performance on a subset of data."""
    print(f"\n{'='*50}")
    print(f"Testing {test_name}")
    print(f"{'='*50}")
    
    # Remove test database if it exists
    if os.path.exists(db_path):
        os.remove(db_path)
    
    # Create parser
    parser = parser_class(db_path=db_path)
    
    # For testing, we'll override the batch size to be smaller for fair comparison
    if hasattr(parser, 'batch_size'):
        original_batch_size = parser.batch_size
        if test_name == "Original Parser":
            parser.batch_size = 1000  # Original size
        else:
            parser.batch_size = 5000  # Moderate increase for test
    
    xml_path = "data/export/apple_health_export/export.xml"
    
    if not os.path.exists(xml_path):
        print(f"Error: XML file not found at {xml_path}")
        return None
    
    try:
        # Start timing
        start_time = time.time()
        
        # For performance testing, we'll process only first N elements
        # This is simulated by running parser normally but tracking performance
        print(f"Starting {test_name} performance test...")
        
        # Note: For a real test, we'd need to modify the parser to stop after N records
        # For now, we'll run for a short time and extrapolate
        parser.parse_file(xml_path)
        
        # End timing
        end_time = time.time()
        duration = end_time - start_time
        
        # Calculate metrics
        total_processed = (parser.stats["records"] + parser.stats["workouts"] + 
                          parser.stats["correlations"] + parser.stats["activity_summaries"])
        
        if duration > 0:
            rate = total_processed / duration
            
            print(f"\nResults for {test_name}:")
            print(f"  Duration: {duration:.1f} seconds")
            print(f"  Records processed: {total_processed:,}")
            print(f"  Processing rate: {rate:.1f} records/second")
            print(f"  Duplicates found: {parser.stats['duplicates']:,}")
            print(f"  Errors: {parser.stats['errors']:,}")
            
            if hasattr(parser, 'stats') and 'bulk_inserts' in parser.stats:
                print(f"  Bulk inserts: {parser.stats['bulk_inserts']:,}")
            
            return {
                'duration': duration,
                'records': total_processed,
                'rate': rate,
                'duplicates': parser.stats['duplicates'],
                'errors': parser.stats['errors']
            }
        
    except KeyboardInterrupt:
        print(f"\n{test_name} interrupted by user")
        return None
    except Exception as e:
        print(f"\nError in {test_name}: {e}")
        return None
    finally:
        # Cleanup
        if os.path.exists(db_path):
            os.remove(db_path)


def run_performance_comparison():
    """Run performance comparison between parsers."""
    print("Apple Health Parser Performance Comparison")
    print("=" * 60)
    print("This test compares the original parser vs the optimized parser")
    print("Note: Test will run on the full dataset to measure real performance")
    print("=" * 60)
    
    # Test original parser
    print("\n🔄 Testing Original Parser...")
    original_results = test_parser_performance(
        AppleHealthParser, 
        "data/test_original.db", 
        "Original Parser"
    )
    
    # Test optimized parser
    print("\n🚀 Testing Optimized Parser...")
    optimized_results = test_parser_performance(
        OptimizedAppleHealthParser, 
        "data/test_optimized.db", 
        "Optimized Parser"
    )
    
    # Compare results
    if original_results and optimized_results:
        print(f"\n{'='*60}")
        print("PERFORMANCE COMPARISON SUMMARY")
        print(f"{'='*60}")
        
        rate_improvement = optimized_results['rate'] / original_results['rate']
        time_reduction = (original_results['duration'] - optimized_results['duration']) / original_results['duration'] * 100
        
        print(f"Original Parser Rate:    {original_results['rate']:.1f} records/sec")
        print(f"Optimized Parser Rate:   {optimized_results['rate']:.1f} records/sec")
        print(f"Speed Improvement:       {rate_improvement:.1f}x faster")
        print(f"Time Reduction:          {time_reduction:.1f}%")
        
        # Estimate time for full dataset (3.4M records)
        full_dataset_size = 3_400_000
        original_time_estimate = full_dataset_size / original_results['rate'] / 3600  # hours
        optimized_time_estimate = full_dataset_size / optimized_results['rate'] / 3600  # hours
        
        print(f"\nEstimated time for 3.4M records:")
        print(f"Original Parser:         {original_time_estimate:.1f} hours")
        print(f"Optimized Parser:        {optimized_time_estimate:.1f} hours")
        print(f"Time Savings:            {original_time_estimate - optimized_time_estimate:.1f} hours")
        
        if rate_improvement >= 10:
            print(f"\n🎉 SUCCESS! Achieved {rate_improvement:.1f}x performance improvement!")
        elif rate_improvement >= 5:
            print(f"\n✅ GOOD! Achieved {rate_improvement:.1f}x performance improvement!")
        else:
            print(f"\n⚠️  MODEST improvement of {rate_improvement:.1f}x - consider further optimizations")
    
    else:
        print("\n❌ Could not complete performance comparison due to errors")


def main():
    """Main function."""
    try:
        run_performance_comparison()
    except KeyboardInterrupt:
        print("\n\nPerformance test interrupted by user")
        return 1
    except Exception as e:
        print(f"\nError during performance test: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())