#!/usr/bin/env python3
"""Example usage of the Apple Health parser with duplicate handling."""

from src.apple_health_mcp.parser import AppleHealthParser


def main():
    """Example of parsing Apple Health data with duplicate handling."""
    # Initialize the parser
    parser = AppleHealthParser(db_path="data/sqlite.db")
    
    # Parse the Apple Health export
    # The parser will automatically handle duplicates:
    # - Check if records already exist based on type, start_date, end_date, and value
    # - Skip inserting duplicates and count them separately
    # - Handle duplicates efficiently for batch processing
    
    print("Starting Apple Health data parsing...")
    print("Duplicate records will be automatically detected and skipped.")
    print("-" * 60)
    
    try:
        parser.parse_file("data/export/apple_health_export/export.xml")
        
        print("\nParsing completed successfully!")
        print("-" * 60)
        print("Summary:")
        print(f"- Total new records: {parser.stats['records']:,}")
        print(f"- Total new workouts: {parser.stats['workouts']:,}")
        print(f"- Total new correlations: {parser.stats['correlations']:,}")
        print(f"- Total new activity summaries: {parser.stats['activity_summaries']:,}")
        print(f"- Total duplicates skipped: {parser.stats['duplicates']:,}")
        print(f"- Total errors: {parser.stats['errors']:,}")
        
        if parser.stats['duplicates'] > 0:
            print(f"\n✓ Successfully skipped {parser.stats['duplicates']:,} duplicate records!")
            print("  This means the data was already in the database.")
        
    except Exception as e:
        print(f"\nError during parsing: {e}")
        raise


if __name__ == "__main__":
    main()