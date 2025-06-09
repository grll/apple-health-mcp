#!/usr/bin/env python3
"""Parse Apple Health data with detailed progress tracking."""

import sys
import time
from pathlib import Path

# Add src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from apple_health_mcp.parser import AppleHealthParser


def main():
    """Main function to run the parser with progress."""
    # Default paths
    xml_path = "data/export/apple_health_export/export.xml"
    db_path = "data/sqlite.db"

    # Check if XML file exists
    if not Path(xml_path).exists():
        print(f"Error: XML file not found at {xml_path}")
        return 1

    print("Apple Health Data Parser")
    print("=" * 50)
    print("Note: This will take several minutes for large files.")
    print("The parser will show progress every 5,000 records.")
    print("=" * 50)

    try:
        # Create parser with more frequent progress updates
        parser = AppleHealthParser(db_path=db_path)
        
        # Override batch size for more frequent commits
        parser.batch_size = 500  # Smaller batches = more frequent progress
        
        # Start time
        start_time = time.time()
        
        # Parse the file
        parser.parse_file(xml_path)

        # End time
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"\nParsing completed in {duration:.1f} seconds ({duration/60:.1f} minutes)")
        print(f"Database created successfully at: {db_path}")
        
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