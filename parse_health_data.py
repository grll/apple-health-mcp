#!/usr/bin/env python3
"""Script to parse Apple Health export data into SQLite database."""

import sys
from pathlib import Path

# Add src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from apple_health_mcp.parser import AppleHealthParser


def main():
    """Main function to run the parser."""
    # Default paths
    xml_path = "data/export/apple_health_export/export.xml"
    db_path = "data/sqlite.db"

    # Check if XML file exists
    if not Path(xml_path).exists():
        print(f"Error: XML file not found at {xml_path}")
        print("Please ensure the Apple Health export is extracted to data/export/")
        return 1

    print("Apple Health Data Parser")
    print("=" * 50)

    try:
        # Create parser and parse the file
        parser = AppleHealthParser(db_path=db_path)
        parser.parse_file(xml_path)

        print("\nDatabase created successfully at:", db_path)
        return 0

    except KeyboardInterrupt:
        print("\nParsing interrupted by user")
        return 1
    except Exception as e:
        print(f"\nError during parsing: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
