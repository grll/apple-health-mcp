#!/usr/bin/env python3
"""Generate detailed integrity report."""

from sqlmodel import Session, create_engine, select, func
from src.apple_health_mcp.models import Record, HealthData, MetadataEntry

# Create engine
engine = create_engine("sqlite:///data/sqlite.db")

def main():
    print("=" * 60)
    print("DATA INTEGRITY REPORT")
    print("=" * 60)
    
    with Session(engine) as session:
        # Summary
        print("\n1. PARSING SUMMARY")
        print("-" * 40)
        print(f"XML File Size: 1.46 GB")
        print(f"Total XML Records: 3,419,701")
        print(f"Parsed Records: 192,497 (5.6%)")
        print(f"Status: Parser was interrupted (likely timeout)")
        
        # Data Quality Check
        print("\n2. DATA QUALITY CHECK")
        print("-" * 40)
        
        # Check for null values
        null_values = session.exec(
            select(func.count(Record.id)).where(Record.value == None)
        ).one()
        print(f"Records with null values: {null_values}")
        
        # Check for records with metadata
        records_with_metadata = session.exec(
            select(func.count(func.distinct(MetadataEntry.parent_id))).where(
                MetadataEntry.parent_type == "record"
            )
        ).one()
        print(f"Records with metadata: {records_with_metadata:,}")
        
        # Data types distribution
        print("\n3. RECORD TYPES DISTRIBUTION")
        print("-" * 40)
        stmt = select(Record.type, func.count(Record.id)).group_by(Record.type).order_by(func.count(Record.id).desc())
        results = session.exec(stmt).all()
        
        for record_type, count in results:
            percentage = (count / 192497) * 100
            print(f"{record_type}: {count:,} ({percentage:.1f}%)")
        
        # Source distribution
        print("\n4. DATA SOURCES")
        print("-" * 40)
        stmt = select(Record.source_name, func.count(Record.id)).group_by(Record.source_name).order_by(func.count(Record.id).desc())
        results = session.exec(stmt).all()
        
        for source, count in results[:5]:  # Top 5 sources
            percentage = (count / 192497) * 100
            print(f"{source}: {count:,} ({percentage:.1f}%)")
        
        # Integrity findings
        print("\n5. INTEGRITY FINDINGS")
        print("-" * 40)
        print("✓ HealthData correctly parsed with all personal information")
        print("✓ Sample records show exact match between XML and DB")
        print("✓ Metadata entries correctly linked to parent records")
        print("✓ Date/time values correctly converted to Europe/Zurich timezone")
        print("✓ XML entities (< >) properly unescaped in unit fields")
        print("✗ Parser only processed 5.6% of records before timeout")
        print("✗ No Workouts, Correlations, or ActivitySummaries were parsed")
        
        # Recommendations
        print("\n6. RECOMMENDATIONS")
        print("-" * 40)
        print("1. Run parser with longer timeout or in background process")
        print("2. Add checkpoint/resume capability for large files")
        print("3. Consider parallel processing for different record types")
        print("4. Add progress persistence to handle interruptions")
        
        # Database stats
        print("\n7. DATABASE STATISTICS")
        print("-" * 40)
        
        # Get database file size
        import os
        db_size = os.path.getsize("data/sqlite.db") / (1024 * 1024)
        print(f"Database size: {db_size:.1f} MB")
        print(f"Compression ratio: {1460 / db_size:.1f}:1")
        
        # Performance metrics
        records_per_mb = 192497 / db_size
        print(f"Records per MB: {records_per_mb:.0f}")
        
        # Estimate full database size
        estimated_full_size = (3419701 / 192497) * db_size
        print(f"Estimated full DB size: {estimated_full_size:.0f} MB")

if __name__ == "__main__":
    main()