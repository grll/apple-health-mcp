#!/usr/bin/env python3
"""Benchmark script to test specific optimizations on a smaller dataset."""

import sys
import time
import os
from pathlib import Path
from collections import defaultdict

# Add src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from lxml import etree
from sqlmodel import Session, create_engine, select, text
from apple_health_mcp.models import Record, HealthData


def benchmark_duplicate_checking():
    """Benchmark different duplicate checking approaches."""
    print("Benchmarking Duplicate Checking Approaches")
    print("=" * 50)
    
    # Create test database
    db_path = "data/benchmark_test.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    
    engine = create_engine(f"sqlite:///{db_path}")
    
    # Create test data
    print("Creating test dataset...")
    with Session(engine) as session:
        # Create health data
        health_data = HealthData(
            locale="en_US",
            export_date=time.time(),
            date_of_birth="1990-01-01",
            biological_sex="Male",
            blood_type="O+",
            fitzpatrick_skin_type="",
            cardio_fitness_medications_use=""
        )
        session.add(health_data)
        session.commit()
        
        # Create test records
        test_records = []
        for i in range(10000):
            record = Record(
                type=f"HKQuantityTypeIdentifierStepCount",
                source_name="Health",
                start_date=f"2024-01-{(i % 30) + 1:02d} 12:00:00 +0000",
                end_date=f"2024-01-{(i % 30) + 1:02d} 12:01:00 +0000",
                value=str(i % 1000),
                health_data_id=health_data.id
            )
            test_records.append(record)
        
        session.add_all(test_records)
        session.commit()
        print(f"Created {len(test_records)} test records")
    
    # Test 1: Individual SELECT queries (original approach)
    print("\nTest 1: Individual SELECT queries (Original)")
    start_time = time.time()
    
    duplicate_count = 0
    with Session(engine) as session:
        for i in range(1000):  # Test on 1000 lookups
            # Simulate looking for duplicates
            stmt = select(Record).where(
                Record.type == "HKQuantityTypeIdentifierStepCount",
                Record.start_date == f"2024-01-{(i % 30) + 1:02d} 12:00:00 +0000",
                Record.end_date == f"2024-01-{(i % 30) + 1:02d} 12:01:00 +0000",
                Record.value == str(i % 1000)
            )
            result = session.exec(stmt).first()
            if result:
                duplicate_count += 1
    
    original_time = time.time() - start_time
    print(f"Original approach: {original_time:.3f}s, {duplicate_count} duplicates found")
    
    # Test 2: Bulk EXISTS query
    print("\nTest 2: Bulk EXISTS query")
    start_time = time.time()
    
    duplicate_count = 0
    with Session(engine) as session:
        # Build bulk query
        test_keys = []
        for i in range(1000):
            test_keys.append((
                "HKQuantityTypeIdentifierStepCount",
                f"2024-01-{(i % 30) + 1:02d} 12:00:00 +0000",
                f"2024-01-{(i % 30) + 1:02d} 12:01:00 +0000",
                str(i % 1000)
            ))
        
        # Create VALUES clause
        values_clause = ", ".join([
            f"('{key[0]}', '{key[1]}', '{key[2]}', '{key[3]}')"
            for key in test_keys
        ])
        
        # Execute bulk query
        bulk_query = f"""
        SELECT test_keys.type, test_keys.start_date, test_keys.end_date, test_keys.value
        FROM (VALUES {values_clause}) AS test_keys(type, start_date, end_date, value)
        WHERE EXISTS (
            SELECT 1 FROM record r 
            WHERE r.type = test_keys.type 
            AND r.start_date = test_keys.start_date 
            AND r.end_date = test_keys.end_date 
            AND r.value = test_keys.value
        )
        """
        
        results = session.exec(text(bulk_query)).fetchall()
        duplicate_count = len(results)
    
    bulk_time = time.time() - start_time
    print(f"Bulk EXISTS approach: {bulk_time:.3f}s, {duplicate_count} duplicates found")
    
    # Test 3: In-memory set (optimized approach)
    print("\nTest 3: In-memory set lookup")
    start_time = time.time()
    
    # Load existing keys into memory
    existing_keys = set()
    with Session(engine) as session:
        for record in session.exec(select(Record)).all():
            key = (record.type, record.start_date, record.end_date, record.value)
            existing_keys.add(key)
    
    # Test lookups
    duplicate_count = 0
    for i in range(1000):
        key = (
            "HKQuantityTypeIdentifierStepCount",
            f"2024-01-{(i % 30) + 1:02d} 12:00:00 +0000",
            f"2024-01-{(i % 30) + 1:02d} 12:01:00 +0000",
            str(i % 1000)
        )
        if key in existing_keys:
            duplicate_count += 1
    
    memory_time = time.time() - start_time
    print(f"In-memory set approach: {memory_time:.3f}s, {duplicate_count} duplicates found")
    
    # Calculate improvements
    print(f"\nPerformance Comparison:")
    print(f"Original vs Bulk:    {original_time / bulk_time:.1f}x improvement")
    print(f"Original vs Memory:  {original_time / memory_time:.1f}x improvement")
    print(f"Bulk vs Memory:      {bulk_time / memory_time:.1f}x improvement")
    
    # Cleanup
    os.remove(db_path)


def benchmark_batch_sizes():
    """Benchmark different batch sizes."""
    print("\nBenchmarking Batch Sizes")
    print("=" * 50)
    
    batch_sizes = [100, 1000, 5000, 10000, 20000]
    
    for batch_size in batch_sizes:
        print(f"\nTesting batch size: {batch_size}")
        
        db_path = f"data/batch_test_{batch_size}.db"
        if os.path.exists(db_path):
            os.remove(db_path)
        
        engine = create_engine(f"sqlite:///{db_path}")
        
        # Create test records
        test_records = []
        for i in range(50000):  # 50k records
            record = Record(
                type="HKQuantityTypeIdentifierStepCount",
                source_name="Health",
                start_date=f"2024-01-01 {i % 24:02d}:{i % 60:02d}:00 +0000",
                end_date=f"2024-01-01 {i % 24:02d}:{i % 60:02d}:01 +0000",
                value=str(i),
                health_data_id=1
            )
            test_records.append(record)
        
        # Time batch insertion
        start_time = time.time()
        
        with Session(engine) as session:
            for i in range(0, len(test_records), batch_size):
                batch = test_records[i:i + batch_size]
                session.add_all(batch)
                session.commit()
        
        batch_time = time.time() - start_time
        records_per_second = len(test_records) / batch_time
        
        print(f"  Time: {batch_time:.2f}s")
        print(f"  Rate: {records_per_second:.0f} records/sec")
        
        # Cleanup
        os.remove(db_path)


def benchmark_sqlite_settings():
    """Benchmark different SQLite performance settings."""
    print("\nBenchmarking SQLite Settings")
    print("=" * 50)
    
    test_configs = [
        {"name": "Default", "settings": {}},
        {"name": "WAL Mode", "settings": {
            "journal_mode": "WAL",
            "synchronous": "NORMAL"
        }},
        {"name": "Memory Optimized", "settings": {
            "journal_mode": "WAL",
            "synchronous": "NORMAL",
            "cache_size": "1000000",
            "temp_store": "MEMORY"
        }},
        {"name": "Full Optimization", "settings": {
            "journal_mode": "WAL",
            "synchronous": "NORMAL",
            "cache_size": "1000000",
            "temp_store": "MEMORY",
            "mmap_size": "268435456"
        }}
    ]
    
    for config in test_configs:
        print(f"\nTesting: {config['name']}")
        
        db_path = f"data/sqlite_test_{config['name'].lower().replace(' ', '_')}.db"
        if os.path.exists(db_path):
            os.remove(db_path)
        
        engine = create_engine(f"sqlite:///{db_path}")
        
        # Apply settings
        with Session(engine) as session:
            for setting, value in config['settings'].items():
                session.exec(text(f"PRAGMA {setting}={value}"))
            session.commit()
        
        # Create test data
        test_records = []
        for i in range(20000):
            record = Record(
                type="HKQuantityTypeIdentifierStepCount",
                source_name="Health",
                start_date=f"2024-01-01 {i % 24:02d}:{i % 60:02d}:00 +0000",
                end_date=f"2024-01-01 {i % 24:02d}:{i % 60:02d}:01 +0000",
                value=str(i),
                health_data_id=1
            )
            test_records.append(record)
        
        # Time insertion
        start_time = time.time()
        
        with Session(engine) as session:
            session.add_all(test_records)
            session.commit()
        
        config_time = time.time() - start_time
        records_per_second = len(test_records) / config_time
        
        print(f"  Time: {config_time:.2f}s")
        print(f"  Rate: {records_per_second:.0f} records/sec")
        
        # Cleanup
        os.remove(db_path)


def main():
    """Run all benchmarks."""
    print("Apple Health Parser Optimization Benchmarks")
    print("=" * 60)
    print("Testing individual optimization components")
    print("=" * 60)
    
    try:
        benchmark_duplicate_checking()
        benchmark_batch_sizes()
        benchmark_sqlite_settings()
        
        print("\n" + "=" * 60)
        print("Benchmark Complete!")
        print("Key findings should show:")
        print("- In-memory duplicate checking is 10-100x faster")
        print("- Larger batch sizes (5k-10k) perform better")
        print("- SQLite optimizations provide 2-5x improvement")
        print("=" * 60)
        
    except Exception as e:
        print(f"Error during benchmarking: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())