# Duplicate Data Handling in Apple Health Parser

The Apple Health parser includes robust duplicate detection and handling to ensure data integrity when parsing the same export file multiple times or when updating with new exports.

## How Duplicate Detection Works

The parser checks for duplicates based on unique combinations of fields for each entity type:

### Records
- **Unique by**: `type`, `start_date`, `end_date`, `value`, `health_data_id`
- Example: A heart rate measurement at a specific time with a specific value

### Workouts
- **Unique by**: `workout_activity_type`, `start_date`, `end_date`, `health_data_id`
- Example: A running workout from 10:00 AM to 10:30 AM

### Correlations
- **Unique by**: `type`, `start_date`, `end_date`, `health_data_id`
- Example: A blood pressure correlation at a specific time

### Activity Summaries
- **Unique by**: `date_components`, `health_data_id`
- Example: Daily activity summary for 2024-01-15

### Clinical Records
- **Unique by**: `identifier`, `health_data_id`
- Example: A specific clinical document with a unique identifier

### Audiograms
- **Unique by**: `type`, `start_date`, `end_date`, `health_data_id`
- Example: A hearing test performed at a specific time

### Vision Prescriptions
- **Unique by**: `type`, `date_issued`, `health_data_id`
- Example: An eyeglass prescription issued on a specific date

## Performance Optimizations

1. **Database Indexes**: The models include indexes on frequently queried fields for fast duplicate checking
2. **Batch Processing**: Child entities are processed in batches to reduce database roundtrips
3. **Early Detection**: Duplicates are detected before attempting insertion

## Usage Example

```python
from src.apple_health_mcp.parser import AppleHealthParser

# Initialize parser
parser = AppleHealthParser(db_path="data/sqlite.db")

# First parse - all data will be inserted
parser.parse_file("data/export/apple_health_export/export.xml")
print(f"Inserted {parser.stats['records']} new records")
print(f"Found {parser.stats['duplicates']} duplicates")  # Should be 0

# Second parse - all data will be detected as duplicates
parser.parse_file("data/export/apple_health_export/export.xml")
print(f"Inserted {parser.stats['records']} new records")  # Should be 0
print(f"Found {parser.stats['duplicates']} duplicates")  # Should match first parse
```

## Statistics Tracking

The parser tracks detailed statistics including:
- Number of new records inserted
- Number of duplicates found and skipped
- Breakdown by entity type (records, workouts, correlations, etc.)
- Error count for problematic data

## Benefits

1. **Idempotent Parsing**: You can safely run the parser multiple times on the same data
2. **Incremental Updates**: Parse new exports without duplicating existing data
3. **Data Integrity**: Ensures no duplicate records in the database
4. **Performance**: Efficient duplicate detection with minimal overhead
5. **Transparency**: Clear reporting of what was inserted vs. skipped

## Testing Duplicate Handling

Use the provided test script to verify duplicate handling:

```bash
python test_duplicate_handling.py
```

This will:
1. Parse the data into a test database
2. Parse the same data again
3. Verify that no duplicates were created
4. Report on the effectiveness of duplicate detection