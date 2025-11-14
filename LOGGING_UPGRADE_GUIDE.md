# Data Import Logging & Validation Upgrade Guide

## Overview

Three new modules provide unified logging, validation, and error handling for both MOLO and Stellar data imports:

1. **data_import_logger.py** - Unified logging framework
2. **data_validator.py** - Data validation between CSV and database
3. **error_handler.py** - Consistent error handling with retry logic

## Module Features

### DataImportLogger
- ✅ Uniform logging format for MOLO and Stellar
- ✅ Tracks all import stages (extraction, staging, merge, validation)
- ✅ Detailed metrics per table (row counts, durations, success rates)
- ✅ System-level and overall summaries
- ✅ JSON export for external monitoring
- ✅ Color-coded status indicators (✅❌⚠️)

### DataValidator
- ✅ Validates CSV row count matches staging table
- ✅ Verifies staging data merged to DW tables
- ✅ Sample ID existence checks
- ✅ NULL ID detection
- ✅ Data type validation
- ✅ Referential integrity checks

### ErrorHandler
- ✅ Automatic retry logic for failed operations
- ✅ Row-by-row fallback for batch insert failures
- ✅ Consistent error reporting and logging
- ✅ Error categorization (staging, merge, validation)
- ✅ Error summary and aggregation

## Integration Example

### Basic Integration

```python
import logging
from data_import_logger import DataImportLogger, ImportStage
from data_validator import DataValidator
from error_handler import ErrorHandler

# Initialize logger
logger = logging.getLogger(__name__)
import_logger = DataImportLogger(logger)
validator = DataValidator(db_connector, logger)
error_handler = ErrorHandler(logger)

# Start system import
import_logger.start_system_import("MOLO")

# Process each table
for csv_file, table_name in tables:
    # Start table tracking
    import_logger.start_table_import(
        "MOLO", 
        table_name, 
        csv_file, 
        csv_row_count=len(csv_data)
    )
    
    try:
        # 1. STAGING INSERT with error handling
        success_count, error_count = error_handler.handle_staging_insert(
            table_name,
            db_connector.insert_method,
            parsed_data
        )
        import_logger.log_staging_insert(success_count, error_count)
        
        # 2. MERGE OPERATION
        merge_result = error_handler.handle_merge_operation(
            f"SP_MERGE_{table_name}",
            db_connector,
            table_name
        )
        import_logger.log_merge_operation(
            matched=merge_result['matched'],
            inserted=merge_result['inserted'],
            updated=merge_result['updated'],
            error_count=merge_result['errors']
        )
        
        # 3. VALIDATION
        passed, issues = validator.validate_table_import(
            csv_content,
            f"STG_{table_name}",
            f"DW_{table_name}"
        )
        import_logger.log_validation_result(passed, issues)
        
    except Exception as e:
        import_logger.add_error(ImportStage.STAGING_INSERT, str(e))
    
    finally:
        # End table tracking
        import_logger.end_table_import("MOLO")

# End system import
import_logger.end_system_import("MOLO")

# Print final summary
import_logger.print_final_summary()

# Export to JSON for monitoring
summary_json = import_logger.export_summary_to_dict()
```

### Enhanced download_csv_from_s3.py Integration

```python
def main():
    # Setup logging
    logger = logging.getLogger(__name__)
    import_logger = DataImportLogger(logger)
    validator = DataValidator(db, logger)
    error_handler = ErrorHandler(logger)
    
    # Process MOLO
    if config.get('enable_molo', True):
        import_logger.start_system_import("MOLO")
        
        for csv_file in molo_csv_files:
            table_name = csv_to_table_name(csv_file)
            csv_content = extract_csv(csv_file)
            csv_row_count = len(csv.DictReader(io.StringIO(csv_content)))
            
            import_logger.start_table_import(
                "MOLO", 
                table_name, 
                csv_file, 
                csv_row_count
            )
            
            # Parse data
            parsed_data = parse_function(csv_content)
            
            # Insert to staging with error handling
            success_count, error_count = error_handler.handle_staging_insert(
                f"STG_MOLO_{table_name}",
                getattr(db, f"insert_{table_name}"),
                parsed_data
            )
            import_logger.log_staging_insert(success_count, error_count)
            
            import_logger.end_table_import("MOLO")
        
        # Run all merges
        import_logger.start_table_import(
            "MOLO", 
            "ALL_MERGES", 
            "SP_RUN_ALL_MOLO_STELLAR_MERGES"
        )
        
        merge_result = error_handler.handle_merge_operation(
            "SP_RUN_ALL_MOLO_STELLAR_MERGES",
            db,
            "ALL_TABLES"
        )
        
        import_logger.log_merge_operation(
            matched=merge_result['matched']
        )
        import_logger.end_table_import("MOLO")
        
        import_logger.end_system_import("MOLO")
    
    # Process Stellar (same pattern)
    if config.get('enable_stellar', True):
        import_logger.start_system_import("Stellar")
        # ... similar to MOLO ...
        import_logger.end_system_import("Stellar")
    
    # Final summary
    import_logger.print_final_summary()
    
    # Save to file for monitoring
    with open('import_summary.json', 'w') as f:
        json.dump(import_logger.export_summary_to_dict(), f, indent=2)
```

## Sample Output

### Console Output

```
================================================================================
MOLO DATA IMPORT STARTED
================================================================================

────────────────────────────────────────────────────────────────────────────────
📊 Processing: MOLO_BOATS
   Source: Boats.csv
   CSV Rows: 793
   ✅ Success
   Staging: 793 inserted, 0 errors
   Merge: 12 updated, 781 inserted
   Duration: 2.34s

────────────────────────────────────────────────────────────────────────────────
📊 Processing: MOLO_INVOICES
   Source: InvoiceSet.csv
   CSV Rows: 5,247
   ⚠️  Warning
   Staging: 5,245 inserted, 2 errors
   Merge: 1,203 updated, 4,044 inserted
   Duration: 8.91s
   ⚠️  Validation Issues: 1
      • Row count mismatch: CSV=5,247, Staging=5,245 (difference: 2)

================================================================================
MOLO IMPORT SUMMARY
================================================================================
Tables Processed: 47
Success Rate: 95.7%
Total CSV Rows: 125,439
Total Staged: 125,401
Total Merged: 98,234 inserted, 27,167 updated
Duration: 245.3s (4.1 minutes)
⚠️  Total Warnings: 3
❌ Total Errors: 38

⚠️  Tables with Warnings (3):
   • MOLO_INVOICES
   • MOLO_TRANSACTIONS
   • MOLO_CONTACTS

================================================================================
COMPLETE DATA IMPORT SUMMARY
================================================================================
✅ MOLO:
   Tables: 47
   Success Rate: 95.7%
   Records: 125,401 staged, 125,401 merged
   Warnings: 3

✅ Stellar:
   Tables: 28
   Success Rate: 100.0%
   Records: 87,234 staged, 87,234 merged

✅ ALL IMPORTS COMPLETED SUCCESSFULLY
⚠️  6 validation warnings detected
================================================================================
```

### JSON Export

```json
{
  "molo": {
    "system_name": "MOLO",
    "tables_processed": 47,
    "success_rate": 95.7,
    "total_csv_rows": 125439,
    "total_staging_inserted": 125401,
    "total_merge_inserts": 98234,
    "total_merge_updates": 27167,
    "total_errors": 38,
    "total_warnings": 3,
    "duration_seconds": 245.3,
    "tables": {
      "MOLO_BOATS": {
        "csv_file": "Boats.csv",
        "csv_rows": 793,
        "staging_inserted": 793,
        "merge_updated": 12,
        "merge_inserted": 781,
        "status": "✅ Success",
        "errors": 0,
        "warnings": 0
      }
    }
  },
  "stellar": { ... },
  "timestamp": "2025-11-13T17:45:23.456789"
}
```

## Migration Steps

1. **Install new modules** (already created)
   - data_import_logger.py
   - data_validator.py
   - error_handler.py

2. **Update download_csv_from_s3.py**
   - Add imports
   - Initialize logger, validator, error_handler
   - Wrap staging inserts with error_handler.handle_staging_insert()
   - Add validation after each table
   - Use import_logger for consistent formatting

3. **Update molo_db_functions.py**
   - Modify insert methods to return (success_count, error_count)
   - Add better exception handling
   - Return detailed error information

4. **Update run_merges.py**
   - Use error_handler.handle_merge_operation()
   - Track merge metrics
   - Report validation results

5. **Configure logging**
   ```python
   logging.basicConfig(
       level=logging.INFO,
       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
       handlers=[
           logging.FileHandler('data_import.log'),
           logging.StreamHandler()
       ]
   )
   ```

## Validation Examples

### Basic Validation
```python
# Validate a single table
passed, issues = validator.validate_table_import(
    csv_content,
    "STG_MOLO_BOATS",
    "DW_MOLO_BOATS"
)
```

### Referential Integrity
```python
# Check foreign key relationships
passed, issues = validator.check_referential_integrity(
    "DW_MOLO_BOATS",           # child table
    "DW_MOLO_BOAT_TYPES",      # parent table
    "BOAT_TYPE_ID",            # foreign key
    "ID"                       # parent primary key
)
```

### Data Type Validation
```python
# Validate column data types
from datetime import datetime
from decimal import Decimal

passed, issues = validator.validate_data_types(
    "DW_MOLO_INVOICES",
    {
        "ID": int,
        "TOTAL": Decimal,
        "DATE_FIELD": datetime,
        "TITLE": str
    }
)
```

## Benefits

1. **Uniform Logging** - Same format for MOLO and Stellar
2. **Better Error Tracking** - Know exactly what failed and why
3. **Data Validation** - Catch issues before they propagate
4. **Automated Retry** - Recover from transient failures
5. **Performance Metrics** - Track duration and throughput
6. **Monitoring Integration** - JSON export for external tools
7. **Debugging** - Clear error messages with context

## Next Steps

1. Integrate into download_csv_from_s3.py
2. Test with sample data
3. Review error logs and adjust validation rules
4. Add monitoring alerts based on JSON export
5. Document any table-specific validation rules
