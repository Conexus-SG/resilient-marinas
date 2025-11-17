# AWS Marina Data ETL Pipeline - Application Guide

## Overview

This application is an **ETL (Extract, Transform, Load) pipeline** that synchronizes marina management data from two source systems:

1. **MOLO Marina System** - Marina management (slips, reservations, boats, invoices, etc.)
2. **Stellar Business System** - Boat rental operations (bookings, payments, customers, pricing, etc.)

The pipeline downloads data from **AWS S3 buckets**, processes it, and loads it into an **Oracle Autonomous Database** data warehouse for business intelligence and reporting.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         AWS S3 Buckets                              │
├────────────────────────────────┬────────────────────────────────────┤
│  MOLO System                   │  Stellar Business System           │
│  Bucket: cnxtestbucket         │  Bucket: resilient-ims-backups     │
│  Format: ZIP files with CSVs   │  Format: .gz DATA files with CSVs  │
│  Files: 47 MOLO tables         │  Files: 29 Stellar tables          │
└────────────────────────────────┴────────────────────────────────────┘
                                 ↓
┌─────────────────────────────────────────────────────────────────────┐
│                      Python ETL Scripts                             │
├─────────────────────────────────────────────────────────────────────┤
│  download_csv_from_s3.py      - Main orchestrator                  │
│  download_stellar_from_s3.py  - Stellar data processor             │
│  molo_db_functions.py         - MOLO database operations           │
│  stellar_db_functions.py      - Stellar database operations        │
└─────────────────────────────────────────────────────────────────────┘
                                 ↓
┌─────────────────────────────────────────────────────────────────────┐
│               Oracle Autonomous Database (Chicago)                  │
│                     DSN: oax4504110443_low                          │
├─────────────────────────────────────────────────────────────────────┤
│  STG_* Tables (Staging)        DW_* Tables (Data Warehouse)        │
│  ├── STG_MOLO_*    (47)        ├── DW_MOLO_*    (47)               │
│  └── STG_STELLAR_* (29)        └── DW_STELLAR_* (29)               │
│                                                                     │
│  Stored Procedures: 65 total                                       │
│  ├── SP_MERGE_MOLO_*    (35 procedures)                            │
│  ├── SP_MERGE_STELLAR_* (29 procedures)                            │
│  └── SP_RUN_ALL_MOLO_STELLAR_MERGES (master orchestrator)          │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow: 3-Step ETL Process

### Step 1: Extract - Download from S3
- **MOLO**: Download latest ZIP file, extract 47 CSV files
- **Stellar**: Download latest .gz DATA files, decompress 29 CSV files

### Step 2: Transform & Load - Staging Tables (STG_*)
- Parse CSV data
- Apply data type conversions and validations
- **TRUNCATE** existing staging tables
- **INSERT** fresh data into STG_MOLO_* and STG_STELLAR_* tables

### Step 3: Merge - Data Warehouse (DW_*)
- Execute stored procedures
- **MERGE** staging data into data warehouse tables
- **UPDATE** existing records (based on primary key)
- **INSERT** new records
- Track `DW_LAST_INSERTED` and `DW_LAST_UPDATED` timestamps

---

## File Structure & Script Descriptions

### Configuration Files

#### `config.json`
**Purpose**: Centralized credential and configuration management

**Contains**:
```json
{
  "aws": {
    "access_key_id": "AKIA...",
    "secret_access_key": "secret...",
    "region": "us-east-1"
  },
  "database": {
    "user": "API_USER",
    "password": "<password>",
    "dsn": "oax4504110443_low"
  },
  "s3": {
    "molo_bucket": "cnxtestbucket",
    "stellar_bucket": "resilient-ims-backups"
  }
}
```

**Security**: Never commit to version control. Use `config.json.template` as reference.

#### `wallet_demo/`
**Purpose**: Oracle Autonomous Database connection wallet

**Contains**:
- `cwallet.sso` - Wallet credentials
- `tnsnames.ora` - Database connection strings
- `sqlnet.ora` - Network configuration

---

### Core Python Scripts

#### `download_csv_from_s3.py` (Main Orchestrator)
**Purpose**: Main ETL script that coordinates both MOLO and Stellar data processing

**What it does**:
1. Loads credentials from `config.json`
2. Initializes Oracle Instant Client and wallet
3. Connects to AWS S3 and Oracle Database
4. Downloads latest MOLO ZIP file from S3
5. Extracts and processes 47 MOLO CSV files:
   - MarinaLocations, Piers, Slips, SlipTypes
   - Reservations, Companies, Contacts, Boats, Accounts
   - Invoices, InvoiceItems, Transactions
   - ItemMasters, SeasonalPrices, TransientPrices
   - 35+ reference/lookup tables
6. Calls Stellar processing module if available
7. Executes master merge stored procedure

**Key Functions**:
- `load_config_file()` - Loads config.json
- `parse_*_data()` - 47 CSV parser functions (one per MOLO table)
- `setup_logging()` - Configures dual logging (console + file)

**Usage**:
```bash
# Process both MOLO and Stellar
python3 download_csv_from_s3.py

# MOLO only
python3 download_csv_from_s3.py --process-molo

# Stellar only  
python3 download_csv_from_s3.py --process-stellar

# Enable field-level validation (CSV vs DB comparison)
python3 download_csv_from_s3.py --validate-fields

# Enable merge change validation (Staging vs DW comparison)
python3 download_csv_from_s3.py --validate-merge-changes

# Full validation with custom sample size
python3 download_csv_from_s3.py \
    --validate-fields \
    --validate-merge-changes \
    --validation-sample-size 20
```

**Validation Options** (see `FIELD_VALIDATION_GUIDE.md` for details):
- `--validate-fields` - Compare CSV values with database values to detect corruption
- `--validate-merge-changes` - Verify merge operations don't modify data unexpectedly
- `--validation-sample-size N` - Number of records to sample per table (default: 10)

**Output**:
- Inserts into 47 STG_MOLO_* staging tables
- Calls Stellar processing
- Executes all 65 merge stored procedures
- Logs to console and `molo_processing.log`

---

#### `download_stellar_from_s3.py` (Stellar Processor)
**Purpose**: Processes Stellar Business marina rental system data

**What it does**:
1. Downloads gzipped DATA files from S3 (resilient-ims-backups bucket)
2. Decompresses and parses **29 complete Stellar CSV files**:
   
   **Core Reference Data (9 tables)**:
   - **customers** (52 columns) - User accounts, billing/mailing addresses, club membership, credit cards
   - **locations** (22 columns) - Marina locations, operating details, status, Zoho integration
   - **seasons** (20 columns) - Seasonal pricing periods, time restrictions by weekday/weekend/holiday
   - **accessories** (19 columns) - Rentable equipment (tubes, skis, kayaks), pricing, deposits
   - **accessory_options** (6 columns) - Accessory variants (sizes, colors, types)
   - **accessory_tiers** (8 columns) - Pricing tiers by rental duration
   - **amenities** (16 columns) - Location amenities (parking, showers, wifi), icons, display
   - **categories** (15 columns) - Boat categories, filtering options, min nights
   - **holidays** (2 columns) - Holiday dates (composite key: location_id + holiday_date)
   
   **Booking System (4 tables)**:
   - **bookings** (82 columns) - Main booking records, totals, fees, taxes, payment status
   - **booking_boats** (57 columns) - Individual boat rentals, check-in/out, damages, fuel
   - **booking_payments** (56 columns) - Payment transactions, credit card, refunds, reporting
   - **booking_accessories** (8 columns) - Accessories attached to bookings (composite key)
   
   **Boat Inventory & Pricing (11 tables)**:
   - **style_groups** (11 columns) - Boat categories, safety tests, max departures
   - **styles** (98 columns) - Boat types with extensive pricing/availability rules
   - **style_boats** (39 columns) - Individual boats, hull numbers, maintenance, insurance
   - **customer_boats** (9 columns) - Customer-owned boats in slips
   - **season_dates** (4 columns) - Season date ranges
   - **style_hourly_prices** (22 columns) - Hourly pricing by day of week
   - **style_times** (26 columns) - Available time slots with 4 departure windows
   - **style_prices** (12 columns) - Fixed prices by time slot (uses TIME_ID as PK)
   - **club_tiers** (28 columns) - Membership tiers, credits, fees, restrictions
   - **coupons** (30 columns) - Discount codes, restrictions, usage tracking
   - **waitlists** (18 columns) - Booking waitlist requests
   
   **Point of Sale (5 tables)**:
   - **pos_items** (9 columns) - Retail items for sale
   - **pos_sales** (11 columns) - POS transactions
   - **fuel_sales** (14 columns) - Fuel sales with qty and type
   - **closed_dates** (9 columns) - Dates when marina is closed
   - **blacklists** (10 columns) - Banned customers

3. Inserts into 29 STG_STELLAR_* staging tables

**Key Parser Functions** (29 total):
- `parse_customers_data()` - 52 columns: name, addresses, emergency contacts, club membership
- `parse_locations_data()` - 22 columns: code, name, type, minimums, delivery, pricing
- `parse_seasons_data()` - 20 columns: season dates, time restrictions
- `parse_accessories_data()` - 19 columns: rentable equipment pricing
- `parse_accessory_options_data()` - 6 columns: variants
- `parse_accessory_tiers_data()` - 8 columns: duration-based pricing
- `parse_amenities_data()` - 16 columns: location features
- `parse_categories_data()` - 15 columns: boat categories
- `parse_holidays_data()` - 2 columns: composite key (location_id, holiday_date)
- `parse_bookings_data()` - 82 columns: booking totals, fees, taxes
- `parse_booking_boats_data()` - 57 columns: boat rentals, check-in/out
- `parse_booking_payments_data()` - 56 columns: payment transactions
- `parse_booking_accessories_data()` - 8 columns: accessory line items
- `parse_style_groups_data()` - 11 columns: boat groups
- `parse_styles_data()` - 98 columns: boat types (CSV has 124, we use 98)
- `parse_style_boats_data()` - 39 columns: physical boats
- `parse_customer_boats_data()` - 9 columns: customer-owned boats
- `parse_season_dates_data()` - 4 columns: date ranges
- `parse_style_hourly_prices_data()` - 22 columns: hourly pricing
- `parse_style_times_data()` - 26 columns: time slots
- `parse_style_prices_data()` - 12 columns: fixed pricing (TIME_ID as PK)
- `parse_club_tiers_data()` - 28 columns: membership tiers
- `parse_coupons_data()` - 30 columns: discount codes
- `parse_pos_items_data()` - 9 columns: retail inventory
- `parse_pos_sales_data()` - 11 columns: POS transactions
- `parse_fuel_sales_data()` - 14 columns: fuel transactions
- `parse_waitlists_data()` - 18 columns: booking requests
- `parse_closed_dates_data()` - 9 columns: closure dates
- `parse_blacklists_data()` - 10 columns: banned customers

**Special Handling**:
- Customers table uses `USER_ID` as primary key (not ID)
- Holidays table has composite key (LOCATION_ID, HOLIDAY_DATE) - no ID column
- Booking_accessories has composite key (BOOKING_ID, ACCESSORY_ID) - no ID column
- Style_prices uses TIME_ID as primary key (not ID)
- Styles CSV has 124 columns but staging table only uses 98 (26 legacy columns ignored)
- CSV column names mapped to staging table columns (e.g., checkout_date → CHECK_OUT_DATE)
- All parsers match actual prod_resilient_2025-10-01_16_03-DATA CSV structures
- Robust error handling for missing/null fields

**Usage**:
```python
# Imported by download_csv_from_s3.py
from download_stellar_from_s3 import process_stellar_data_from_s3

# Or standalone
python3 download_stellar_from_s3.py
```

**Output**:
- Inserts into 29 STG_STELLAR_* staging tables
- Logs to console and `stellar_processing.log`

---

#### `molo_db_functions.py`
**Purpose**: Oracle database connector and MOLO table operations

**What it does**:
- Establishes Oracle database connection with wallet authentication
- Provides 47 `insert_*()` methods for MOLO tables
- Handles data type conversions and NULL handling
- Manages staging table truncation
- Executes master merge stored procedure

**Key Class**: `OracleConnector`

**Key Methods**:
- `__init__()` - Initialize connection with wallet setup
- `_setup_oracle_wallet()` - Configure TNS_ADMIN for wallet
- `_initialize_oracle_client()` - Load Oracle Instant Client
- `truncate_staging_tables()` - Clear all 47 STG_MOLO_* tables
- `insert_marina_locations()` - 12 columns
- `insert_contacts()` - 43 columns (largest MOLO table)
- `insert_invoices()` - 54 columns (complex billing data)
- `insert_transactions()` - 49 columns (payment processing)
- `run_all_merges()` - Execute SP_RUN_ALL_MOLO_STELLAR_MERGES

**Database Setup**:
- Oracle Instant Client: `/opt/oracle/instantclient`
- Wallet Location: `./wallet_demo`
- Connection String: `oax4504110443_low` (Chicago low-latency)

---

#### `stellar_db_functions.py`
**Purpose**: Oracle database connector and Stellar table operations

**What it does**:
- Same architecture as molo_db_functions.py
- Provides 29 `insert_*()` methods for Stellar tables
- Handles Stellar-specific primary keys (USER_ID for customers)
- Manages composite keys (holidays: LOCATION_ID + HOLIDAY_DATE)

**Key Class**: `OracleConnector` (parallel to MOLO version)

**Key Methods**:
- `insert_customers()` - 52 columns, uses USER_ID as PK
- `insert_locations()` - 22 columns, marina operating locations
- `insert_bookings()` - 82 columns (rental reservations)
- `insert_booking_boats()` - 57 columns (boat assignment details)
- `insert_booking_payments()` - 56 columns (payment processing)
- `insert_styles()` - 98 columns (boat types with complex pricing)
- `insert_holidays()` - 2 columns, NO ID (composite key)

**Special Handling**:
- Customers: `USER_ID` primary key instead of `ID`
- Holidays: Composite key (LOCATION_ID, HOLIDAY_DATE)
- Styles: 98 columns including hourly/nightly/multi-day pricing rules

---

### Stored Procedure Scripts

#### `generate_merge_procedures_smart.py`
**Purpose**: Dynamically generate all 65 stored procedures by querying database schema

**What it does**:
1. Connects to Oracle database
2. Queries `user_tab_columns` to get column names for each STG_* table
3. Detects primary keys (ID, USER_ID, TIME_ID, composite keys)
4. Generates MERGE procedures with explicit column mappings
5. Creates 35 MOLO + 29 Stellar + 1 master procedure
6. Saves individual .sql files + combined deployment file

**Tables Processed**:
- **MOLO (35 tables)**:
  - Core: ACCOUNTS, BOATS, COMPANIES, CONTACTS, MARINA_LOCATIONS, PIERS, SLIPS
  - Operations: RESERVATIONS, INVOICES, INVOICE_ITEMS, TRANSACTIONS
  - Products: ITEM_MASTERS, SEASONAL_PRICES, TRANSIENT_PRICES
  - Reference: BOAT_TYPES, POWER_NEEDS, INVOICE_STATUS, TRANSACTION_TYPES, etc.

- **Stellar (29 tables)**:
  - Core: CUSTOMERS, LOCATIONS, BOOKINGS, BOOKING_BOATS, BOOKING_PAYMENTS
  - Style/Boats: STYLE_GROUPS, STYLES, STYLE_BOATS, CUSTOMER_BOATS
  - Pricing: SEASONS, SEASON_DATES, STYLE_HOURLY_PRICES, STYLE_TIMES, STYLE_PRICES
  - Accessories: ACCESSORIES, ACCESSORY_OPTIONS, ACCESSORY_TIERS, BOOKING_ACCESSORIES
  - Sales: POS_ITEMS, POS_SALES, FUEL_SALES
  - Membership: CLUB_TIERS, COUPONS
  - Operations: WAITLISTS, CLOSED_DATES, HOLIDAYS, BLACKLISTS
  - Reference: CATEGORIES, AMENITIES

**Procedure Template**:
```sql
CREATE OR REPLACE PROCEDURE SP_MERGE_{SYSTEM}_{TABLE}
IS
    v_merged NUMBER := 0;
BEGIN
    MERGE INTO DW_{SYSTEM}_{TABLE} tgt
    USING STG_{SYSTEM}_{TABLE} src
    ON (tgt.{PK_COLUMN} = src.{PK_COLUMN})
    WHEN MATCHED THEN
        UPDATE SET
            tgt.COL1 = src.COL1,
            tgt.COL2 = src.COL2,
            ...
            tgt.DW_LAST_UPDATED = SYSTIMESTAMP
    WHEN NOT MATCHED THEN
        INSERT (COL1, COL2, ..., DW_LAST_INSERTED, DW_LAST_UPDATED)
        VALUES (src.COL1, src.COL2, ..., SYSTIMESTAMP, SYSTIMESTAMP);
    
    v_merged := SQL%ROWCOUNT;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('DW_{SYSTEM}_{TABLE}: Merged ' || v_merged || ' records');
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;
```

**Master Procedure**:
```sql
CREATE OR REPLACE PROCEDURE SP_RUN_ALL_MOLO_STELLAR_MERGES
IS
BEGIN
    -- Execute all 35 MOLO merge procedures
    SP_MERGE_MOLO_ACCOUNTS;
    SP_MERGE_MOLO_BOATS;
    ...
    
    -- Execute all 29 Stellar merge procedures
    SP_MERGE_STELLAR_CUSTOMERS;
    SP_MERGE_STELLAR_BOOKINGS;
    ...
    
    COMMIT;
END;
```

**Usage**:
```bash
python3 generate_merge_procedures_smart.py
```

**Output**:
- `stored_procedures/sp_merge_molo_*.sql` (35 files)
- `stored_procedures/sp_merge_stellar_*.sql` (29 files)
- `stored_procedures/sp_run_all_merges.sql` (master)
- `stored_procedures/deploy_all_procedures.sql` (combined)

---

#### `deploy_procedures_simple.py`
**Purpose**: Deploy all 65 stored procedures to Oracle database

**What it does**:
1. Connects to Oracle database
2. Reads `stored_procedures/deploy_all_procedures.sql`
3. Splits into individual CREATE PROCEDURE statements
4. Executes each procedure with error handling
5. Reports success/failure for each procedure

**Key Features**:
- Parses procedure names from SQL statements
- Handles statement delimiters (`/`)
- Skips comment lines (`--`, `PROMPT`)
- Tracks deployment success count

**Known Issue Fix**:
- **Problem**: ACCOUNTS procedure failed due to empty first line in SQL file
- **Solution**: Parser now handles leading whitespace correctly
- **Status**: All 65 procedures now deploy successfully

**Usage**:
```bash
python3 deploy_procedures_simple.py
```

**Output**:
```
======================================================================
Deploying Stored Procedures
======================================================================

Connecting to oax4504110443_low...
✅ Connected

Reading stored_procedures/deploy_all_procedures.sql...
Found 65 procedures to deploy

[1/65] Deploying SP_MERGE_MOLO_ACCOUNTS... ✅
[2/65] Deploying SP_MERGE_MOLO_BOATS... ✅
...
[64/65] Deploying SP_MERGE_STELLAR_BLACKLISTS... ✅
[65/65] Deploying SP_RUN_ALL_MOLO_STELLAR_MERGES... ✅

======================================================================
✅ Deployed: 65
======================================================================

To execute merges, run:
  python3 run_merges.py
```

---

#### `stored_procedures/` Directory
**Contents**: All 65 generated stored procedure SQL files

**File Organization**:
```
stored_procedures/
├── sp_merge_molo_accounts.sql
├── sp_merge_molo_boats.sql
├── sp_merge_molo_companies.sql
├── sp_merge_molo_contacts.sql
├── sp_merge_molo_invoices.sql
├── sp_merge_molo_transactions.sql
├── ... (29 more MOLO procedures)
├── sp_merge_stellar_customers.sql
├── sp_merge_stellar_locations.sql
├── sp_merge_stellar_bookings.sql
├── sp_merge_stellar_booking_boats.sql
├── sp_merge_stellar_styles.sql
├── ... (24 more Stellar procedures)
├── sp_run_all_merges.sql (master orchestrator)
└── deploy_all_procedures.sql (combined file for deployment)
```

**Each Procedure**:
- **Input**: STG_* staging table
- **Output**: DW_* data warehouse table
- **Logic**: MERGE (UPDATE existing, INSERT new)
- **Tracking**: DW_LAST_INSERTED, DW_LAST_UPDATED
- **Error Handling**: ROLLBACK on exception

---

## Database Schema

### Staging Tables (STG_*)
**Purpose**: Temporary staging area for raw S3 data

**Pattern**: `STG_{SYSTEM}_{TABLE}`

**Characteristics**:
- Exact copy of CSV structure
- TRUNCATE before each load
- No DW tracking columns
- No indexes (fast insert)

**Examples**:
- `STG_MOLO_MARINA_LOCATIONS` (12 columns)
- `STG_MOLO_CONTACTS` (43 columns)
- `STG_STELLAR_CUSTOMERS` (52 columns, USER_ID PK)
- `STG_STELLAR_BOOKINGS` (82 columns)

### Data Warehouse Tables (DW_*)
**Purpose**: Production data warehouse with history tracking

**Pattern**: `DW_{SYSTEM}_{TABLE}`

**Additional Columns**:
- `DW_ID` - Surrogate key (auto-increment)
- `DW_LAST_INSERTED` - Timestamp of first insert
- `DW_LAST_UPDATED` - Timestamp of last update

**Indexes**: Primary key on source ID + DW_ID

**Examples**:
- `DW_MOLO_MARINA_LOCATIONS` (15 columns = 12 data + 3 DW)
- `DW_STELLAR_CUSTOMERS` (55 columns = 52 data + 3 DW)

---

## Running the Application

### Complete ETL Pipeline
```bash
# 1. Process MOLO and Stellar data (downloads, stages, merges)
python3 download_csv_from_s3.py

# This internally executes:
# - Download MOLO ZIP from S3
# - Extract and parse 47 MOLO CSVs
# - TRUNCATE 47 STG_MOLO_* tables
# - INSERT into STG_MOLO_* tables
# - Download Stellar .gz files from S3
# - Decompress and parse 29 Stellar CSVs
# - TRUNCATE 29 STG_STELLAR_* tables
# - INSERT into STG_STELLAR_* tables
# - Execute SP_RUN_ALL_MOLO_STELLAR_MERGES
# - MERGE all 76 tables (47 MOLO + 29 Stellar: STG_* → DW_*)
```

### Individual System Processing
```bash
# MOLO only
python3 download_csv_from_s3.py --molo-only

# Stellar only
python3 download_csv_from_s3.py --stellar-only
```

### Manual Merge Execution
```bash
# If you want to re-run merges without re-downloading
python3 run_merges.py
```

### Stored Procedure Management
```bash
# Regenerate all 65 procedures from database schema
python3 generate_merge_procedures_smart.py

# Deploy all procedures to database
python3 deploy_procedures_simple.py
```

---

## Data Warehouse Tracking

### Insert vs Update Logic
The MERGE procedures implement intelligent upsert logic:

```sql
-- If record exists (matched on primary key): UPDATE
WHEN MATCHED THEN
    UPDATE SET
        tgt.COLUMN1 = src.COLUMN1,
        tgt.COLUMN2 = src.COLUMN2,
        tgt.DW_LAST_UPDATED = SYSTIMESTAMP  -- Track last update

-- If record is new (not matched): INSERT
WHEN NOT MATCHED THEN
    INSERT (columns..., DW_LAST_INSERTED, DW_LAST_UPDATED)
    VALUES (src.columns..., SYSTIMESTAMP, SYSTIMESTAMP)  -- Track insert
```

### Timestamp Tracking
- **DW_LAST_INSERTED**: Set once on first insert, never updated
- **DW_LAST_UPDATED**: Updated every time record changes
- **Use Case**: Track data freshness, audit changes, incremental reporting

---

## Error Handling & Logging

### Logging Configuration
**Dual Output**:
- Console: Real-time progress monitoring
- Files: 
  - `molo_processing.log` - MOLO ETL logs
  - `stellar_processing.log` - Stellar ETL logs

**Log Levels**:
- INFO: Normal operations, record counts
- WARNING: Missing data, non-fatal errors
- ERROR: Fatal errors, exceptions
- DEBUG: Detailed parsing information

### Common Error Scenarios

#### 1. S3 Connection Failure
**Error**: `NoCredentialsError: Unable to locate credentials`
**Solution**: Check `config.json` has valid AWS credentials

#### 2. Oracle Connection Failure
**Error**: `DPI-1047: Cannot locate a 64-bit Oracle Client library`
**Solution**: Install Oracle Instant Client at `/opt/oracle/instantclient`

#### 3. Wallet Not Found
**Error**: `TNS:could not resolve the connect identifier specified`
**Solution**: Ensure `wallet_demo/` directory exists with wallet files

#### 4. Column Mismatch
**Error**: `ORA-00904: invalid identifier`
**Solution**: Regenerate procedures with `generate_merge_procedures_smart.py`

#### 5. Primary Key Violation
**Error**: `ORA-00001: unique constraint violated`
**Solution**: Check for duplicate IDs in source CSV, investigate data quality

---

## Deployment Checklist

### Prerequisites
- [ ] Python 3.8+
- [ ] Oracle Instant Client 21.x
- [ ] Oracle wallet files in `wallet_demo/`
- [ ] AWS credentials with S3 read access
- [ ] Oracle database credentials with DDL/DML permissions

### Configuration
- [ ] Copy `config.json.template` to `config.json`
- [ ] Update AWS credentials in `config.json`
- [ ] Update Oracle credentials in `config.json`
- [ ] Verify S3 bucket names
- [ ] Test Oracle connection with SQL*Plus

### Database Setup
- [ ] Create all 152 tables (76 STG_* + 76 DW_*)
- [ ] Generate 65 stored procedures
- [ ] Deploy stored procedures
- [ ] Verify all procedures are VALID

### First Run
- [ ] Test MOLO-only processing
- [ ] Test Stellar-only processing
- [ ] Verify staging tables populated
- [ ] Verify data warehouse tables updated
- [ ] Check DW_LAST_INSERTED/UPDATED timestamps

---

## Monitoring & Maintenance

### Daily Operations
1. Run ETL pipeline: `python3 download_csv_from_s3.py`
2. Check logs for errors
3. Verify record counts in DW_* tables
4. Monitor S3 bucket for new files

### Weekly Maintenance
1. Review data quality issues in logs
2. Check for new tables in source systems
3. Regenerate procedures if schema changes
4. Archive old log files

### Schema Evolution
If source CSV structure changes:
1. Update staging table DDL
2. Update data warehouse table DDL
3. Regenerate merge procedures: `python3 generate_merge_procedures_smart.py`
4. Redeploy procedures: `python3 deploy_procedures_simple.py`
5. Update parser functions in Python scripts

---

## Performance Optimization

### Current Design
- **Batch Inserts**: Using `executemany()` for bulk loading
- **Minimal Indexes**: STG_* tables have no indexes for fast insert
- **MERGE vs INSERT**: DW_* tables use MERGE for upsert efficiency
- **Connection Pooling**: Single connection per ETL run

### Potential Improvements
- **Parallel Processing**: Process MOLO and Stellar in parallel threads
- **Partitioning**: Partition DW_* tables by date for large datasets
- **Incremental Load**: Load only changed records (requires change tracking)
- **Compression**: Enable table compression for DW_* tables

---

## Troubleshooting Guide

### Issue: All procedures fail to deploy
**Symptom**: `deploy_procedures_simple.py` reports 0 deployed
**Diagnosis**: Check SQL syntax in `deploy_all_procedures.sql`
**Solution**: Regenerate procedures with `generate_merge_procedures_smart.py`

### Issue: ACCOUNTS procedure fails
**Symptom**: Parser error "list index out of range"
**Diagnosis**: Empty first line in SQL file
**Solution**: Fixed in current version - remove leading blank lines

### Issue: Stellar module not available
**Symptom**: `STELLAR_AVAILABLE = False` in logs
**Diagnosis**: `download_stellar_from_s3.py` not found
**Solution**: Verify file exists and is in same directory

### Issue: No data in DW_* tables
**Symptom**: STG_* tables populated but DW_* empty
**Diagnosis**: Stored procedures not executed or failed
**Solution**: Manually run `SP_RUN_ALL_MOLO_STELLAR_MERGES` in SQL*Plus

---

## Security Considerations

### Credential Management
- **Never commit**: `config.json` to version control
- **Use templates**: Commit `config.json.template` with placeholder values
- **Rotate credentials**: Update credentials quarterly
- **Least privilege**: Use database user with minimal required permissions

### Wallet Security
- **Encrypt wallet**: Use wallet password in production
- **Restrict access**: Set file permissions to 600 on wallet files
- **Secure transfer**: Use SCP/SFTP when moving wallet files
- **Version control**: Never commit wallet files

### AWS Security
- **IAM policies**: Restrict S3 access to specific buckets
- **MFA**: Enable multi-factor authentication for AWS console
- **Access logs**: Enable S3 access logging
- **Encryption**: Enable S3 server-side encryption

---

## Support & Documentation

### Key Documentation Files
- `APPLICATION_GUIDE.md` - This file
- `README.md` - Quick start guide
- `CONFIG_FILE_GUIDE.md` - Configuration reference
- `STORED_PROCEDURES_GUIDE.md` - Procedure documentation
- `CSV_TO_DB_COLUMN_MAPPINGS.md` - Column mapping reference

### Additional Resources
- Oracle Autonomous Database documentation
- Oracle Python Driver (oracledb) documentation
- AWS SDK for Python (boto3) documentation
- S3 bucket file listings for latest data

---

## Version History

### Current Version: 2.0
- **Date**: November 12, 2025
- **Changes**:
  - ✅ All 65 stored procedures deploying successfully
  - ✅ Fixed ACCOUNTS procedure empty line issue
  - ✅ Stellar module with correct CSV column mappings
  - ✅ **All 29 Stellar parser functions operational** (completed 20 additional parsers)
  - ✅ Comprehensive logging to file + console
  - ✅ Master merge procedure orchestrating all 64 child procedures

### Known Limitations
- No incremental loading - full refresh only
- No data quality validation rules
- No error notification system

### Planned Enhancements
- Implement incremental change detection
- Add email alerts for ETL failures
- Create data quality validation framework
- Build BI dashboard for monitoring

---

*Last Updated: November 12, 2025*
*Maintained by: Stefan Holodnick*
