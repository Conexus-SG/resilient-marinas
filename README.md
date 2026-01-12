# AWS Marina Data ETL Pipeline

## Overview

This application is an **ETL (Extract, Transform, Load) pipeline** that synchronizes marina management data from two source systems:

1. **MOLO Marina System** - Marina management (slips, reservations, boats, invoices, etc.)
2. **Stellar Business System** - Boat rental operations (bookings, payments, customers, pricing, etc.)

The pipeline downloads data from **AWS S3 buckets**, processes it, and loads it into an **Oracle Autonomous Database** data warehouse for business intelligence and reporting.

---

## Project Structure

```
aws-retrieve-csv/
├── Configuration & Environment
│   ├── config.json                 - Database and S3 credentials (DO NOT COMMIT)
│   ├── config.json.template        - Template for config.json
│   ├── .env                        - Environment variables (DO NOT COMMIT)
│   ├── .env.template               - Template for .env
│   ├── .gitignore                  - Git ignore rules
│   └── .dockerignore               - Docker build ignore rules
│
├── Core ETL Scripts
│   ├── download_csv_from_s3.py     - Main ETL orchestrator (MOLO processor)
│   ├── download_stellar_from_s3.py - Stellar system data processor
│   ├── molo_db_functions.py        - MOLO database connector and operations
│   ├── stellar_db_functions.py     - Stellar database connector and operations
│   └── data_validator.py           - CSV field and merge change validator
│
├── Deployment & Procedures
│   ├── deploy_procedures.py        - Deploy stored procedures to database
│   └── stored_procedures/          - SQL stored procedure files (79 total)
│       ├── sp_merge_molo_*.sql    - MOLO table merge procedures (48 files)
│       ├── sp_merge_stellar_*.sql - Stellar table merge procedures (29 files)
│       ├── sp_run_all_merges.sql  - Master orchestrator procedure
│       └── deploy_all_procedures.sql - Combined deployment script
│
├── Database Schema DDL
│   ├── tables/
│   │   ├── oracle_molo_staging_tables.sql     - STG_MOLO_* table definitions
│   │   ├── oracle_molo_business_tables.sql    - DW_MOLO_* table definitions
│   │   ├── oracle_stellar_staging_tables.sql  - STG_STELLAR_* table definitions
│   │   └── oracle_stellar_business_tables.sql - DW_STELLAR_* table definitions
│   │
│   └── views/
│       ├── dw_molo_daily_boat_lengths_vw.sql
│       ├── dw_molo_daily_slip_count_vw.sql
│       ├── dw_molo_daily_slip_occupancy_vw.sql
│       ├── dw_molo_rate_over_linear_foot.sql
│       ├── dw_molo_rate_over_linear_foot_vw.sql
│       ├── dw_stellar_daily_rentals_vw.sql
│       └── DW_NS_X_FIN_*.sql (15 financial reporting views)
│
├── Containerization
│   ├── Dockerfile                  - Container image definition
│   ├── docker-compose.yml          - Multi-container orchestration
│   └── deploy-oci.sh               - Oracle Cloud Infrastructure deployment
│
├── Dependencies & Logs
│   ├── requirements.txt            - Python package dependencies
│   ├── stellar_processing.log      - Stellar ETL execution logs
│   ├── error_log.txt               - Error log from recent runs
│   │
│   ├── wallet/                     - Oracle Autonomous Database wallet
│   │   ├── cwallet.sso
│   │   ├── tnsnames.ora
│   │   └── sqlnet.ora
│   │
│   ├── utils/                      - Utility files (environment configs)
│   │   └── oci-compartment.env
│   │
│   └── __pycache__/                - Python cache (generated)
│
├── Documentation
│   ├── README.md                   - This file
│   └── .git/                       - Git repository
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         AWS S3 Buckets                              │
├────────────────────────────────┬────────────────────────────────────┤
│  MOLO System                   │  Stellar Business System           │
│  Bucket: cnxtestbucket         │  Bucket: resilient-ims-backups     │
│  Format: ZIP files with CSVs   │  Format: .gz DATA files with CSVs  │
│  Files: 48 MOLO tables         │  Files: 29 Stellar tables          │
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
│  ├── STG_MOLO_*    (48)        ├── DW_MOLO_*    (48)               │
│  └── STG_STELLAR_* (29)        └── DW_STELLAR_* (29)               │
│                                                                     │
│  Stored Procedures: 79 total                                       │
│  ├── SP_MERGE_MOLO_*    (48 procedures)                            │
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
- **INSERT** fresh data into STG_MOLO_* (48 tables) and STG_STELLAR_* (29 tables)

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
**Purpose**: Centralized database and S3 credential management

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

#### `.env` and `.env.template`
**Purpose**: Environment variables for containerized deployments and system configuration

**Usage**: For Docker containers and OCI deployments. Template provided for reference.

**Security**: Never commit `.env` to version control.

#### `wallet/`
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
5. Extracts and processes 48 MOLO CSV files:
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
- Wallet Location: `./wallet`
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

### Utility & Validation Scripts

#### `data_validator.py`
**Purpose**: CSV field-level and merge operation validation

**What it does**:
1. Compares CSV values against database staging tables to detect data corruption
2. Validates merge operations by comparing staging (STG_*) and data warehouse (DW_*) tables
3. Reports field-by-field discrepancies and unexpected merge changes
4. Supports configurable sampling for performance

**Key Functions**:
- Field validation: Detect CSV corruption before database loading
- Merge validation: Ensure stored procedures don't modify data unexpectedly
- Sampling: Validate subset of records for large datasets

**Usage** (called from `download_csv_from_s3.py`):
```bash
# Enable field-level validation
python3 download_csv_from_s3.py --validate-fields

# Enable merge change validation
python3 download_csv_from_s3.py --validate-merge-changes

# Full validation with custom sample size
python3 download_csv_from_s3.py \
    --validate-fields \
    --validate-merge-changes \
    --validation-sample-size 20
```

---

### Stored Procedure Management

#### `deploy_procedures.py`
**Purpose**: Deploy generated stored procedures to Oracle database

**What it does**:
1. Connects to Oracle database
2. Reads stored procedure SQL files from `stored_procedures/` directory
3. Executes each CREATE OR REPLACE PROCEDURE statement
4. Tracks deployment success/failure
5. Reports overall deployment status

**Usage**:
```bash
python3 deploy_procedures.py
```

**Output**: Deployment status for each of the 65 procedures

---

#### `stored_procedures/` Directory
**Contents**: 65 generated stored procedure SQL files plus deployment scripts

**File Organization**:
```
stored_procedures/
├── MOLO Procedures (48 files)
│   ├── sp_merge_molo_accounts.sql
│   ├── sp_merge_molo_boats.sql
│   ├── sp_merge_molo_companies.sql
│   ├── sp_merge_molo_contacts.sql
│   ├── sp_merge_molo_invoices.sql
│   ├── sp_merge_molo_transactions.sql
│   ├── sp_merge_molo_reservations.sql
│   ├── sp_merge_molo_marina_locations.sql
│   ├── sp_merge_molo_piers.sql
│   ├── sp_merge_molo_slips.sql
│   ├── sp_merge_molo_item_masters.sql
│   ├── sp_merge_molo_seasonal_prices.sql
│   ├── sp_merge_molo_transient_prices.sql
│   ├── sp_merge_molo_vessel_engine_class.sql
│   └── ... (33 more MOLO procedures)
│
├── Stellar Procedures (29 files)
│   ├── sp_merge_stellar_customers.sql
│   ├── sp_merge_stellar_locations.sql
│   ├── sp_merge_stellar_bookings.sql
│   ├── sp_merge_stellar_booking_boats.sql
│   ├── sp_merge_stellar_booking_payments.sql
│   ├── sp_merge_stellar_styles.sql
│   ├── sp_merge_stellar_style_boats.sql
│   ├── sp_merge_stellar_style_groups.sql
│   ├── sp_merge_stellar_accessories.sql
│   ├── sp_merge_stellar_amenities.sql
│   ├── sp_merge_stellar_categories.sql
│   ├── sp_merge_stellar_seasons.sql
│   ├── sp_merge_stellar_club_tiers.sql
│   ├── sp_merge_stellar_coupons.sql
│   └── ... (14 more Stellar procedures)
│
├── Master Procedure
│   ├── sp_run_all_merges.sql
│   └── sp_run_all_molo_stellar_merges.sql (alternative naming)
│
└── Deployment Scripts
    ├── deploy_all_procedures.sql (combined deployment)
    └── deploy_updated_procedures.sql (incremental deployment)
```

**Each Procedure**:
- **Input**: STG_* staging table
- **Output**: DW_* data warehouse table
- **Logic**: MERGE (UPDATE existing, INSERT new)
- **Tracking**: DW_LAST_INSERTED, DW_LAST_UPDATED timestamps
- **Error Handling**: ROLLBACK on exception

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

---

### Database Schema DDL

#### `tables/` Directory
**Purpose**: SQL DDL scripts for creating all database tables

**Contents**:
- `oracle_molo_staging_tables.sql` - Creates 48 STG_MOLO_* tables
- `oracle_molo_business_tables.sql` - Creates 48 DW_MOLO_* tables with DW tracking columns
- `oracle_stellar_staging_tables.sql` - Creates 29 STG_STELLAR_* tables
- `oracle_stellar_business_tables.sql` - Creates 29 DW_STELLAR_* tables with DW tracking columns

**Table Naming Convention**:
- Staging: `STG_{SYSTEM}_{TABLE}` (exact CSV structure)
- Data Warehouse: `DW_{SYSTEM}_{TABLE}` (CSV structure + DW tracking)

**DW Tracking Columns** (added to all DW_* tables):
- `DW_ID` - Surrogate key (auto-increment primary key)
- `DW_LAST_INSERTED` - Timestamp of first insert
- `DW_LAST_UPDATED` - Timestamp of last update

#### `views/` Directory
**Purpose**: Business intelligence and reporting views

**Contents** (21 total views):

**MOLO Marina Views** (5 views):
- `dw_molo_daily_boat_lengths_vw.sql` - Daily boat length distribution analytics
- `dw_molo_daily_slip_count_vw.sql` - Daily slip inventory count
- `dw_molo_daily_slip_occupancy_vw.sql` - Daily slip occupancy rates
- `dw_molo_rate_over_linear_foot.sql` - Rate calculation per linear foot
- `dw_molo_rate_over_linear_foot_vw.sql` - Rate over linear foot view

**Stellar Rental Views** (1 view):
- `dw_stellar_daily_rentals_vw.sql` - Daily rental activity analytics

**NetSuite Financial Reporting Views** (15 views):
- `DW_NS_X_FIN_CASHSALE_V.sql` - Cash sale transactions
- `DW_NS_X_FIN_CC_REFUND_V.sql` - Credit card refund transactions
- `DW_NS_X_FIN_CHECK_V.sql` - Check payment records
- `DW_NS_X_FIN_CREDIT_CARD_V.sql` - Credit card transactions
- `DW_NS_X_FIN_CUST_CREDIT_V.sql` - Customer credit transactions
- `DW_NS_X_FIN_CUST_PAYMENT_V.sql` - Customer payment records
- `DW_NS_X_FIN_CUST_REFUND_V.sql` - Customer refund records
- `DW_NS_X_FIN_DEPENTRY_V.sql` - Deposit entry records
- `DW_NS_X_FIN_DEPOSIT_V.sql` - Deposit transactions
- `DW_NS_X_FIN_INVOICE_V.sql` - Invoice records
- `DW_NS_X_FIN_JOURNAL_V.sql` - Journal entry records
- `DW_NS_X_FIN_REPORT_V.sql` - Financial reporting view
- `DW_NS_X_FIN_VENDBILL_PAYMENT_V.sql` - Vendor bill payment records
- `DW_NS_X_FIN_VENDBILL_V.sql` - Vendor bill records
- `DW_NS_X_FIN_VENDCRED_V.sql` - Vendor credit records

---

### Containerization & Deployment

#### `Dockerfile`
**Purpose**: Define containerized Python ETL application environment

**Features**:
- Python 3.x runtime
- Oracle Instant Client pre-installed
- Python dependencies from requirements.txt
- Wallet integration for database access

#### `docker-compose.yml`
**Purpose**: Orchestrate multi-container deployments

**Usage**:
```bash
docker-compose up
```

#### `deploy-oci.sh`
**Purpose**: Deploy containerized application to Oracle Cloud Infrastructure

**Features**:
- OCI Container Registry integration
- Environment variable configuration
- Database wallet mounting
- Scheduled ETL execution

---

### Python Dependencies

#### `requirements.txt`
**Purpose**: List all Python package dependencies

**Key Dependencies**:
- `oracledb` - Oracle Python driver
- `boto3` - AWS SDK for S3 access
- `pandas` - Data manipulation
- `python-dotenv` - Environment variable management

---

### Logging

#### `stellar_processing.log`
**Purpose**: Execution log from Stellar ETL processing

#### `error_log.txt`
**Purpose**: Error log from recent ETL runs

---

### Additional Files

#### `utils/` Directory
**Purpose**: Utility files and configuration

**Contents**:
- `oci-compartment.env` - Oracle Cloud Infrastructure compartment configuration

#### `.gitignore`
**Purpose**: Prevent committing sensitive files to version control

**Excludes**:
- `config.json` - Database credentials
- `.env` - Environment variables
- `wallet/` - Oracle wallet files
- `*.log` - Log files
- `__pycache__/` - Python cache

#### `.dockerignore`
**Purpose**: Prevent unnecessary files from being copied into Docker images

---

## Running the Application

### Complete ETL Pipeline
```bash
# Process MOLO and Stellar data (downloads, stages, merges)
python3 download_csv_from_s3.py

# With validation enabled
python3 download_csv_from_s3.py --validate-fields --validate-merge-changes
```

**What happens internally**:
- Download MOLO ZIP from S3 and extract 43 CSVs
- Download Stellar .gz files from S3 and decompress 20 CSVs
- TRUNCATE existing staging tables (STG_* tables)
- INSERT fresh data into staging tables
- Execute SP_RUN_ALL_MERGES (master orchestrator)
- MERGE all staging data into data warehouse tables (STG_* → DW_*)

### Deployment & Configuration
```bash
# Deploy stored procedures to database
python3 deploy_procedures.py
```

---

## Data Warehouse Tracking

The system tracks data freshness through two audit timestamps on all DW_* tables:

- **DW_LAST_INSERTED**: Timestamp of initial insert (never updated)
- **DW_LAST_UPDATED**: Timestamp of most recent insert or update

**MERGE Logic**:
- **Existing Records**: UPDATE all columns, refresh DW_LAST_UPDATED
- **New Records**: INSERT with both timestamps set to current time

**Use Cases**:
- Track data recency for reporting
- Audit change history
- Enable incremental load strategies

---

## Error Handling & Logging

### Log Files
- **MOLO Processing**: Logged during `download_csv_from_s3.py` execution
- **Stellar Processing**: Saved to `stellar_processing.log`
- **Errors**: Captured in `error_log.txt`

### Common Issues & Solutions

#### S3 Connection Failure
**Error**: `NoCredentialsError: Unable to locate credentials`
**Solution**: Verify AWS credentials in `config.json`

#### Oracle Connection Failure
**Error**: `DPI-1047: Cannot locate a 64-bit Oracle Client library`
**Solution**: Install Oracle Instant Client; configure wallet path

#### Wallet/TNS Error
**Error**: `TNS:could not resolve the connect identifier specified`
**Solution**: Ensure `wallet/` directory contains required connection files

#### Database Write Failure
**Error**: `ORA-00904: invalid identifier` or `ORA-00001: unique constraint violated`
**Solution**: Regenerate procedures with correct schema, validate CSV primary keys

---

## Deployment Checklist

### Prerequisites
- [ ] Python 3.8+
- [ ] Oracle Instant Client 21.x
- [ ] Oracle wallet files in `wallet/`
- [ ] AWS credentials with S3 read access
- [ ] Oracle database credentials with DDL/DML permissions

### Configuration
- [ ] Copy `config.json.template` to `config.json`
- [ ] Update AWS credentials in `config.json`
- [ ] Update Oracle credentials in `config.json`
- [ ] Verify S3 bucket names (MOLO: `cnxtestbucket`, Stellar: `resilient-ims-backups`)
- [ ] Test Oracle connection with SQL*Plus

### Database Setup
- [ ] Create all database tables using scripts in `tables/` directory
- [ ] Deploy all stored procedures: `python3 deploy_procedures.py`
- [ ] Verify all procedures compiled successfully
- [ ] Test merge logic with sample data

### First Run
- [ ] Test with `--validate-fields` flag to detect CSV corruption
- [ ] Verify staging (STG_*) tables populated correctly
- [ ] Verify data warehouse (DW_*) tables updated
- [ ] Check DW_LAST_INSERTED and DW_LAST_UPDATED timestamps

---

## Monitoring & Maintenance

### Daily Operations
1. Execute ETL pipeline: `python3 download_csv_from_s3.py`
2. Monitor `stellar_processing.log` and `error_log.txt` for issues
3. Verify record counts in DW_* tables increased appropriately
4. Check for new data files in S3 buckets

### Weekly Maintenance
1. Review data quality issues in error logs
2. Check for schema changes in source systems
3. Validate sample records using: `python3 download_csv_from_s3.py --validate-fields`
4. Archive old log files to save disk space

### When Source Schema Changes
1. Update table DDL in `tables/` directory
2. Deploy updated table definitions to database
3. Verify new columns are present in STG_* tables
4. Deploy new stored procedures

---

## Performance Tuning

### Current Implementation
- **Batch Loading**: Python script uses bulk insert for fast staging load
- **Index Strategy**: STG_* tables unindexed for speed; DW_* tables indexed
- **Merge Efficiency**: SQL MERGE statement optimized for upsert operations
- **Connection**: Single persistent connection per ETL run

### Optimization Opportunities
- **Parallel Processing**: Load MOLO and Stellar in parallel threads
- **Incremental Loads**: Track and load only changed records
- **Compression**: Enable table compression on large DW_* tables
- **Partitioning**: Partition by date for faster queries

---

## Security Considerations

### Credential Management
- **Version Control**: Never commit `config.json` or `.env` files
- **Use Templates**: Reference `config.json.template` and `.env.template`
- **Rotation**: Update credentials quarterly or after team changes
- **Least Privilege**: Database user should have minimal required permissions

### Wallet Security
- **Encryption**: Use wallet password in production environments

- **File Permissions**: Set wallet files to 600 (read-only)
- **Secure Transport**: Use SCP/SFTP when transferring wallet files
- **No Version Control**: Never commit wallet files to git

### AWS Security
- **IAM Policies**: Restrict S3 access to specific buckets
- **Multi-Factor Auth**: Enable MFA for AWS console
- **Access Logging**: Enable S3 access logging for audit trail
- **Encryption**: Enable S3 server-side encryption (SSE-S3 or SSE-KMS)

---

## Documentation & Support

### Project Files
- `README.md` - This file (project overview)
- `requirements.txt` - Python package dependencies
- `Dockerfile` - Container image definition
- `docker-compose.yml` - Multi-container orchestration
- `deploy-oci.sh` - Oracle Cloud deployment script

### Data Files  
- `tables/` - Database table DDL scripts
- `views/` - Analytics and reporting views
- `stored_procedures/` - MERGE procedure definitions
- `wallet/` - Oracle database connection credentials

### Configuration
- `config.json.template` - AWS and database credential template
- `.env.template` - Environment variable template
- `.gitignore` - Git exclusions for sensitive files

### External Resources
- [Oracle Autonomous Database Documentation](https://docs.oracle.com/en/cloud/paas/autonomous-data-warehouse-cloud/)
- [Python oracledb Driver](https://python-oracledb.readthedocs.io/)
- [AWS SDK for Python (boto3)](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
- [Oracle MERGE Statement](https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/MERGE.html)

---

## Version History

### Version 2.0 - December 2025
**Current Release**

**Changes**:
- ✅ Project structure updated to reflect current file organization
- ✅ 43 MOLO stored procedures (sp_merge_molo_*.sql)
- ✅ 20 Stellar stored procedures (sp_merge_stellar_*.sql)
- ✅ Database schema DDL in `tables/` directory
- ✅ Analytics views in `views/` directory
- ✅ Docker containerization support
- ✅ OCI deployment script
- ✅ Data validation framework
- ✅ Comprehensive logging

**System Coverage**:
- **MOLO**: 48 tables (marina management, invoicing, transactions)
- **Stellar**: 29 tables (boat rental bookings, pricing, customers)
- **Total**: 77 data tables + 21 views for analytics and reporting

### Known Limitations
- Full refresh only (no incremental loading)
- Manual procedure regeneration required for schema changes
- Single-threaded processing (sequential MOLO → Stellar)

### Future Enhancements
- Incremental load with change tracking
- Parallel processing for MOLO and Stellar
- Automated alerting on ETL failures
- Enhanced data quality validation
- Business intelligence dashboard

---

*Last Updated: January 11, 2026*
*Maintained by: Stefan Holodnick*
