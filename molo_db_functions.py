"""
MOLO Database Functions Module

This module contains Oracle database connector and all merge functions 
for MOLO marina management system tables. Extracted from download_csv_from_s3.py
for better code organization and maintainability.

Contains 47 merge functions for comprehensive marina data management including:
- Core entities: marina_locations, companies, contacts, boats, accounts
- Operations: invoices, invoice_items, transactions, reservations
- Product data: item_masters, seasonal_prices, transient_prices
- Reference data: boat_types, power_needs, invoice_status, transaction_types
- And many more lookup tables and configuration entities

Dependencies:
    - oracledb: Oracle database connectivity
    - logging: For structured logging
    - os: For environment variable access
"""

import os
import logging
import oracledb
from datetime import datetime

# Set up logging
logger = logging.getLogger(__name__)


class OracleConnector:
    """
    Oracle Database connector with support for Oracle Autonomous Database.
    
    This class handles database connections using Oracle Instant Client and
    provides methods for MERGE operations on marina-related tables.
    
    Attributes:
        connection: Oracle database connection object
        cursor: Database cursor for executing SQL statements
    """
    
    def __init__(self, user, password, dsn):
        """
        Initialize Oracle database connection.
        
        Args:
            user (str): Database username
            password (str): Database password  
            dsn (str): Database data source name (connection string)
        """
        # Debug: Log TNS_ADMIN before setup
        logger.info(f"🔍 TNS_ADMIN before setup: {os.environ.get('TNS_ADMIN', 'NOT SET')}")
        
        # Configure Oracle wallet for Autonomous Database
        self._setup_oracle_wallet()
        
        # Initialize Oracle Instant Client
        self._initialize_oracle_client()
        
        # Establish database connection
        logger.info("Attempting to connect to Oracle database...")
        logger.info(f"   User: {user}")
        logger.info(f"   DSN: {dsn}")
        
        self.connection = oracledb.connect(
            user=user,
            password=password,
            dsn=dsn
        )
        logger.info("✅ Oracle database connection successful!")
        self.cursor = self.connection.cursor()
    
    def _setup_oracle_wallet(self):
        """Set up Oracle wallet environment for Autonomous Database."""
        # Check if we're in a container by looking for /.dockerenv or checking if Oracle Instant Client has wallet files
        container_wallet = '/opt/oracle/instantclient/network/admin'
        is_container = (os.path.exists('/.dockerenv') or 
                       (os.path.exists(container_wallet) and 
                        os.path.exists(os.path.join(container_wallet, 'cwallet.sso'))))
        
        if is_container:
            # Force container path
            wallet_dir = container_wallet
            os.environ['TNS_ADMIN'] = wallet_dir
            logger.info(f"✅ Container detected - TNS_ADMIN forced to: {wallet_dir}")
        elif 'TNS_ADMIN' in os.environ:
            # Use existing TNS_ADMIN if set
            wallet_dir = os.environ['TNS_ADMIN']
            logger.info(f"✅ TNS_ADMIN already set to: {wallet_dir}")
        else:
            # Get absolute path to wallet directory (for local development)
            script_dir = os.path.dirname(os.path.abspath(__file__))
            wallet_dir = os.path.join(script_dir, "wallet_demo")
            wallet_dir = os.path.abspath(wallet_dir)
            os.environ['TNS_ADMIN'] = wallet_dir
            logger.info(f"✅ TNS_ADMIN set to: {wallet_dir}")
        
        # Verify wallet directory exists
        if os.path.exists(wallet_dir):
            # List all files in wallet directory for debugging
            try:
                files_in_wallet = os.listdir(wallet_dir)
                logger.info(f"📁 Files in wallet directory: {files_in_wallet}")
            except Exception as e:
                logger.warning(f"⚠️  Could not list wallet directory: {e}")
            
            # Verify wallet files exist
            required_files = ['cwallet.sso', 'tnsnames.ora', 'sqlnet.ora']
            missing_files = [f for f in required_files if not os.path.exists(os.path.join(wallet_dir, f))]
            
            if missing_files:
                logger.warning(f"⚠️  Missing wallet files: {missing_files}")
            else:
                logger.info("✅ All required wallet files found")
                
            # Read and log tnsnames.ora content for debugging
            try:
                tnsnames_path = os.path.join(wallet_dir, 'tnsnames.ora')
                with open(tnsnames_path, 'r') as f:
                    tnsnames_content = f.read()
                logger.info(f"📄 tnsnames.ora first 200 chars: {tnsnames_content[:200]}")
            except Exception as e:
                logger.warning(f"⚠️  Could not read tnsnames.ora: {e}")
        else:
            logger.error(f"❌ Wallet directory not found: {wallet_dir}")
            raise FileNotFoundError(f"Wallet directory not found: {wallet_dir}")
    
    def _initialize_oracle_client(self):
        """Initialize Oracle Instant Client with common installation paths."""
        try:
            # Try different common paths for Oracle Instant Client
            client_paths = [
                "/opt/oracle/instantclient",           # Linux/Docker
                r"C:\oracle\instantclient_21_3",       # Windows
                r"C:\oracle\instantclient"             # Windows alternative
            ]
            
            for path in client_paths:
                if os.path.exists(path):
                    oracledb.init_oracle_client(lib_dir=path)
                    logger.info(f"Oracle Instant Client initialized from: {path}")
                    break
            else:
                logger.warning(
                    "Oracle Instant Client path not found, trying without lib_dir"
                )
                oracledb.init_oracle_client()
                
        except Exception as e:
            logger.warning(f"Oracle client already initialized or error: {e}")
    
    def truncate_staging_tables(self):
        """
        Truncate all MOLO staging tables before data load.
        
        This method clears all staging tables in preparation for a fresh data load.
        Following the rosnet-api-integration pattern: truncate, insert to staging,
        then call stored procedures to merge into data warehouse.
        """
        staging_tables = [
            'STG_MOLO_MARINA_LOCATIONS', 'STG_MOLO_PIERS', 'STG_MOLO_SLIP_TYPES',
            'STG_MOLO_SLIPS', 'STG_MOLO_RESERVATIONS', 'STG_MOLO_COMPANIES',
            'STG_MOLO_CONTACTS', 'STG_MOLO_BOATS', 'STG_MOLO_ACCOUNTS',
            'STG_MOLO_INVOICES', 'STG_MOLO_INVOICE_ITEMS', 'STG_MOLO_TRANSACTIONS',
            'STG_MOLO_ITEM_MASTERS', 'STG_MOLO_SEASONAL_PRICES', 'STG_MOLO_TRANSIENT_PRICES',
            'STG_MOLO_RECORD_STATUS', 'STG_MOLO_BOAT_TYPES', 'STG_MOLO_POWER_NEEDS',
            'STG_MOLO_RESERVATION_STATUS', 'STG_MOLO_RESERVATION_TYPES', 'STG_MOLO_CONTACT_TYPES',
            'STG_MOLO_INVOICE_STATUS', 'STG_MOLO_INVOICE_TYPES', 'STG_MOLO_TRANSACTION_TYPES',
            'STG_MOLO_TRANSACTION_METHODS', 'STG_MOLO_INSURANCE', 'STG_MOLO_EQUIPMENT',
            'STG_MOLO_ACCOUNT_STATUS', 'STG_MOLO_CONTACT_AUTO_CHARGE', 'STG_MOLO_STATEMENTS_PREFERENCE',
            'STG_MOLO_INVOICE_ITEM_TYPES', 'STG_MOLO_PAYMENT_METHODS', 'STG_MOLO_SEASONAL_CHARGE_METHODS',
            'STG_MOLO_SEASONAL_INVOICING_METHODS', 'STG_MOLO_TRANSIENT_CHARGE_METHODS',
            'STG_MOLO_TRANSIENT_INVOICING_METHODS', 'STG_MOLO_RECURRING_INVOICE_OPTIONS',
            'STG_MOLO_DUE_DATE_SETTINGS', 'STG_MOLO_ITEM_CHARGE_METHODS', 'STG_MOLO_INSURANCE_STATUS',
            'STG_MOLO_EQUIPMENT_TYPES', 'STG_MOLO_EQUIPMENT_FUEL_TYPES', 'STG_MOLO_VESSEL_ENGINE_CLASS',
            'STG_MOLO_CITIES', 'STG_MOLO_COUNTRIES', 'STG_MOLO_CURRENCIES',
            'STG_MOLO_PHONE_TYPES', 'STG_MOLO_ADDRESS_TYPES', 'STG_MOLO_INSTALLMENTS_PAYMENT_METHODS',
            'STG_MOLO_PAYMENTS_PROVIDER'
        ]
        
        logger.info("Truncating MOLO staging tables...")
        for table in staging_tables:
            try:
                self.cursor.execute(f"TRUNCATE TABLE {table}")
                logger.debug(f"✅ Truncated {table}")
            except Exception as e:
                logger.warning(f"Could not truncate {table}: {e}")
        
        self.connection.commit()
        logger.info(f"✅ Successfully truncated {len(staging_tables)} MOLO staging tables")
    
    def merge_single_table(self, table_name):
        """
        Execute a single merge stored procedure immediately after data load.
        
        Args:
            table_name (str): Name of the table (e.g., 'BOATS', 'CONTACTS', 'INVOICES')
                            Will be converted to procedure name SP_MERGE_MOLO_{table_name}
        
        Returns:
            dict: Merge statistics {'inserted': int, 'updated': int} or None if no OUT parameters
        """
        procedure_name = f'SP_MERGE_MOLO_{table_name.upper()}'
        
        try:
            # Enable DBMS_OUTPUT to capture procedure logging
            self.cursor.callproc("dbms_output.enable")
            
            # Check if procedure has OUT parameters (inserted_count, updated_count)
            # Try calling with OUT parameters first
            try:
                inserted = self.cursor.var(int)
                updated = self.cursor.var(int)
                self.cursor.callproc(procedure_name, [inserted, updated])
                
                # Fetch DBMS_OUTPUT
                line_var = self.cursor.var(str)
                status_var = self.cursor.var(int)
                while True:
                    self.cursor.callproc("dbms_output.get_line", (line_var, status_var))
                    if status_var.getvalue() != 0:
                        break
                    output_line = line_var.getvalue()
                    if output_line:
                        logger.info(f"  📋 {output_line}")
                
                self.connection.commit()
                
                result = {
                    'inserted': inserted.getvalue(),
                    'updated': updated.getvalue()
                }
                
                logger.info(
                    f"✅ {procedure_name}: "
                    f"{result['inserted']} inserted, {result['updated']} updated"
                )
                
                return result
                
            except Exception:
                # Procedure doesn't have OUT parameters (MERGE style), call without params
                self.cursor.callproc(procedure_name)
                
                # Fetch DBMS_OUTPUT
                line_var = self.cursor.var(str)
                status_var = self.cursor.var(int)
                while True:
                    self.cursor.callproc("dbms_output.get_line", (line_var, status_var))
                    if status_var.getvalue() != 0:
                        break
                    output_line = line_var.getvalue()
                    if output_line:
                        logger.info(f"  📋 {output_line}")
                
                self.connection.commit()
                logger.info(f"✅ {procedure_name}: Merge completed")
                
                return None
                
        except Exception as e:
            logger.error(f"❌ Error executing {procedure_name}: {e}")
            self.connection.rollback()
            raise
    
    def run_all_merges(self):
        """
        Execute the master stored procedure that merges all staging tables to data warehouse.
        
        Returns a dictionary of merge statistics for key tables:
        {
            'DW_MOLO_BOATS': {'inserted': 10, 'updated': 783},
            'DW_MOLO_CONTACTS': {'inserted': 5, 'updated': 1234},
            ...
        }
        
        This calls SP_RUN_ALL_MOLO_STELLAR_MERGES which internally calls all individual
        merge procedures for MOLO and Stellar systems. The stored procedures handle:
        - MERGE logic (UPDATE existing, INSERT new)
        - INSERTED_DATE and UPDATED_DATE management
        - Transaction commits
        """
        logger.info("Executing stored procedure to merge all MOLO staging data to data warehouse...")
        try:
            # Enable DBMS_OUTPUT to capture procedure logging
            self.cursor.callproc("dbms_output.enable")
            
            # Create cursor variable for OUT parameter
            stats_cursor = self.cursor.var(oracledb.DB_TYPE_CURSOR)
            
            # Call procedure with OUT parameter
            self.cursor.callproc('SP_RUN_ALL_MOLO_STELLAR_MERGES', [stats_cursor])
            
            # Fetch and log DBMS_OUTPUT from stored procedures
            line_var = self.cursor.var(str)
            status_var = self.cursor.var(int)
            while True:
                self.cursor.callproc("dbms_output.get_line", (line_var, status_var))
                if status_var.getvalue() != 0:
                    break
                output_line = line_var.getvalue()
                if output_line:
                    logger.info(f"  📋 {output_line}")
            
            # Fetch merge statistics from cursor
            merge_stats = {}
            result_cursor = stats_cursor.getvalue()
            if result_cursor:
                for row in result_cursor:
                    table_name, inserted_count, updated_count = row
                    merge_stats[table_name] = {
                        'inserted': inserted_count,
                        'updated': updated_count
                    }
                result_cursor.close()
            
            self.connection.commit()
            logger.info("✅ Successfully completed all MOLO merge operations")
            
            # Log summary of tracked tables
            if merge_stats:
                logger.info(f"📊 Captured statistics for {len(merge_stats)} key tables")
                for table in sorted(merge_stats.keys()):
                    stats = merge_stats[table]
                    logger.debug(
                        f"   {table}: {stats['inserted']} inserted, {stats['updated']} updated"
                    )
            
            return merge_stats
            
        except Exception as e:
            logger.warning(
                f"⚠️  Stored procedure SP_RUN_ALL_MOLO_STELLAR_MERGES not found or failed: {e}"
            )
            logger.warning(
                "   Data has been loaded into STG_MOLO_* staging tables successfully."
            )
            logger.warning(
                "   Create the stored procedures to merge STG_* → DW_* tables."
            )
            # Don't raise - let the process continue
            self.connection.rollback()
    
    def insert_marina_locations(self, data_rows):
        """
        Insert marina locations data into STG_MOLO_MARINA_LOCATIONS staging table.
        
        Args:
            data_rows (list): List of tuples containing marina location data
        """
        if not data_rows:
            logger.info("No marina location data to insert")
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_MARINA_LOCATIONS (
                ID, NAME, PRIMARY_PHONE_NUMBER, PRIMARY_FAX_NUMBER,
                ORGANIZATION_ID, MARINA_HASH, UNIT_SYSTEM,
                DEFAULT_ARRIVAL_TIME, DEFAULT_DEPARTURE_TIME,
                EMAIL_ADDRESS, MARINA_WEBSITE, TIME_ZONE
            )
            VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Inserted {len(data_rows)} marina location records to staging")
        except Exception as e:
            logger.exception(f"Error inserting marina locations to staging: {e}")
            self.connection.rollback()
            raise

    def insert_piers(self, data_rows):
        """
        Insert piers data into STG_MOLO_PIERS staging table.
        
        Args:
            data_rows (list): List of tuples containing pier data
        """
        if not data_rows:
            logger.info("No pier data to insert")
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_PIERS (
                id, name, MARINA_LOCATION_ID
            )
            VALUES (:1, :2, :3)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Inserted {len(data_rows)} pier records to staging")
            
            # Execute merge procedure immediately after insert
            logger.info("Executing merge for PIERS...")
            self.merge_single_table('PIERS')
            
        except Exception as e:
            logger.exception(f"Error inserting piers to staging: {e}")
            self.connection.rollback()
            raise

    def insert_slip_types(self, data_rows):
        """
        Insert slip types data into STG_MOLO_SLIP_TYPES staging table.
        
        Args:
            data_rows (list): List of tuples containing slip type data
        """
        if not data_rows:
            logger.info("No slip type data to insert")
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_SLIP_TYPES (
                id, name
            )
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Inserted {len(data_rows)} slip type records to staging")
        except Exception as e:
            logger.exception(f"Error inserting slip types to staging: {e}")
            self.connection.rollback()
            raise

    def insert_slips(self, data_rows):
        """
        Insert slips data into STG_MOLO_SLIPS table.
        
        Args:
            data_rows (list): List of tuples containing slip data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_SLIPS (
                ID, NAME, TYPE, RECOMMENDED_LOA, RECOMMENDED_BEAM, 
                RECOMMENDED_DRAFT, RECOMMENDED_AIR_DRAFT, MAXIMUM_LOA, 
                MAXIMUM_BEAM, MAXIMUM_DRAFT, MAXIMUM_AIR_DRAFT,
                MARINA_LOCATION_ID, PIER_ID, STATUS, START_DATE, END_DATE,
                DO_NOT_COUNT_IN_OCCUPANCY, ACTIVE, CREATION_DATE_TIME, 
                CREATION_USER, SLIP_TYPE_ID, PAYMENT_PROCESSING_FEE,
                MANAGEMENT_FEE, OWNER_ID, PAYMENT_PROCESSING_FEE_TYPE_ID,
                MANAGEMENT_FEE_TYPE_ID, OVERRIDE_OCCUPANCY_LOA, HASH_ID,
                MAINTENANCE_FEE, SVG_ID, ASSESSMENT, LOAN, ORDER_COLUMN,
                SIGN_NAME, MAX_WEIGHT
            )
            VALUES (
                :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, 
                :15, :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, :26, 
                :27, :28, :29, :30, :31, :32, :33, :34, :35
            )
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully inserted {len(data_rows)} slip records")
            
            # Execute merge procedure immediately after insert
            logger.info("Executing merge for SLIPS...")
            self.merge_single_table('SLIPS')
            
        except Exception as e:
            logger.exception(f"Error inserting slips to staging: {e}")
            self.connection.rollback()

    def insert_reservations(self, data_rows):
        """
        Insert reservations data into STG_MOLO_RESERVATIONS table.
        
        Args:
            data_rows (list): List of tuples containing reservation data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_RESERVATIONS (id, MARINA_LOCATION_ID, CREATION_TIME, RESERVATION_STATUS_ID, 
                       RESERVATION_TYPE_ID, CONTACT_ID, BOAT_ID, SCHEDULED_ARRIVAL_TIME, 
                       SCHEDULED_DEPARTURE_TIME, CANCELLATION_TIME, ACCOUNT_ID, 
                       SLIP_ID, rate, name, HASH_ID, RESERVATION_SOURCE)
            VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} reservation records")
            
            # Execute merge procedure immediately after insert
            logger.info("Executing merge for RESERVATIONS...")
            self.merge_single_table('RESERVATIONS')
            
        except Exception as e:
            logger.exception(f"Error inserting reservations to staging: {e}")
            self.connection.rollback()
    
    def insert_companies(self, data_rows):
        """
        Insert companies data into STG_MOLO_COMPANIES table.
        
        Args:
            data_rows (list): List of tuples containing company data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_COMPANIES (
                ID, NAME, OWNER, PRIMARY_FAX_NUMBER, PRIMARY_PHONE_NUMBER, CITY_ID, 
                       IMAGE, DESCRIPTION, PARTNER_ID, MOLO_API_PARTNER_ID, 
                       COMPANY_MOLO_API_PARTNER_COMPANY_ID, INVOICE_AT_COMPANY_LEVEL, 
                       MOLO_CONTACT_ID, STRIPE_CUSTOMER_ID, LOGIN_PROVIDER_ID, DEFAULT_CC_FEE, 
                       TIER1_PERCENT_ACH_FEE, TIER2_PERCENT_ACH_FEE
            )
            VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} company records")
        except Exception as e:
            logger.exception(f"Error inserting companies to staging: {e}")
            self.connection.rollback()

    def insert_contacts(self, data_rows):
        """
        Insert contacts data into STG_MOLO_CONTACTS table.
        
        Args:
            data_rows (list): List of tuples containing contact data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_CONTACTS (
                ID, EMAILS, FIRST_NAME, MIDDLE_NAME, LAST_NAME, MARINA_LOCATION_ID, NOTES, 
                       RECORD_STATUS_ID, IS_SUPPLIER, IS_CUSTOMER, XERO_ID, COMPANY_CONTACT_NAME, 
                       CREATION_USER, CREATION_DATE_TIME, CIM_ID, MARINA_LOCATION1_ID, QB_CUSTOMER_ID, 
                       STATEMENTS_PREFERENCE_ID, HASH_ID, MOLO_API_PARTNER_ID, TAX_EXEMPT_STATUS, 
                       AUTOMATIC_DISCOUNT_PERCENT, COST_PLUS_DISCOUNT, LINKED_PARENT_CONTACT, CONTACT_AUTO_CHARGE_ID, 
                       LAST_EDITED_DATE_TIME, LAST_EDITED_USER_ID, LAST_EDITED_MOLO_API_PARTNER_ID, STRIPE_CUSTOMER_ID, 
                       ACCOUNT_LIMIT, FILESTACK_ID, SHOW_COMPANY_NAME_PRINTED, BOOKING_MERGING_DONE, 
                       DATE_OF_BIRTH, IDS_CUSTOMER_ID, DO_NOT_LAUNCH, DO_NOT_LAUNCH_REASON, DRIVER_LICENSE_ID, 
                       QUICKBOOKS_ID, QUICKBOOKS_NAME, QBO_VENDOR_ID, SKIP_FOR_FINANCE_CHARGES, MAIN_CONTACT_ID
            )
            VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29, :30, :31, :32, :33, :34, :35, :36, :37, :38, :39, :40, :41, :42, :43)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully inserted {len(data_rows)} contact records to staging")
            
            # Immediately merge to DW
            self.merge_single_table('CONTACTS')
        except Exception as e:
            logger.exception(f"Error inserting contacts to staging: {e}")
            self.connection.rollback()

    def insert_boats(self, data_rows):
        """
        Insert boats data into STG_MOLO_BOATS table.
        
        Args:
            data_rows (list): List of tuples containing boat data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_BOATS (
                ID, PHOTO, MAKE, MODEL, NAME, LOA, BEAM, DRAFT, AIR_DRAFT, 
                       REGISTRATION_NUMBER, REGISTRATION_STATE, CREATION_TIME, BOAT_TYPE_ID, 
                       MARINA_LOCATION_ID, POWER_NEED_ID, NOTES, RECORD_STATUS_ID, ASPNET_USER_ID, 
                       MAST_LENGTH, WEIGHT, COLOR, HULL_ID, KEY_LOCATION_CODE, YEAR, HASH_ID, 
                       MOLO_API_PARTNER_ID, POWER_NEED1_ID, LAST_EDITED_DATE_TIME, LAST_EDITED_USER_ID, 
                       LAST_EDITED_MOLO_API_PARTNER_ID, FILESTACK_ID, TONNAGE, GALLON_CAPACITY, 
                       IS_ACTIVE, BOOKING_MERGING_DONE, DECAL_NUMBER, MANUFACTURER, SERIAL_NUMBER, 
                       REGISTRATION_EXPIRATION
            )
            VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29, :30, :31, :32, :33, :34, :35, :36, :37, :38, :39)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully inserted {len(data_rows)} boat records to staging")
            
            # Immediately merge to DW
            self.merge_single_table('BOATS')
        except Exception as e:
            logger.exception(f"Error inserting boats to staging: {e}")
            self.connection.rollback()

    def insert_accounts(self, data_rows):
        """
        Insert accounts data into STG_MOLO_ACCOUNTS table.
        
        Args:
            data_rows (list): List of tuples containing account data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_ACCOUNTS (ID, ACCOUNT_STATUS_ID, MARINA_LOCATION_ID, CONTACT_ID)
            VALUES (:1, :2, :3, :4)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} account records")
        except Exception as e:
            logger.exception(f"Error inserting accounts to staging: {e}")
            self.connection.rollback()

    def insert_invoices(self, data_rows):
        """
        Insert invoices data into STG_MOLO_INVOICES table.
        
        Args:
            data_rows (list): List of tuples containing invoice data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_INVOICES (
                ID, DATE_FIELD, DOLLAR_DISCOUNT, PERCENT_DISCOUNT, ACTIVE, CLOSING_DATE, 
                       DISCOUNT_TOTAL, OPENED, PAYED, SUBTOTAL, SUBTOTAL_WO_DISCOUNT, TAX_TOTAL, 
                       TITLE, TOTAL, RESERVATION_ID, ACCOUNT_ID, SERVICE_PAID_AMOUNT, 
                       MARINA_PAID_AMOUNT, GAS_PAID_AMOUNT, INVOICE_STATUS_ID, START_DATE, 
                       INSTALLMENTS_PAYMENT_METHOD_ID, SCHEDULED_FOR_CRON, ORIGINAL_INVOICE, 
                       PAYMENTS_SENT_TO_XERO, WORK_ORDER_ID, IS_INSTALLMENT_INVOICE, VOID_USER, 
                       VOID_DATE_TIME, CREATION_USER, PAYMENT_ID, QB_INVOICE_ID, INVOICE_TYPE_ID, 
                       INVOICE_DATE, DUE_DATE, CURRENCY_CODE, LAST_MODIFIED_DATE_TIME, 
                       LAST_MODIFIED_ASPNET_USER, VOID_REASON, CREATE_PARTNER_ID, VOID_PARTNER_ID, 
                       UPDATE_HASH, SCHEDULED_FOR_INVENTORY_CRON, SCHEDULED_FOR_SUBLET_CRON, 
                       SCHEDULED_FOR_LABOR_CRON, CREATED_ON_MOBILE, STRIPE_INVOICE_ID, SENT_TO_STRIPE, 
                       RESOURCE_BOOKING_ID, MODIFIED_ON_MOBILE, NOTE, QUICKBOOKS_INVOICE_ID, 
                       TAX_CAP, IS_SURCHARGE
            )
            VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29, :30, :31, :32, :33, :34, :35, :36, :37, :38, :39, :40, :41, :42, :43, :44, :45, :46, :47, :48, :49, :50, :51, :52, :53, :54)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} invoice records")
            
            # Execute merge procedure immediately after insert
            logger.info("Executing merge for INVOICES...")
            self.merge_single_table('INVOICES')
            
        except Exception as e:
            logger.exception(f"Error inserting invoices to staging: {e}")
            self.connection.rollback()

    def insert_invoice_items(self, data_rows):
        """
        Merge invoice items data into DW_MOLO_INVOICE_ITEMS table.
        
        Args:
            data_rows (list): List of tuples containing invoice item data
        """
        import sys
        
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_INVOICE_ITEMS (
                ID, PREFIX, QUANTITY, TITLE, TYPE_FIELD, VALUE_FIELD, DISCOUNT,
                DISCOUNT_TYPE, TAXABLE, TAX, MISC, DISCOUNT_TOTAL, PRICE_SUFFIX,
                SUB_TOTAL, SUBTOTAL_WO_DISCOUNT, TAX_TOTAL, TOTAL, INVOICE_ID,
                CHARGE_GROUP, PAYMENT_ACCOUNT, PRICE_STR, IS_VOID, DATE_FIELD,
                TEXT_AUX, TEXT_AUX2, DISCOUNT_DATE_TIME, DISCOUNT_USER_ID,
                NOTES, PR_TYPE, STATUS_FIELD, DELETION_USER_ID,
                DELETION_DATE_TIME, OVERPAYMENT_ID, VOID_USER, VOID_DATE_TIME,
                MISC2, PREPAYMENT_ID, CREDIT_INVOICE_ID, ALLOCATION_TYPE,
                RESERVATION_ID, ITEM_MASTER_ID, ASPNET_USER_ID,
                INVOICE_ITEM_TYPE_ID, SEASONAL_PRICE_ID, TRANSIENT_PRICE_ID,
                SV_JOB_ID, ORIGINAL_CREDIT_ITEM, TAX_EXEMPT,
                LAST_MODIFIED_DATE_TIME, LAST_MODIFIED_ASPNET_USER,
                DELETION_REASON, VOID_REASON, START_DATE_TIME, END_DATE_TIME,
                CREATION_PARTNER_ID, DELETE_PARTNER_ID, VOID_PARTNER_ID,
                ORIGINAL_PRICE, OVERRIDE_XERO_TAX_RATE,
                OVERRIDE_XERO_SALES_ACCOUNT, ORIGINAL_RESERVATION_PRICE,
                NUMBER_OF_DECIMALS, STRIPE_TRANSACTION_DATA_ID,
                VALUE_ALTERNATIVE, STRIPE_TERMINAL_ID, STRIPE_APPLICATION_NAME,
                STRIPE_AID, ENTERED_AMOUNT, EXCHANGE_RATE, CURRENCIES_ID,
                SV_CHARGE_INSTANCE_ID, SV_LABOR_INSTANCE_ID, SV_PART_INSTANCE_ID,
                REVENUE_GL_CODE, AR_GL_CODE, PAYMENT_GL_CODE, COGS_GL_CODE,
                INVENTORY_GL_CODE, SALES_TAX_GL_CODE, PREPAYMENT_GL_CODE,
                ACCOUNT_TYPE, APPLICATION_CRYPTOGRAM, AUTHORIZATION_CODE,
                AUTHORIZATION_RESPONSE_CODE, CARDHOLDER_VERIFICATION_METHOD,
                TERMINAL_VERIFICATION_RESULTS, TRANSACTION_STATUS_INFORMATION,
                TRACKING_CODE, DISCOUNT_GL_CODE, OVERRIDE_TRACKING_CATEGORY1,
                OVERRIDE_TRACKING_CATEGORY2, QUICKBOOKS_PAYMENT_ID,
                ALLOW_TOTAL_PRICE_ENTRY, ADDED_AUTOMATICALLY,
                ALLOCATION_PERFORMED_DATE, CREATED_DATE
            )
            VALUES (
                :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14,
                :15, :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, :26,
                :27, :28, :29, :30, :31, :32, :33, :34, :35, :36, :37, :38,
                :39, :40, :41, :42, :43, :44, :45, :46, :47, :48, :49, :50,
                :51, :52, :53, :54, :55, :56, :57, :58, :59, :60, :61, :62,
                :63, :64, :65, :66, :67, :68, :69, :70, :71, :72, :73, :74,
                :75, :76, :77, :78, :79, :80, :81, :82, :83, :84, :85, :86,
                :87, :88, :89, :90, :91, :92, :93, :94, :95, :96
            )
        """
        
        try:
            # Process in batches of 5000 to avoid memory issues and show progress
            batch_size = 5000
            total_rows = len(data_rows)
            
            logger.info(f"  Inserting {total_rows:,} invoice items in batches of {batch_size:,}...")
            sys.stdout.flush()
            
            # Set input sizes for all 96 columns to avoid type detection issues
            # Positions 23, 26, 32, 35, 49, 53, 54, 95, 96 are TIMESTAMP columns
            input_sizes = [None] * 96  # Start with all None (auto-detect for non-datetime)
            
            # Mark datetime columns as TIMESTAMP type to prevent misdetection as VARCHAR
            for pos in [22, 25, 31, 34, 48, 52, 53, 94, 95]:  # 0-indexed positions
                input_sizes[pos] = oracledb.DB_TYPE_TIMESTAMP
            
            self.cursor.setinputsizes(input_sizes)
            
            for i in range(0, total_rows, batch_size):
                batch = data_rows[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (total_rows + batch_size - 1) // batch_size
                
                logger.info(f"  Inserting batch {batch_num}/{total_batches} ({len(batch):,} records)...")
                sys.stdout.flush()
                
                try:
                    self.cursor.executemany(insert_sql, batch)
                    self.connection.commit()
                except Exception as e:
                    # Analyze the failing batch to find datetime values
                    if batch:
                        logger.error(f"Batch {batch_num} failed. Checking for datetime values...")
                        first_row = batch[0]
                        for idx, val in enumerate(first_row, 1):
                            if isinstance(val, datetime):
                                logger.error(f"  ❌ Position {idx}: datetime = {val}")
                    raise
                
                logger.info(f"  ✅ Batch {batch_num}/{total_batches} committed")
                sys.stdout.flush()
            
            logger.info(f"✅ Successfully inserted {total_rows:,} invoice item records")
            sys.stdout.flush()
            
            # Execute merge procedure immediately after insert
            logger.info("Executing merge for INVOICE_ITEMS...")
            sys.stdout.flush()
            self.merge_single_table('INVOICE_ITEMS')
            
        except Exception as e:
            logger.exception(f"Error merging invoice items: {e}")
            self.connection.rollback()

    def insert_transactions(self, data_rows):
        """
        Insert transactions data into STG_MOLO_TRANSACTIONS table.
        
        Args:
            data_rows (list): List of tuples containing transaction data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_TRANSACTIONS (ID, MARINA_LOCATION_ID, CREATION_TIME, INVOICE_ID, TRANSACTION_TYPE_ID, 
                       TRANSACTION_METHOD_ID, VALUE_FIELD, IS_REFUNDED, CUSTOMER_IP_ADDRESS, 
                       CUSTOMER_DEVICE, REFUND_REASON, AUX, CHECK_NUMBER, CC_TYPE, INVOICE_ITEM_ID, 
                       SENT_TO_XERO, OVERPAYMENT_ID, PAYMENT_COLLECTED_OFFLINE, PART_OF_OVERPAYMENT, 
                       PREPAYMENT_ID, ACCOUNT_TRANSACTION_TRANSACTION_ID, PAYMENT_ID, CREATION_DATE, 
                       ASPNET_USER_ID, HASH_ID, CUSTOM_TRANSACTION_METHODS_ID, REFERENCE, IS_VOID, 
                       AMOUNT_REFUNDED, STRIPE_TRANSACTION_DATA_ID, PAYMENT_INTENT_ID, SENT_TO_PAYOUT, 
                       STRIPE_AUTHORIZATIONS_ID, STRIPE_RESPONSE_ID, STRIPE_READER_SERIAL_NUMBER, 
                       STRIPE_TERMINAL_ID, CREATED_ON_MOBILE, ONLINE_PERCENT_FEE, ONLINE_FEE_AMOUNT, 
                       SCHEDULED_FOR_ONLINE_FEE_CRON, ONLINE_PAYMENT_FEE_ID, BANK_NAME, LAST4, 
                       STRIPE_BANK_ACCOUNT_ID, STRIPE_BATCH_ID, ROUTING_NUMBER, FULLY_REFUNDED, 
                       LAST_UPDATED, PAYMENT_SOURCE)
            VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29, :30, :31, :32, :33, :34, :35, :36, :37, :38, :39, :40, :41, :42, :43, :44, :45, :46, :47, :48, :49)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} transaction records")
            
            # Execute merge procedure immediately after insert
            logger.info("Executing merge for TRANSACTIONS...")
            self.merge_single_table('TRANSACTIONS')
            
        except Exception as e:
            logger.exception(f"Error inserting transactions to staging: {e}")
            self.connection.rollback()

    def insert_item_masters(self, data_rows):
        """
        Merge item masters data into DW_MOLO_ITEM_MASTERS table.
        
        Args:
            data_rows (list): List of tuples containing item master data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_ITEM_MASTERS (
                ID, NAME, AMOUNT, ITEM_CHARGE_METHOD_ID, TAXABLE, AVAILABLE_AS_ADD_ON,
                MARINA_LOCATION_ID, PRICE, TAX, SINGLE, CHARGE_CATEGORY, AMOUNT_IS_DECIMAL,
                NUMBER_OF_DECIMALS, ITEM_SHORT_NAME, ITEM_CODE, TRACKED_INVENTORY,
                QUANTITY_ON_HAND, PURCHASE_PRICE, FIRST_TRACKING_CATEGORY, SECOND_TRACKING_CATEGORY,
                XERO_ID, SALE_FREQUENCY, LOW_QUANTITY_WARNING, MARINA_LOCATION1_ID,
                MARINA_LOCATION2_ID, PEDESTAL_ID, PEDESTAL1_ID, QB_ITEM_ID,
                XERO_ITEM_ID, BARCODE, DISTRIBUTE_TO_OWNERS, FUEL_CLOUD_PRODUCT_ID,
                HASH_ID, REQUIRES_AGE_VERIFICATION, MINIMUM_AGE, CREATION_DATE_TIME,
                CREATION_ASPNET_USER_ID, RECORD_STATUS_ID, UPDATE_HASH, SUBLET_ITEM,
                INTERNAL_REVENUE_XERO_ACCOUNT_ID, INTERNAL_COGS_XERO_ACCOUNT_ID, WIP_XERO_ACCOUNT_ID,
                INVENTORY_REVALUATION_ID, MARINA_LOCATION6_ID, FINALE_PRODUCT_URL,
                REVENUE_GL_CODE, COGS_GL_CODE, INVENTORY_GL_CODE, AR_GL_CODE,
                SALES_TAX_GL_CODE, ONLY_USE_LAST2_AVERAGE, DEFERRED_REVENUE_RECOGNITION,
                DEFERRED_RECOGNITION_GL_CODE, TRACKING_CODE, ADD_DESCRIPTION_TO_INVOICE_NOTE,
                MARINA_LOCATION7_ID, IGNORE_INVENTORY_QOH, QOH_COMMITTED, QOH_ON_ORDER,
                ALLOW_TOTAL_PRICE_ENTRY, MARINA_LOCATION9_ID, ALLOW_POSTING_TO_NON_INCOME_ACCOUNTS,
                ORDER_COLUMN, ENABLE_NEGATIVE_INVENTORY, WIP
            )
            VALUES (
                :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16,
                :17, :18, :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29, :30,
                :31, :32, :33, :34, :35, :36, :37, :38, :39, :40, :41, :42, :43, :44,
                :45, :46, :47, :48, :49, :50, :51, :52, :53, :54, :55, :56, :57, :58,
                :59, :60, :61, :62, :63, :64, :65, :66
            )
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} item master records")
        except Exception as e:
            logger.exception(f"Error merging item masters: {e}")
            self.connection.rollback()

    def insert_seasonal_prices(self, data_rows):
        """
        Merge seasonal prices data into DW_MOLO_SEASONAL_PRICES table.
        
        Args:
            data_rows (list): List of tuples containing seasonal price data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_SEASONAL_PRICES (id, SEASON_NAME, START_DATE, END_DATE, SEASONAL_CHARGE_METHOD_ID, 
                       PRICE_PER_FOOT, FLAT_RATE, TAXABLE, MARINA_LOCATION_ID, ACTIVE, 
                       TAX, RATE_DETAILS, RATE_SHORT_NAME, ONLINE_PAYMENT_PLACEHOLDER, 
                       XERO_ITEM_CODE, XERO_ID, FIRST_TRACKING_CATEGORY, SECOND_TRACKING_CATEGORY, 
                       SEASONAL_INVOICING_METHOD_ID, CREATION_DATE_TIME, ASPNET_USER_ID, 
                       CHECK_IN_TERMS, CHECK_OUT_TERMS, ONLINE_PAYMENT_COMPLETION, DUE_DATE_DAYS, 
                       DUE_DATE_SETTINGS_ID, CHARGE_CATEGORY, INTRO_TEXT, REVENUE_GL_CODE, 
                       AR_GL_CODE, SALES_TAX_GL_CODE)
            VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29, :30, :31)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} seasonal price records")
        except Exception as e:
            logger.exception(f"Error merging seasonal prices: {e}")
            self.connection.rollback()

    def insert_transient_prices(self, data_rows):
        """
        Merge transient prices data into DW_MOLO_TRANSIENT_PRICES table.
        
        Args:
            data_rows (list): List of tuples containing transient price data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_TRANSIENT_PRICES (
                ID, START_DATE, END_DATE, FEE, RATE_NAME, TRANSIENT_CHARGE_METHOD_ID,
                MARINA_LOCATION_ID, TAXABLE, TAX, RATE_DETAILS, RATE_SHORT_NAME,
                ONLINE_PAYMENT_PLACEHOLDER, XERO_ITEM_CODE, XERO_ID, FIRST_TRACKING_CATEGORY,
                SECOND_TRACKING_CATEGORY, TRANSIENT_INVOICING_METHOD_ID, SV_INVENTORY_CATEGORY_ID,
                SV_INVENTORY_SUB_CATEGORY_ID, CREATION_DATE_TIME, ASPNET_USER_ID,
                CHECK_IN_TERMS, CHECK_OUT_TERMS, ONLINE_PAYMENT_COMPLETION, DUE_DATE_DAYS,
                DUE_DATE_SETTINGS_ID, HOURLY_CALCULATION, ROUND_MINUTES, MINIMUM_HOURS,
                NUM_HOURS_BLOCK, CHARGE_CATEGORY, INTRO_TEXT, REVENUE_GL_CODE,
                AR_GL_CODE, SALES_TAX_GL_CODE, DELETION_DATETIME, DELETION_ASPNET_USER_ID,
                RECORD_STATUS_ID, RECURRING_INVOICE_OPTIONS_ID, RECURRING, TRACKING_CODE,
                ALTERNATE_RESERVATION_NAME, RESOURCE_RATE, QUANTITY_CAP,
                ALLOW_POSTING_TO_NON_INCOME_ACCOUNTS
            )
            VALUES (
                :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15,
                :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, :26, :27, :28,
                :29, :30, :31, :32, :33, :34, :35, :36, :37, :38, :39, :40, :41,
                :42, :43, :44, :45
            )
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} transient price records")
        except Exception as e:
            logger.exception(f"Error merging transient prices: {e}")
            self.connection.rollback()

    def insert_record_status(self, data_rows):
        """
        Merge record status data into DW_MOLO_RECORD_STATUS table.
        
        Args:
            data_rows (list): List of tuples containing record status data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_RECORD_STATUS (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} record status records")
        except Exception as e:
            logger.exception(f"Error merging record status: {e}")
            self.connection.rollback()

    def insert_boat_types(self, data_rows):
        """
        Merge boat types data into DW_MOLO_BOAT_TYPES table.
        
        Args:
            data_rows (list): List of tuples containing boat type data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_BOAT_TYPES (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} boat type records")
        except Exception as e:
            logger.exception(f"Error merging boat types: {e}")
            self.connection.rollback()

    def insert_power_needs(self, data_rows):
        """
        Merge power needs data into DW_MOLO_POWER_NEEDS table.
        
        Args:
            data_rows (list): List of tuples containing power need data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_POWER_NEEDS (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} power need records")
        except Exception as e:
            logger.exception(f"Error merging power needs: {e}")
            self.connection.rollback()

    def insert_reservation_status(self, data_rows):
        """
        Merge reservation status data into DW_MOLO_RESERVATION_STATUS table.
        
        Args:
            data_rows (list): List of tuples containing reservation status data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_RESERVATION_STATUS (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} reservation status records")
        except Exception as e:
            logger.exception(f"Error merging reservation status: {e}")
            self.connection.rollback()

    def insert_reservation_types(self, data_rows):
        """
        Merge reservation types data into DW_MOLO_RESERVATION_TYPES table.
        
        Args:
            data_rows (list): List of tuples containing reservation type data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_RESERVATION_TYPES (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} reservation type records")
        except Exception as e:
            logger.exception(f"Error merging reservation types: {e}")
            self.connection.rollback()

    def insert_contact_types(self, data_rows):
        """
        Merge contact types data into DW_MOLO_CONTACT_TYPES table.
        
        Args:
            data_rows (list): List of tuples containing contact type data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_CONTACT_TYPES (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} contact type records")
        except Exception as e:
            logger.exception(f"Error merging contact types: {e}")
            self.connection.rollback()

    def insert_invoice_status(self, data_rows):
        """
        Merge invoice status data into DW_MOLO_INVOICE_STATUS table.
        
        Args:
            data_rows (list): List of tuples containing invoice status data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_INVOICE_STATUS (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} invoice status records")
        except Exception as e:
            logger.exception(f"Error merging invoice status: {e}")
            self.connection.rollback()

    def insert_invoice_types(self, data_rows):
        """
        Merge invoice types data into DW_MOLO_INVOICE_TYPES table.
        
        Args:
            data_rows (list): List of tuples containing invoice type data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_INVOICE_TYPES (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} invoice type records")
        except Exception as e:
            logger.exception(f"Error merging invoice types: {e}")
            self.connection.rollback()

    def insert_transaction_types(self, data_rows):
        """
        Merge transaction types data into DW_MOLO_TRANSACTION_TYPES table.
        
        Args:
            data_rows (list): List of tuples containing transaction type data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_TRANSACTION_TYPES (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} transaction type records")
        except Exception as e:
            logger.exception(f"Error merging transaction types: {e}")
            self.connection.rollback()

    def insert_transaction_methods(self, data_rows):
        """
        Merge transaction methods data into DW_MOLO_TRANSACTION_METHODS table.
        
        Args:
            data_rows (list): List of tuples containing transaction method data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_TRANSACTION_METHODS (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} transaction method records")
        except Exception as e:
            logger.exception(f"Error merging transaction methods: {e}")
            self.connection.rollback()

    def insert_insurance(self, data_rows):
        """
        Insert insurance data into STG_MOLO_INSURANCE table.
        
        Args:
            data_rows (list): List of tuples containing insurance data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_INSURANCE (
                ID, PROVIDER, LISTED_INDIVIDUAL, ACCOUNT_NUMBER, POLICY_NUMBER, 
                GROUP_NUMBER, LIABILITY_MAXIMUM, EFFECTIVE_DATE, EXPIRATION_DATE, 
                NOTES, CREATION_USER, CREATION_DATE_TIME, LAST_EDIT_USER, 
                LAST_EDIT_DATE_TIME, DELETE_USER, DELETE_DATE_TIME, 
                INSURANCE_STATUS_ID, BOAT_ID, HASH_ID
            )
            VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully inserted {len(data_rows)} insurance records")
        except Exception as e:
            logger.exception(f"Error inserting insurance to staging: {e}")
            self.connection.rollback()

    def insert_equipment(self, data_rows):
        """
        Insert equipment data into STG_MOLO_EQUIPMENT table.
        
        Args:
            data_rows (list): List of tuples containing equipment data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_EQUIPMENT (id, name, description, equipment_type_id, fuel_type_id, model, 
                       manufacturer, year_built, serial_number, location)
            VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} equipment records")
        except Exception as e:
            logger.exception(f"Error inserting equipment to staging: {e}")
            self.connection.rollback()

    def insert_account_status(self, data_rows):
        """
        Merge account status data into DW_MOLO_ACCOUNT_STATUS table.
        
        Args:
            data_rows (list): List of tuples containing account status data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_ACCOUNT_STATUS (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} account status records")
        except Exception as e:
            logger.exception(f"Error merging account status: {e}")
            self.connection.rollback()

    def insert_contact_auto_charge(self, data_rows):
        """
        Merge contact auto charge data into DW_MOLO_CONTACT_AUTO_CHARGE table.
        
        Args:
            data_rows (list): List of tuples containing contact auto charge data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_CONTACT_AUTO_CHARGE (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} contact auto charge records")
        except Exception as e:
            logger.exception(f"Error merging contact auto charge: {e}")
            self.connection.rollback()

    def insert_statements_preference(self, data_rows):
        """
        Merge statements preference data into DW_MOLO_STATEMENTS_PREFERENCE table.
        
        Args:
            data_rows (list): List of tuples containing statements preference data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_STATEMENTS_PREFERENCE (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} statements preference records")
        except Exception as e:
            logger.exception(f"Error merging statements preference: {e}")
            self.connection.rollback()

    def insert_invoice_item_types(self, data_rows):
        """
        Merge invoice item types data into DW_MOLO_INVOICE_ITEM_TYPES table.
        
        Args:
            data_rows (list): List of tuples containing invoice item types data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_INVOICE_ITEM_TYPES (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} invoice item type records")
        except Exception as e:
            logger.exception(f"Error merging invoice item types: {e}")
            self.connection.rollback()

    def insert_payment_methods(self, data_rows):
        """
        Merge payment methods data into DW_MOLO_PAYMENT_METHODS table.
        
        Args:
            data_rows (list): List of tuples containing payment methods data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_PAYMENT_METHODS (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} payment method records")
        except Exception as e:
            logger.exception(f"Error merging payment methods: {e}")
            self.connection.rollback()

    def insert_seasonal_charge_methods(self, data_rows):
        """
        Merge seasonal charge methods data into DW_MOLO_SEASONAL_CHARGE_METHODS table.
        
        Args:
            data_rows (list): List of tuples containing seasonal charge methods data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_SEASONAL_CHARGE_METHODS (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} seasonal charge method records")
        except Exception as e:
            logger.exception(f"Error merging seasonal charge methods: {e}")
            self.connection.rollback()

    def insert_seasonal_invoicing_methods(self, data_rows):
        """
        Merge seasonal invoicing methods data into DW_MOLO_SEASONAL_INVOICING_METHODS table.
        
        Args:
            data_rows (list): List of tuples containing seasonal invoicing methods data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_SEASONAL_INVOICING_METHODS (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} seasonal invoicing method records")
        except Exception as e:
            logger.exception(f"Error merging seasonal invoicing methods: {e}")
            self.connection.rollback()

    def insert_transient_charge_methods(self, data_rows):
        """
        Merge transient charge methods data into DW_MOLO_TRANSIENT_CHARGE_METHODS table.
        
        Args:
            data_rows (list): List of tuples containing transient charge methods data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_TRANSIENT_CHARGE_METHODS (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} transient charge method records")
        except Exception as e:
            logger.exception(f"Error merging transient charge methods: {e}")
            self.connection.rollback()

    def insert_transient_invoicing_methods(self, data_rows):
        """
        Merge transient invoicing methods data into DW_MOLO_TRANSIENT_INVOICING_METHODS table.
        
        Args:
            data_rows (list): List of tuples containing transient invoicing methods data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_TRANSIENT_INVOICING_METHODS (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} transient invoicing method records")
        except Exception as e:
            logger.exception(f"Error merging transient invoicing methods: {e}")
            self.connection.rollback()

    def insert_recurring_invoice_options(self, data_rows):
        """
        Merge recurring invoice options data into DW_MOLO_RECURRING_INVOICE_OPTIONS table.
        
        Args:
            data_rows (list): List of tuples containing recurring invoice options data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_RECURRING_INVOICE_OPTIONS (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} recurring invoice option records")
        except Exception as e:
            logger.exception(f"Error merging recurring invoice options: {e}")
            self.connection.rollback()

    def insert_due_date_settings(self, data_rows):
        """
        Merge due date settings data into DW_MOLO_DUE_DATE_SETTINGS table.
        
        Args:
            data_rows (list): List of tuples containing due date settings data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_DUE_DATE_SETTINGS (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} due date setting records")
        except Exception as e:
            logger.exception(f"Error merging due date settings: {e}")
            self.connection.rollback()

    def insert_item_charge_methods(self, data_rows):
        """
        Merge item charge methods data into DW_MOLO_ITEM_CHARGE_METHODS table.
        
        Args:
            data_rows (list): List of tuples containing item charge methods data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_ITEM_CHARGE_METHODS (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} item charge method records")
        except Exception as e:
            logger.exception(f"Error merging item charge methods: {e}")
            self.connection.rollback()

    def insert_insurance_status(self, data_rows):
        """
        Merge insurance status data into DW_MOLO_INSURANCE_STATUS table.
        
        Args:
            data_rows (list): List of tuples containing insurance status data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_INSURANCE_STATUS (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} insurance status records")
        except Exception as e:
            logger.exception(f"Error merging insurance status: {e}")
            self.connection.rollback()

    def insert_equipment_types(self, data_rows):
        """
        Merge equipment types data into DW_MOLO_EQUIPMENT_TYPES table.
        
        Args:
            data_rows (list): List of tuples containing equipment types data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_EQUIPMENT_TYPES (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} equipment type records")
        except Exception as e:
            logger.exception(f"Error merging equipment types: {e}")
            self.connection.rollback()

    def insert_equipment_fuel_types(self, data_rows):
        """
        Merge equipment fuel types data into DW_MOLO_EQUIPMENT_FUEL_TYPES table.
        
        Args:
            data_rows (list): List of tuples containing equipment fuel types data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_EQUIPMENT_FUEL_TYPES (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} equipment fuel type records")
        except Exception as e:
            logger.exception(f"Error merging equipment fuel types: {e}")
            self.connection.rollback()

    def insert_vessel_engine_class(self, data_rows):
        """
        Merge vessel engine class data into DW_MOLO_VESSEL_ENGINE_CLASS table.
        
        Args:
            data_rows (list): List of tuples containing vessel engine class data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_VESSEL_ENGINE_CLASS (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} vessel engine class records")
        except Exception as e:
            logger.exception(f"Error merging vessel engine class: {e}")
            self.connection.rollback()

    def insert_cities(self, data_rows):
        """
        Insert cities data into STG_MOLO_CITIES table.
        
        Args:
            data_rows (list): List of tuples containing cities data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_CITIES (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} city records")
        except Exception as e:
            logger.exception(f"Error inserting cities to staging: {e}")
            self.connection.rollback()

    def insert_countries(self, data_rows):
        """
        Insert countries data into STG_MOLO_COUNTRIES table.
        
        Args:
            data_rows (list): List of tuples containing countries data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_COUNTRIES (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} country records")
        except Exception as e:
            logger.exception(f"Error inserting countries to staging: {e}")
            self.connection.rollback()

    def insert_currencies(self, data_rows):
        """
        Insert currencies data into STG_MOLO_CURRENCIES table.
        
        Args:
            data_rows (list): List of tuples containing currencies data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_CURRENCIES (id, name, code, symbol)
            VALUES (:1, :2, :3, :4)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} currency records")
        except Exception as e:
            logger.exception(f"Error inserting currencies to staging: {e}")
            self.connection.rollback()

    def insert_phone_types(self, data_rows):
        """
        Merge phone types data into DW_MOLO_PHONE_TYPES table.
        
        Args:
            data_rows (list): List of tuples containing phone types data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_PHONE_TYPES (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} phone type records")
        except Exception as e:
            logger.exception(f"Error merging phone types: {e}")
            self.connection.rollback()

    def insert_address_types(self, data_rows):
        """
        Merge address types data into DW_MOLO_ADDRESS_TYPES table.
        
        Args:
            data_rows (list): List of tuples containing address types data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_ADDRESS_TYPES (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} address type records")
        except Exception as e:
            logger.exception(f"Error merging address types: {e}")
            self.connection.rollback()

    def insert_installments_payment_methods(self, data_rows):
        """
        Merge installments payment methods data into DW_MOLO_INSTALLMENTS_PAYMENT_METHODS table.
        
        Args:
            data_rows (list): List of tuples containing installments payment methods data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_INSTALLMENTS_PAYMENT_METHODS (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} installments payment method records")
        except Exception as e:
            logger.exception(f"Error merging installments payment methods: {e}")
            self.connection.rollback()

    def insert_payments_provider(self, data_rows):
        """
        Merge payments provider data into DW_MOLO_PAYMENTS_PROVIDER table.
        
        Args:
            data_rows (list): List of tuples containing payments provider data
        """
        if not data_rows:
            return
        
        insert_sql = """
            INSERT INTO STG_MOLO_PAYMENTS_PROVIDER (id, name)
            VALUES (:1, :2)
        """
        
        try:
            self.cursor.executemany(insert_sql, data_rows)
            self.connection.commit()
            logger.info(f"✅ Successfully merged {len(data_rows)} payments provider records")
        except Exception as e:
            logger.exception(f"Error merging payments provider: {e}")
            self.connection.rollback()

    def close(self):
        """Close database connection and cursor."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("✅ Database connection closed")

