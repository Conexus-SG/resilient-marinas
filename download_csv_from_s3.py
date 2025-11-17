#!/usr/bin/env python3
"""
Marina Data Processing Pipeline - MOLO & Stellar Business

This script downloads and processes marina management data from two sources:

1. MOLO System: Downloads the latest ZIP file from S3 bucket, extracts CSV files
   (MarinaLocations, Piers, SlipTypes, Slips, Reservations, etc.), and synchronizes 
   the data with Oracle database tables using INSERT operations into staging tables.

2. Stellar Business: Downloads the latest gzipped DATA file from S3 bucket 
   (resilient-ims-backups), extracts CSV files (locations, customers, bookings, etc.),
   and synchronizes with Oracle database using INSERT operations into staging tables.

Configuration:
- Credentials are loaded from config.json file (create from config.json.template)
- config.json contains AWS credentials, database credentials, and S3 bucket names
- Keep config.json secure and never commit it to version control

Both data sources can be processed together or independently using command-line flags.

Author: Stefan Holodnick
Date: October 2025
"""

# Standard library imports
import argparse
import base64
import csv
import io
import json
import logging
import os
import zipfile
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

# Third-party imports
import boto3
import oracledb
from botocore.exceptions import NoCredentialsError, ClientError

# Local imports
from molo_db_functions import OracleConnector

# Optional validation imports
try:
    from data_validator import DataValidator
    VALIDATION_AVAILABLE = True
except ImportError:
    VALIDATION_AVAILABLE = False
    logger.warning("DataValidator not available - validation disabled")

# =============================================================================
# GLOBAL CONFIGURATION
# =============================================================================

# Global logger instance - configured by setup_logging()
logger = logging.getLogger(__name__)

# Import Stellar processing function
try:
    from download_stellar_from_s3 import process_stellar_data_from_s3
    STELLAR_AVAILABLE = True
    logger.info("Stellar processing module loaded successfully")
except ImportError as e:
    STELLAR_AVAILABLE = False
    logger.warning(f"Stellar processing module not available: {e}")

# Target CSV files to extract from the ZIP archive
TARGET_CSV_FILES = [
    'MarinaLocations', 'Slips', 'SlipTypes', 'Reservations', 'Piers',
    'Companies', 'Contacts', 'Boats', 'Accounts', 'InvoiceSet', 
    'InvoiceItemSet', 'Transactions', 'ItemMasters', 'SeasonalPrices',
    'TransientPrices', 'RecordStatusSet', 'BoatTypes', 'PowerNeeds',
    'ReservationStatus', 'ReservationTypes', 'ContactTypes',
    'InvoiceStatusSet', 'InvoiceTypeSet', 'TransactionTypeSet', 
    'TransactionMethodSet', 'InsuranceSet', 'EquipmentSet', 'AccountStatus',
    'ContactAutoChargeSet', 'StatementsPreferenceSet', 'InvoiceItemTypeSet',
    'PaymentMethods', 'SeasonalChargeMethods', 'SeasonalInvoicingMethodSet',
    'TransientChargeMethods', 'TransientInvoicingMethodSet', 'RecurringInvoiceOptionsSet',
    'DueDateSettingsSet', 'ItemChargeMethods', 'InsuranceStatusSet',
    'EquipmentTypeSet', 'EquipmentFuelTypeSet', 'VesselEngineClassSet',
    'Cities', 'Countries', 'CurrenciesSet', 'PhoneTypes', 'AddressTypeSet',
    'InstalmentsPaymentMethodSet', 'PaymentsProviderSet'
]

# =============================================================================
# CONFIGURATION FILE LOADING
# =============================================================================

def load_config_file(config_path='config.json'):
    """
    Load credentials and configuration from a JSON config file.
    
    Args:
        config_path (str): Path to the config.json file
        
    Returns:
        dict: Configuration dictionary containing credentials and settings
        
    Example config.json structure:
    {
        "aws": {
            "access_key_id": "AKIA...",
            "secret_access_key": "secret123...",
            "region": "us-east-1"
        },
        "database": {
            "user": "OAX_USER",
            "password": "dbpass123",
            "dsn": "oax5007253621_low"
        },
        "s3": {
            "molo_bucket": "cnxtestbucket",
            "stellar_bucket": "resilient-ims-backups"
        },
        "logging": {
            "level": "INFO"
        }
    }
    """
    try:
        # Check if config file exists
        if not os.path.exists(config_path):
            logger.error(f"Configuration file not found: {config_path}")
            logger.error("Please create a config.json file from config.json.template")
            return None
            
        # Load and parse JSON config file
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Validate required sections exist
        required_sections = ['aws', 'database']
        missing_sections = [s for s in required_sections if s not in config]
        if missing_sections:
            logger.error(f"Missing required sections in config file: {missing_sections}")
            return None
            
        # Validate AWS credentials
        if not config['aws'].get('access_key_id') or not config['aws'].get('secret_access_key'):
            logger.error("AWS credentials are incomplete in config file")
            return None
            
        # Validate database credentials
        if not config['database'].get('password'):
            logger.error("Database password is missing in config file")
            return None
            
        logger.info(f"✅ Successfully loaded configuration from {config_path}")
        return config
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in config file: {e}")
        return None
    except Exception as e:
        logger.error(f"Error loading config file: {e}")
        return None


# =============================================================================
# ENVIRONMENT AND OCI SETUP (VAULT FUNCTIONALITY DISABLED)
# =============================================================================

# =============================================================================
# CONFIGURATION & CREDENTIALS
# =============================================================================
# All configuration and credentials are loaded from config.json file
# This simplifies deployment and keeps credentials in one secure location
# No environment variables or .env files are used

oci = None  # OCI SDK not used


# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

class OCILoggingHandler(logging.Handler):
    """
    Custom logging handler that sends log messages to Oracle Cloud Infrastructure 
    Logging service using Resource Principals authentication (for Container Instances).
    
    Attributes:
        log_ocid (str): The OCID of the OCI log object
        logging_client: OCI logging client instance
        source (str): Source identifier for log entries
    """
    
    def __init__(self, log_ocid):
        """
        Initialize the OCI logging handler with Resource Principals authentication.
        
        Args:
            log_ocid (str): The OCID of the OCI log object to send logs to
        """
        super().__init__()
        self.log_ocid = log_ocid
        self.logging_client = None
        self.source = os.path.basename(__file__)
        self._error_count = 0
        
        try:
            # Use Resource Principals authentication (correct for Container Instances)
            signer = oci.auth.signers.get_resource_principals_signer()
            self.logging_client = oci.logging.LoggingClient(config={}, signer=signer)
            print("✅ OCI logging handler initialized with Resource Principals")
        except Exception as e:
            self.logging_client = None
            print(f"⚠️  Warning: Could not initialize OCI logging handler: {e}")
            print("   Continuing with console logging only...")

    def emit(self, record):
        """
        Send a log record to OCI Logging service.
        
        Args:
            record: Python logging record to be sent to OCI
        """
        if not self.logging_client:
            return

        try:
            # Format the log message as structured JSON
            log_entry_data = {
                "message": self.format(record),
                "level": record.levelname,
                "timestamp": datetime.utcfromtimestamp(
                    record.created
                ).isoformat() + 'Z',
                "source": self.source
            }
            
            entry = oci.logging.models.LogEntry(
                data=json.dumps(log_entry_data),
                id=str(hash(f"{datetime.now().isoformat()}{record.message}")),
                time=datetime.utcfromtimestamp(record.created)
            )

            put_logs_details = oci.logging.models.PutLogsDetails(
                log_entries=[entry]
            )

            self.logging_client.put_logs(
                log_id=self.log_ocid,
                put_logs_details=put_logs_details
            )
            
        except Exception as e:
            self._error_count += 1
            if self._error_count <= 3:
                logger.warning(
                    f"⚠️  Warning: Failed to send log to OCI "
                    f"(error #{self._error_count}): {e}"
                )
                if self._error_count == 3:
                    logger.warning("   (Further OCI logging errors will be suppressed)")


def setup_logging():
    """
    Configure the logging system based on environment settings.
    
    In development: Logs to console only
    In production: Logs to both console and OCI Logging service (if configured)
    
    Environment Variables:
        LOG_LEVEL: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        ENVIRONMENT: 'production' or 'development'
        OCI_LOG_OCID: OCID of the OCI log group (production only)
    """
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger.setLevel(log_level)
    
    # Create a standard formatter for all handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Always add console logging
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # In production, attempt to add OCI logging
    if os.getenv('ENVIRONMENT', 'development').lower() == 'production' and oci:
        log_ocid = os.getenv('OCI_LOG_OCID')
        if log_ocid:
            try:
                oci_handler = OCILoggingHandler(log_ocid=log_ocid)
                # Only add if initialization succeeded
                if oci_handler.logging_client:
                    oci_handler.setFormatter(formatter)
                    logger.addHandler(oci_handler)
                    logger.info(f"OCI logging configured for log OCID: {log_ocid}")
                else:
                    logger.warning(
                        "OCI logging handler failed to initialize - "
                        "using console logging only"
                    )
            except Exception as e:
                logger.warning(f"Could not set up OCI logging: {e}")
        else:
            logger.warning(
                "ENVIRONMENT is 'production' but OCI_LOG_OCID is not set. "
                "Skipping OCI logging."
            )


# =============================================================================
# SECRET MANAGEMENT
# =============================================================================

# =============================================================================
# SECRET MANAGEMENT (DISABLED - Using config file instead)
# =============================================================================

# VAULT FUNCTIONALITY HAS BEEN DISABLED
# Credentials are now loaded from config.json file
# This function is preserved for reference but not used

def get_oci_vault_secrets():
    """
    DISABLED: Vault functionality has been removed.
    
    Credentials are now loaded from config.json file using load_config_file().
    This function returns None and logs a warning if called.
    
    Original functionality preserved below for reference.
    """
    logger.warning(
        "⚠️  get_oci_vault_secrets() was called but vault functionality is disabled. "
        "Credentials should be loaded from config.json instead."
    )
    return None


# Original vault implementation preserved below for reference (not executed):
"""
def get_oci_vault_secrets():
    '''
    Retrieve secrets from Oracle Cloud Infrastructure Vault using Resource Principals.
    
    Resource Principals are the correct authentication method for OCI Container Instances
    and automatically use the dynamic group permissions you've configured.
    
    Returns:
        dict: Dictionary containing retrieved secrets, or None if failed
        
    Expected Environment Variables:
        VAULT_OCID: OCID of the OCI Vault
        OCI_COMPARTMENT_ID: OCID of the compartment containing the vault
        AWS_ACCESS_KEY_SECRET_NAME: Name of AWS access key secret in vault
        AWS_SECRET_ACCESS_KEY_SECRET_NAME: Name of AWS secret key secret in vault
        DB_PASSWORD_SECRET_NAME: Name of database password secret in vault
    '''
    '''
    if not oci:
        logger.error("Cannot fetch secrets because OCI SDK is not available.")
        return None

    logger.info("🔐 Retrieving secrets from OCI Vault using Resource Principals...")
    
    # Define which secrets to retrieve from the vault
    secrets_to_fetch = {
        'AWS_ACCESS_KEY_ID': os.environ.get('AWS_ACCESS_KEY_SECRET_NAME'),
        'AWS_SECRET_ACCESS_KEY': os.environ.get('AWS_SECRET_ACCESS_KEY_SECRET_NAME'),
        'DB_PASSWORD': os.environ.get('DB_PASSWORD_SECRET_NAME')
    }
    
    vault_id = os.environ.get('VAULT_OCID')
    compartment_id = os.environ.get('OCI_COMPARTMENT_ID')
    
    if not vault_id:
        logger.error("VAULT_OCID environment variable not set.")
        return None
    
    if not compartment_id:
        logger.error("OCI_COMPARTMENT_ID environment variable not set.")
        return None

    retrieved_secrets = {}
    
    try:
        # Use Resource Principals authentication (correct for Container Instances)
        signer = oci.auth.signers.get_resource_principals_signer()
        logger.info("✅ Using Resource Principals authentication for vault access")
        
        # Create both vault client (for listing) and secrets client (for retrieving)
        vault_client = oci.vault.VaultsClient(config={}, signer=signer)
        secrets_client = oci.secrets.SecretsClient(config={}, signer=signer)
        
        # Retrieve each configured secret from the vault
        for key, secret_name in secrets_to_fetch.items():
            if not secret_name:
                logger.warning(
                    f"Secret name for {key} is not set in environment variables. "
                    "Skipping."
                )
                continue

            logger.info(f"  -> Fetching secret: {secret_name}")
            
            try:
                # First, find the secret by name to get its OCID
                list_secrets_response = vault_client.list_secrets(
                    compartment_id=compartment_id,
                    vault_id=vault_id,
                    name=secret_name,
                    lifecycle_state="ACTIVE"
                )
                
                if not list_secrets_response.data:
                    logger.error(
                        f"Secret '{secret_name}' not found in vault '{vault_id}' "
                        f"or not in ACTIVE state."
                    )
                    continue

                secret_ocid = list_secrets_response.data[0].id
                logger.debug(f"   Found secret OCID: {secret_ocid}")
                
                # Fetch the actual secret content using its OCID
                secret_bundle = secrets_client.get_secret_bundle(secret_id=secret_ocid)
                
                # Decode the base64 encoded secret content
                base64_content = secret_bundle.data.secret_bundle_content.content
                decoded_content = base64.b64decode(base64_content).decode('utf-8')
                retrieved_secrets[key] = decoded_content
                logger.info(f"  -> ✅ Successfully retrieved secret for {key}")

            except Exception as e:
                logger.error(f"Failed to retrieve secret '{secret_name}': {e}")
                continue

    except Exception as e:
        logger.exception(f"Failed to authenticate or connect to OCI Vault: {e}")
        logger.error("   Make sure your Container Instance is in the dynamic group")
        logger.error("   and the dynamic group has the correct vault permissions.")
        return None
        
    # Validate that all required secrets were retrieved
    required_secrets = [name for name in secrets_to_fetch.values() if name]
    if len(retrieved_secrets) < len(required_secrets):
        logger.error(
            f"Not all required secrets could be retrieved from the vault. "
            f"Retrieved {len(retrieved_secrets)} of {len(required_secrets)} secrets."
        )
        return None

    logger.info(f"✅ Successfully retrieved {len(retrieved_secrets)} secrets from OCI Vault")
    return retrieved_secrets
"""



# =============================================================================
# DATABASE CONNECTION AND OPERATIONS
# =============================================================================

# CSV DATA PARSING FUNCTIONS
# =============================================================================

def parse_marina_locations_data(csv_content):
    """
    Parse MarinaLocations CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing marina location data for database insertion
    """
    def safe_string(value, max_length=None, allow_null=True):
        """Safely convert value to string with optional length limit and NULL handling."""
        if value is None or str(value).strip() in ['', 'NULL', 'N/A', 'NONE']:
            return None if allow_null else ''
        result = str(value).strip()
        return result[:max_length] if max_length else result
    
    locations = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            location_data = (
                safe_string(row.get('Id'), allow_null=False) or '',  # ID cannot be null
                safe_string(row.get('Name'), 255, allow_null=False) or 'Unknown',  # Name cannot be null
                safe_string(row.get('PrimaryPhoneNumber'), 50),  # Can be null
                safe_string(row.get('PrimaryFaxNumber'), 50),  # Can be null
                safe_string(row.get('Organization_Id')),  # Can be null
                safe_string(row.get('MarinaHash'), 100),  # Can be null
                safe_string(row.get('UnitSystem'), 20),  # Can be null
                safe_string(row.get('DefaultArrivalTime'), 10),  # Can be null
                safe_string(row.get('DefaultDepartureTime'), 10),  # Can be null
                safe_string(row.get('EmailAddress'), 255),  # Can be null
                safe_string(row.get('MarinaWebsite'), 500),  # Can be null
                safe_string(row.get('TimeZone'), 100)  # Can be null
            )
            locations.append(location_data)
        except Exception as e:
            logger.warning(f"Error parsing marina location row: {e}")
            continue
    
    return locations


def parse_piers_data(csv_content):
    """
    Parse Piers CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing pier data for database insertion
    """
    piers = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            pier_data = (
                row.get('Id', '').strip(),
                row.get('Name', '').strip()[:255],
                row.get('MarinaLocationId', '').strip()
            )
            piers.append(pier_data)
        except Exception as e:
            logger.warning(f"Error parsing pier row: {e}")
            continue
    
    return piers


def parse_slip_types_data(csv_content):
    """
    Parse SlipTypes CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing slip type data for database insertion
    """
    slip_types = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            slip_type_data = (
                row.get('Id', '').strip(),
                row.get('Name', '').strip()[:255]
            )
            slip_types.append(slip_type_data)
        except Exception as e:
            logger.warning(f"Error parsing slip type row: {e}")
            continue
    
    return slip_types


def parse_slips_data(csv_content):
    """
    Parse Slips CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing slip data for database insertion
    """
    slips = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            slip_data = (
                parse_int(row.get('Id', '')),
                row.get('Name', '').strip()[:255] if row.get('Name') else None,
                row.get('Type', '').strip()[:50] if row.get('Type') else None,
                row.get('RecomendedLOA', '').strip()[:50] if row.get('RecomendedLOA') else None,
                row.get('RecomendedBeam', '').strip()[:50] if row.get('RecomendedBeam') else None,
                row.get('RecomendedDraft', '').strip()[:50] if row.get('RecomendedDraft') else None,
                row.get('RecomendedAirDraft', '').strip()[:50] if row.get('RecomendedAirDraft') else None,
                row.get('MaximumLOA', '').strip()[:50] if row.get('MaximumLOA') else None,
                row.get('MaximumBeam', '').strip()[:50] if row.get('MaximumBeam') else None,
                row.get('MaximumDraft', '').strip()[:50] if row.get('MaximumDraft') else None,
                row.get('MaximumAirDraft', '').strip()[:50] if row.get('MaximumAirDraft') else None,
                parse_int(row.get('MarinaLocationId', '')),
                parse_int(row.get('Pier_Id', '')),
                row.get('Status', '').strip()[:50] if row.get('Status') else None,
                parse_datetime(row.get('StartDate', '')),  # START_DATE
                parse_datetime(row.get('EndDate', '')),    # END_DATE
                parse_boolean(row.get('DoNotCountInOccupancy', '')),  # DO_NOT_COUNT_IN_OCCUPANCY
                parse_boolean(row.get('Active', '')),  # Convert TRUE/FALSE to 1/0
                parse_datetime(row.get('CreationDateTime', '')),  # CREATION_DATE_TIME - KEY FIELD
                row.get('CreationUser', '').strip()[:255] if row.get('CreationUser') else None,  # CREATION_USER
                parse_int(row.get('SlipType_Id', '')),
                parse_float(row.get('PaymentProcessingFee', '0')),  # PAYMENT_PROCESSING_FEE
                parse_float(row.get('ManagementFee', '0')),  # MANAGEMENT_FEE
                parse_int(row.get('OwnerId', '')),  # OWNER_ID
                parse_int(row.get('PaymentProcessingFeeTypeId', '')),  # PAYMENT_PROCESSING_FEE_TYPE_ID
                parse_int(row.get('ManagementFeeTypeId', '')),  # MANAGEMENT_FEE_TYPE_ID
                row.get('OverrideOccupancyLOA', '').strip()[:50] if row.get('OverrideOccupancyLOA') else None,
                row.get('HashID', '').strip()[:50] if row.get('HashID') else None,
                parse_float(row.get('MaintenanceFee', '0')),  # MAINTENANCE_FEE
                row.get('SvgId', '').strip()[:255] if row.get('SvgId') else None,  # SVG_ID
                parse_float(row.get('Assessment', '0')),  # ASSESSMENT
                parse_float(row.get('Loan', '0')),  # LOAN
                parse_int(row.get('OrderColumn', '')),  # ORDER_COLUMN
                row.get('SignName', '').strip()[:255] if row.get('SignName') else None,  # SIGN_NAME
                parse_float(row.get('MaxWeight', '0'))  # MAX_WEIGHT
            )
            slips.append(slip_data)
        except Exception as e:
            logger.warning(f"Error parsing slip row: {e}")
            continue
    
    return slips


def parse_reservations_data(csv_content):
    """
    Parse Reservations CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing reservation data for database insertion
    """
    reservations = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            reservation_data = (
                row.get('Id', '').strip(),
                row.get('MarinaLocationId', '').strip(),
                parse_datetime(row.get('CreationTime', '').strip()),
                row.get('ReservationStatusId', '').strip(),
                row.get('ReservationTypeId', '').strip(),
                row.get('ContactId', '').strip(),
                row.get('BoatId', '').strip(),
                parse_datetime(row.get('ScheduledArrivalTime', '').strip()),
                parse_datetime(row.get('ScheduledDepartureTime', '').strip()),
                parse_datetime(row.get('CancellationTime', '').strip()),
                row.get('AccountId', '').strip(),
                row.get('SlipId', '').strip(),
                row.get('Rate', '').strip(),
                row.get('Name', '').strip()[:500],
                row.get('HashID', '').strip()[:50],
                row.get('ReservationSource', '').strip()[:50]
            )
            reservations.append(reservation_data)
        except Exception as e:
            logger.warning(f"Error parsing reservation row: {e}")
            continue
    
    return reservations


def parse_companies_data(csv_content):
    """
    Parse Companies CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing company data for database insertion
    """
    companies = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            company_data = (
                row.get('Id', '').strip(),
                row.get('Name', '').strip()[:255],
                row.get('Owner', '').strip()[:255],
                row.get('PrimaryFaxNumber', '').strip()[:50],
                row.get('PrimaryPhoneNumber', '').strip()[:50],
                row.get('City_Id', '').strip(),
                row.get('Image', '').strip()[:500],
                row.get('Description', '').strip()[:2000],
                row.get('PartnerId', '').strip(),
                row.get('MoloAPI_Partner_Id', '').strip(),
                row.get('CompanyMoloAPI_Partner_Company_Id', '').strip(),
                row.get('InvoiceAtCompanyLevel', '').strip()[:10],
                row.get('MoloContactId', '').strip(),
                row.get('StripeCustomerId', '').strip()[:255],
                row.get('LoginProviderId', '').strip(),
                row.get('DefaultCCFee', '').strip(),
                row.get('Tier1PercentACHFee', '').strip(),
                row.get('Tier2PercentACHFee', '').strip()
            )
            companies.append(company_data)
        except Exception as e:
            logger.warning(f"Error parsing company row: {e}")
            continue
    
    return companies


def parse_contacts_data(csv_content):
    """
    Parse Contacts CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing contact data for database insertion
    """
    def safe_int(value, default=0):
        """Safely convert value to integer with robust error handling."""
        if value is None or str(value).strip() in ['', 'NULL', 'N/A', 'NONE']:
            return default
        try:
            # Handle "1.0" format integers
            if isinstance(value, str) and '.' in value:
                float_val = float(value.strip())
                if float_val.is_integer():
                    return int(float_val)
            return int(str(value).strip())
        except (ValueError, TypeError):
            return default

    def safe_float(value, default=0.0):
        """Safely convert value to float with robust error handling."""
        if value is None or str(value).strip() in ['', 'NULL', 'N/A', 'NONE']:
            return default
        try:
            return float(str(value).strip())
        except (ValueError, TypeError):
            return default

    def safe_bool_as_int(value, default=0):
        """Safely convert boolean value to integer (1/0) with robust error handling."""
        if value is None or str(value).strip() in ['', 'NULL', 'N/A', 'NONE']:
            return default
        str_value = str(value).strip().upper()
        if str_value in ['TRUE', '1', 'YES', 'Y']:
            return 1
        elif str_value in ['FALSE', '0', 'NO', 'N']:
            return 0
        else:
            return default

    def safe_string(value, max_length=None, allow_null=False):
        """Safely convert value to string with optional length limit and NULL handling."""
        if value is None or str(value).strip() in ['', 'NULL', 'N/A', 'NONE']:
            return None if allow_null else ''
        result = str(value).strip()
        return result[:max_length] if max_length else result

    def safe_datetime(value):
        """Safely convert datetime value with robust error handling."""
        if value is None or str(value).strip() in ['', 'NULL', 'N/A', 'NONE']:
            return None  # Allow NULL for datetime fields
        try:
            date_str = str(value).strip()
            if not date_str:
                return None
            
            # Handle different date formats including MM/dd/yyyy HH:mm:ss
            date_formats = [
                '%Y-%m-%d %H:%M:%S',
                '%m/%d/%Y %H:%M:%S', 
                '%m/%d/%Y',
                '%Y-%m-%d', 
                '%d/%m/%Y', 
                '%Y/%m/%d', 
                '%m-%d-%Y'
            ]
            
            for fmt in date_formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
            
            # If no format matches, log and return None
            logger.warning(f"Could not parse date: {date_str}")
            return None
        except:
            return None

    def safe_date(value):
        """Safely convert date value with robust error handling."""
        if value is None or str(value).strip() in ['', 'NULL', 'N/A', 'NONE']:
            return None  # Allow NULL for date fields
        try:
            date_str = str(value).strip()
            if not date_str:
                return None
            
            # Handle different date formats including MM/dd/yyyy HH:mm:ss
            date_formats = [
                '%m/%d/%Y %H:%M:%S', 
                '%m/%d/%Y',
                '%Y-%m-%d', 
                '%d/%m/%Y', 
                '%Y/%m/%d', 
                '%m-%d-%Y'
            ]
            
            for fmt in date_formats:
                try:
                    parsed_date = datetime.strptime(date_str, fmt)
                    return parsed_date.date()  # Return just the date part
                except ValueError:
                    continue
            
            # If no format matches, log and return None
            logger.warning(f"Could not parse date: {date_str}")
            return None
        except:
            return None

    contacts = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            contact_data = (
                safe_string(row.get('Id'), allow_null=False) or '',
                safe_string(row.get('Emails'), 1000, allow_null=False) or '',
                safe_string(row.get('FirstName'), 255, allow_null=False) or '',
                safe_string(row.get('MiddleName'), 255, allow_null=False) or '',
                safe_string(row.get('LastName'), 255, allow_null=False) or '',
                safe_string(row.get('MarinaLocationId'), allow_null=False) or '',
                safe_string(row.get('Notes'), 2000, allow_null=False) or '',
                safe_string(row.get('RecordStatusId'), allow_null=False) or '',
                safe_bool_as_int(row.get('IsSupplier'), 0),
                safe_bool_as_int(row.get('IsCustomer'), 0),
                safe_string(row.get('XeroId'), 255, allow_null=False) or '',
                safe_string(row.get('CompanyContactName'), 255, allow_null=False) or '',
                safe_string(row.get('CreationUser'), 255, allow_null=False) or '',
                safe_datetime(row.get('CreationDateTime')),
                safe_string(row.get('CIM_Id'), 255, allow_null=False) or '',
                safe_string(row.get('MarinaLocation1_Id'), allow_null=False) or '',
                safe_string(row.get('QB_Customer_Id'), 255, allow_null=False) or '',
                safe_string(row.get('StatementsPreference_Id'), allow_null=False) or '',
                safe_string(row.get('HashID'), 50, allow_null=False) or '',
                safe_string(row.get('MoloAPI_PartnerId'), allow_null=False) or '',
                safe_bool_as_int(row.get('TaxExemptStatus'), 0),
                safe_float(row.get('AutomaticDiscountPercent'), 0.0),
                safe_float(row.get('CostPlusDiscount'), 0.0),
                safe_string(row.get('LinkedParentContact'), allow_null=False) or '',
                safe_string(row.get('ContactAutoChargeId'), allow_null=False) or '',
                safe_datetime(row.get('LastEditedDateTime')),
                safe_string(row.get('LastEditedUser_Id'), allow_null=False) or '',
                safe_string(row.get('LastEditedMoloAPIPartner_Id'), allow_null=False) or '',
                safe_string(row.get('StripeCustomer_Id'), 255, allow_null=False) or '',
                safe_float(row.get('AccountLimit'), 0.0),
                safe_string(row.get('Filestack_Id'), 255, allow_null=False) or '',
                safe_bool_as_int(row.get('ShowCompanyNamePrinted'), 0),
                safe_bool_as_int(row.get('BookingMergingDone'), 0),
                safe_date(row.get('DateOfBirth')),
                safe_string(row.get('IDSCustomerID'), 255, allow_null=False) or '',
                safe_bool_as_int(row.get('DoNotLaunch'), 0),
                safe_string(row.get('DoNotLaunchReason'), 500, allow_null=False) or '',
                safe_string(row.get('DriverLicenseId'), 255, allow_null=False) or '',
                safe_string(row.get('QuickbooksId'), 255, allow_null=False) or '',
                safe_string(row.get('QuickbooksName'), 255, allow_null=False) or '',
                safe_string(row.get('QBOVendorId'), 255, allow_null=False) or '',
                safe_bool_as_int(row.get('SkipForFinanceCharges'), 0),
                safe_string(row.get('MainContactId'), allow_null=False) or ''
            )
            contacts.append(contact_data)
        except Exception as e:
            logger.warning(f"Error parsing contact row {row.get('Id', 'Unknown')}: {e}")
            logger.warning(f"Problematic row data: {row}")
            continue
    
    return contacts


def parse_boats_data(csv_content):
    """
    Parse Boats CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing boat data for database insertion
    """
    boats = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            boat_data = (
                row.get('Id', '').strip(),
                row.get('Photo', '').strip()[:500],
                row.get('Make', '').strip()[:255],
                row.get('Model', '').strip()[:255],
                row.get('Name', '').strip()[:255],
                row.get('LOA', '').strip()[:50],
                row.get('Beam', '').strip()[:50],
                row.get('Draft', '').strip()[:50],
                row.get('AirDraft', '').strip()[:50],
                row.get('RegistrationNumber', '').strip()[:255],
                row.get('RegistrationState', '').strip()[:50],
                parse_datetime(row.get('CreationTime', '')),
                row.get('BoatTypeId', '').strip(),
                row.get('MarinaLocationId', '').strip(),
                row.get('PowerNeedId', '').strip(),
                row.get('Notes', '').strip()[:2000] if row.get('Notes') else '',
                row.get('RecordStatusId', '').strip(),
                row.get('AspNetUser_Id', '').strip()[:255],
                row.get('MastLength', '').strip()[:50],
                row.get('Weight', '').strip()[:50],
                row.get('Color', '').strip()[:100],
                row.get('HullID', '').strip()[:255],
                row.get('KeyLocationCode', '').strip()[:100],
                row.get('Year', '').strip()[:10],
                row.get('HashID', '').strip()[:50],
                row.get('MoloAPI_PartnerId', '').strip(),
                row.get('PowerNeed1_Id', '').strip(),
                parse_datetime(row.get('LastEditedDateTime', '')),
                row.get('LastEditedUser_Id', '').strip(),
                row.get('LastEditedMoloAPIPartner_Id', '').strip(),
                row.get('Filestack_Id', '').strip()[:255],
                row.get('Tonnage', '').strip()[:50],
                row.get('GallonCapacity', '').strip()[:50],
                row.get('IsActive', '').strip()[:10],
                row.get('BookingMergingDone', '').strip()[:10],
                row.get('DecalNumber', '').strip()[:100],
                row.get('Manufacturer', '').strip()[:255],
                row.get('SerialNumber', '').strip()[:255],
                parse_date(row.get('RegistrationExpiration', ''))
            )
            boats.append(boat_data)
        except Exception as e:
            logger.warning(f"Error parsing boat row: {e}")
            continue
    
    return boats


def parse_accounts_data(csv_content):
    """
    Parse Accounts CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing account data for database insertion
    """
    accounts = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            account_data = (
                row.get('Id', '').strip(),
                row.get('AccountStatusId', '').strip(),
                row.get('MarinaLocationId', '').strip(),
                row.get('Contact_Id', '').strip()
            )
            accounts.append(account_data)
        except Exception as e:
            logger.warning(f"Error parsing account row: {e}")
            continue
    
    return accounts


def parse_invoices_data(csv_content):
    """
    Parse InvoiceSet CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing invoice data for database insertion
    """
    def safe_int(value, default=0):
        """Safely convert value to integer with robust error handling."""
        if value is None or str(value).strip() in ['', 'NULL', 'N/A', 'NONE']:
            return default
        try:
            # Handle "1.0" format integers
            if isinstance(value, str) and '.' in value:
                float_val = float(value.strip())
                if float_val.is_integer():
                    return int(float_val)
            return int(str(value).strip())
        except (ValueError, TypeError):
            return default

    def safe_float(value, default=0.0):
        """Safely convert value to float with robust error handling."""
        if value is None or str(value).strip() in ['', 'NULL', 'N/A', 'NONE']:
            return default
        try:
            return float(str(value).strip())
        except (ValueError, TypeError):
            return default

    def safe_bool_as_int(value, default=0):
        """Safely convert boolean value to integer (1/0) with robust error handling."""
        if value is None or str(value).strip() in ['', 'NULL', 'N/A', 'NONE']:
            return default
        str_value = str(value).strip().upper()
        if str_value in ['TRUE', '1', 'YES', 'Y']:
            return 1
        elif str_value in ['FALSE', '0', 'NO', 'N']:
            return 0
        else:
            return default

    def safe_string(value, max_length=None, allow_null=False):
        """Safely convert value to string with optional length limit and NULL handling."""
        if value is None or str(value).strip() in ['', 'NULL', 'N/A', 'NONE']:
            return None if allow_null else ''
        result = str(value).strip()
        return result[:max_length] if max_length else result

    def safe_datetime(value):
        """Safely convert datetime value with robust error handling."""
        if value is None or str(value).strip() in ['', 'NULL', 'N/A', 'NONE']:
            return None  # Allow NULL for datetime fields
        try:
            result = parse_datetime(str(value).strip())
            return result if result else None
        except:
            return None

    invoices = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            invoice_data = (
                safe_string(row.get('Id'), allow_null=False) or '',  # ID cannot be null
                safe_datetime(row.get('Date')),
                safe_float(row.get('DolarDiscount'), 0.0),  # Explicit defaults
                safe_float(row.get('PercentDiscount'), 0.0),
                safe_bool_as_int(row.get('Active'), 0),  # Boolean as 1/0
                safe_datetime(row.get('ClosingDate')),
                safe_float(row.get('DiscountTotal'), 0.0),
                safe_bool_as_int(row.get('Opened'), 0),  # Boolean as 1/0
                safe_float(row.get('Payed'), 0.0),
                safe_float(row.get('Subtotal'), 0.0),
                safe_float(row.get('SubtotalWoDiscount'), 0.0),
                safe_float(row.get('TaxTotal'), 0.0),
                safe_string(row.get('Title'), 500, allow_null=False) or 'Untitled Invoice',
                safe_float(row.get('Total'), 0.0),
                safe_int(row.get('ReservationId'), 0),  # Explicit 0 default
                safe_int(row.get('AccountId'), 0),
                safe_float(row.get('ServicePaidAmount'), 0.0),
                safe_float(row.get('MarinaPaidAmount'), 0.0),
                safe_float(row.get('GasPaidAmount'), 0.0),
                safe_int(row.get('InvoiceStatusId'), 0),
                safe_datetime(row.get('StartDate')),
                safe_int(row.get('InstalmentsPaymentMethodId'), 0),
                safe_bool_as_int(row.get('ScheduledForCron'), 0),  # Boolean as 1/0
                safe_string(row.get('OriginalInvoice'), allow_null=False) or '',
                safe_bool_as_int(row.get('PaymentsSentToXero'), 0),  # Boolean as 1/0
                safe_int(row.get('WorkOrderId'), 0),
                safe_bool_as_int(row.get('IsInstallmentInvoice'), 0),  # Boolean as 1/0
                safe_string(row.get('VoidUser'), 255, allow_null=False) or '',
                safe_datetime(row.get('VoidDateTime')),
                safe_string(row.get('CreationUser'), 255, allow_null=False) or '',
                safe_int(row.get('Payment_Id'), 0),
                safe_string(row.get('QB_Invoice_Id'), 255, allow_null=False) or '',
                safe_int(row.get('InvoiceType_Id'), 0),
                safe_datetime(row.get('InvoiceDate')),
                safe_datetime(row.get('DueDate')),
                safe_string(row.get('CurrencyCode'), 10, allow_null=False) or '',
                safe_datetime(row.get('LastModifiedDateTime')),
                safe_string(row.get('LastModifiedAspNetUser'), 255, allow_null=False) or '',
                safe_string(row.get('VoidReason'), 500, allow_null=False) or '',
                safe_int(row.get('CreatePartnerId'), 0),
                safe_int(row.get('VoidPartnerId'), 0),
                safe_string(row.get('UpdateHash'), 500, allow_null=False) or '',
                safe_bool_as_int(row.get('ScheduledForInventoryCron'), 0),  # Boolean as 1/0
                safe_bool_as_int(row.get('ScheduledForSubletCron'), 0),  # Boolean as 1/0
                safe_bool_as_int(row.get('ScheduledForLaborCron'), 0),  # Boolean as 1/0
                safe_bool_as_int(row.get('CreatedOnMobile'), 0),  # Boolean as 1/0
                safe_string(row.get('StripeInvoiceId'), 255, allow_null=False) or '',
                safe_bool_as_int(row.get('SentToStripe'), 0),  # Boolean as 1/0
                safe_int(row.get('ResourceBookingId'), 0),
                safe_bool_as_int(row.get('ModifiedOnMobile'), 0),  # Boolean as 1/0
                safe_string(row.get('Note'), 2000, allow_null=False) or '',
                safe_string(row.get('QuickbooksInvoiceId'), 255, allow_null=False) or '',
                safe_float(row.get('TaxCap'), 0.0),
                safe_bool_as_int(row.get('IsSurcharge'), 0)  # Boolean as 1/0
            )
            invoices.append(invoice_data)
        except Exception as e:
            logger.warning(f"Error parsing invoice row {row.get('Id', 'Unknown')}: {e}")
            logger.warning(f"Problematic row data: {row}")
            continue
    
    return invoices


def parse_invoice_items_data(csv_content):
    """
    Parse InvoiceItemSet CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing invoice item data for database insertion
    """
    def safe_int(value, default=0):
        """Safely convert value to integer with robust error handling."""
        if value is None or str(value).strip() in ['', 'NULL', 'N/A', 'NONE']:
            return default
        try:
            # Handle "1.0" format integers
            if isinstance(value, str) and '.' in value:
                float_val = float(value.strip())
                if float_val.is_integer():
                    return int(float_val)
            return int(str(value).strip())
        except (ValueError, TypeError):
            return default

    def safe_float(value, default=0.0):
        """Safely convert value to float with robust error handling."""
        if value is None or str(value).strip() in ['', 'NULL', 'N/A', 'NONE']:
            return default
        try:
            return float(str(value).strip())
        except (ValueError, TypeError):
            return default

    def safe_bool_as_int(value, default=0):
        """Safely convert boolean value to integer (1/0) with robust error handling."""
        if value is None or str(value).strip() in ['', 'NULL', 'N/A', 'NONE']:
            return default
        str_value = str(value).strip().upper()
        if str_value in ['TRUE', '1', 'YES', 'Y']:
            return 1
        elif str_value in ['FALSE', '0', 'NO', 'N']:
            return 0
        else:
            return default

    def safe_string(value, max_length=None, allow_null=False):
        """Safely convert value to string with optional length limit and NULL handling."""
        if value is None or str(value).strip() in ['', 'NULL', 'N/A', 'NONE']:
            return None if allow_null else ''
        result = str(value).strip()
        return result[:max_length] if max_length else result

    def safe_datetime(value):
        """Safely convert datetime value with robust error handling."""
        if value is None or str(value).strip() in ['', 'NULL', 'N/A', 'NONE']:
            return None  # Allow NULL for datetime fields
        try:
            result = parse_datetime(str(value).strip())
            return result if result else None
        except:
            return None

    invoice_items = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            invoice_item_data = (
                safe_string(row.get('Id'), allow_null=False) or '',
                safe_string(row.get('Prefix'), 10, allow_null=False) or '',
                safe_float(row.get('Quantity'), 0.0),
                safe_string(row.get('Title'), 500, allow_null=False) or 'Untitled Item',
                safe_string(row.get('Type'), 100, allow_null=False) or '',
                safe_float(row.get('Value'), 0.0),
                safe_float(row.get('Discount'), 0.0),
                safe_string(row.get('DiscountType'), 50, allow_null=False) or '',
                safe_bool_as_int(row.get('Taxable'), 0),
                safe_float(row.get('Tax'), 0.0),
                safe_string(row.get('Misc'), 500, allow_null=False) or '',
                safe_float(row.get('DiscountTotal'), 0.0),
                safe_string(row.get('PriceSuffix'), 50, allow_null=False) or '',
                safe_float(row.get('SubTotal'), 0.0),
                safe_float(row.get('SubtotalWoDiscount'), 0.0),
                safe_float(row.get('TaxTotal'), 0.0),
                safe_float(row.get('Total'), 0.0),
                safe_string(row.get('InvoiceId'), allow_null=False) or '',
                safe_string(row.get('ChargeGroup'), 100, allow_null=False) or '',
                safe_string(row.get('PaymentAccount'), 100, allow_null=False) or '',
                safe_string(row.get('PriceStr'), 100, allow_null=False) or '',
                safe_bool_as_int(row.get('IsVoid'), 0),
                safe_datetime(row.get('Date')),  # DATE_FIELD
                safe_string(row.get('TextAux'), 1000, allow_null=True),
                safe_string(row.get('TextAux2'), 1000, allow_null=True),
                safe_datetime(row.get('DiscountDateTime')),
                safe_string(row.get('DiscountUserId'), 1000, allow_null=True),
                safe_string(row.get('Notes'), 1000, allow_null=True),
                safe_string(row.get('PrType'), 1000, allow_null=True),
                safe_string(row.get('Status'), 1000, allow_null=True),
                safe_string(row.get('DeletionUserId'), 1000, allow_null=True),
                safe_datetime(row.get('DeletionDateTime')),
                safe_int(row.get('OverpaymentId'), 0),
                safe_string(row.get('VoidUser'), 1000, allow_null=True),
                safe_datetime(row.get('VoidDateTime')),
                safe_string(row.get('Misc2'), 1000, allow_null=True),
                safe_int(row.get('PrepaymentId'), 0),
                safe_int(row.get('CreditInvoiceId'), 0),
                safe_string(row.get('AllocationType'), 1000, allow_null=True),
                safe_int(row.get('ReservationId'), 0),
                safe_int(row.get('ItemMasterId'), 0),
                safe_string(row.get('AspNetUserId'), 256, allow_null=True),
                safe_int(row.get('InvoiceItemTypeId'), 0),
                safe_int(row.get('SeasonalPriceId'), 0),
                safe_int(row.get('TransientPriceId'), 0),
                safe_int(row.get('SvJobId'), 0),
                safe_int(row.get('OriginalCreditItem'), 0),
                safe_bool_as_int(row.get('TaxExempt'), 0),
                safe_datetime(row.get('LastModifiedDateTime')),
                safe_string(row.get('LastModifiedAspNetUser'), 1000, allow_null=True),
                safe_string(row.get('DeletionReason'), 1000, allow_null=True),
                safe_string(row.get('VoidReason'), 1000, allow_null=True),
                safe_datetime(row.get('StartDateTime')),
                safe_datetime(row.get('EndDateTime')),
                safe_int(row.get('CreationPartnerId'), 0),
                safe_int(row.get('DeletePartnerId'), 0),
                safe_int(row.get('VoidPartnerId'), 0),
                safe_float(row.get('OriginalPrice'), 0.0),
                safe_int(row.get('OverrideXeroTaxRate'), 0),
                safe_int(row.get('OverrideXeroSalesAccount'), 0),
                safe_float(row.get('OriginalReservationPrice'), 0.0),
                safe_int(row.get('NumberOfDecimals'), 0),
                safe_int(row.get('StripeTransactionDataId'), 0),
                safe_float(row.get('ValueAlternative'), 0.0),
                safe_int(row.get('StripeTerminalId'), 0),
                safe_string(row.get('StripeApplicationName'), 1000, allow_null=True),
                safe_string(row.get('StripeAid'), 1000, allow_null=True),
                safe_float(row.get('EnteredAmount'), 0.0),
                safe_float(row.get('ExchangeRate'), 0.0),
                safe_int(row.get('CurrenciesId'), 0),  # 69
                safe_int(row.get('SvChargeInstanceId'), 0),  # 70
                safe_int(row.get('SvLaborInstanceId'), 0),  # 71
                safe_int(row.get('SvPartInstanceId'), 0),  # 72
                safe_string(row.get('RevenueGlCode'), 1000, allow_null=True),  # 73
                safe_string(row.get('ArGlCode'), 1000, allow_null=True),  # 74
                safe_string(row.get('PaymentGlCode'), 1000, allow_null=True),  # 75
                safe_string(row.get('CogsGlCode'), 1000, allow_null=True),  # 76
                safe_string(row.get('InventoryGlCode'), 1000, allow_null=True),  # 77
                safe_string(row.get('SalesTaxGlCode'), 1000, allow_null=True),  # 78
                safe_string(row.get('PrepaymentGlCode'), 1000, allow_null=True),  # 79
                safe_string(row.get('AccountType'), 1000, allow_null=True),  # 80
                safe_string(row.get('ApplicationCryptogram'), 1000, allow_null=True),  # 81
                safe_string(row.get('AuthorizationCode'), 1000, allow_null=True),  # 82
                safe_string(row.get('AuthorizationResponseCode'), 1000, allow_null=True),  # 83
                safe_string(row.get('CardholderVerificationMethod'), 1000, allow_null=True),  # 84
                safe_string(row.get('TerminalVerificationResults'), 1000, allow_null=True),  # 85
                safe_string(row.get('TransactionStatusInformation'), 1000, allow_null=True),  # 86
                safe_string(row.get('TrackingCode'), 1000, allow_null=True),  # 87
                safe_string(row.get('DiscountGlCode'), 1000, allow_null=True),  # 88
                safe_int(row.get('OverrideTrackingCategory1'), 0),  # 89
                safe_int(row.get('OverrideTrackingCategory2'), 0),  # 90
                safe_string(row.get('QuickbooksPaymentId'), 1000, allow_null=True),  # 91
                safe_bool_as_int(row.get('AllowTotalPriceEntry'), 0),  # 92
                safe_bool_as_int(row.get('AddedAutomatically'), 0),  # 93
                safe_datetime(row.get('AllocationPerformedDate')),  # 94
                safe_datetime(row.get('CreatedDate'))  # 95 - THE KEY FIELD YOU MENTIONED
            )
            invoice_items.append(invoice_item_data)
        except Exception as e:
            logger.warning(f"Error parsing invoice item row {row.get('Id', 'Unknown')}: {e}")
            logger.warning(f"Problematic row data: {row}")
            continue
    
    return invoice_items


def parse_transactions_data(csv_content):
    """
    Parse transactions CSV data with robust numeric field handling.
    
    Args:
        csv_content (str): Raw CSV content for transactions
        
    Returns:
        list: List of tuples containing parsed transaction data
    """
    try:
        reader = csv.DictReader(io.StringIO(csv_content))
        parsed_data = []
        
        for row in reader:
            try:
                # Helper function to safely convert to number
                def safe_number(value, default=None):
                    if not value or str(value).strip().upper() in ['NULL', 'N/A', '', 'NONE']:
                        return default
                    try:
                        # Handle decimal values
                        if '.' in str(value):
                            return float(value)
                        return int(value)
                    except (ValueError, TypeError):
                        return default
                
                # Helper function to safely convert to integer
                def safe_int(value, default=None):
                    if not value or str(value).strip().upper() in ['NULL', 'N/A', '', 'NONE']:
                        return default
                    try:
                        return int(float(value))  # Handle "1.0" format
                    except (ValueError, TypeError):
                        return default
                
                # Helper function to safely convert to float
                def safe_float(value, default=None):
                    if not value or str(value).strip().upper() in ['NULL', 'N/A', '', 'NONE']:
                        return default
                    try:
                        return float(value)
                    except (ValueError, TypeError):
                        return default
                
                # Helper function to safely convert boolean-like fields
                def safe_bool_as_int(value, default=0):
                    if not value or str(value).strip().upper() in ['NULL', 'N/A', '', 'NONE']:
                        return default
                    try:
                        val_str = str(value).strip().upper()
                        if val_str in ['TRUE', '1', 'YES', 'Y']:
                            return 1
                        elif val_str in ['FALSE', '0', 'NO', 'N']:
                            return 0
                        else:
                            return int(float(value))
                    except (ValueError, TypeError):
                        return default
                
                # Helper function to safely handle strings
                def safe_string(value, max_length=None):
                    if not value:
                        return None
                    result = str(value).strip()
                    if max_length and len(result) > max_length:
                        result = result[:max_length]
                    return result if result else None
                
                parsed_row = (
                    safe_int(row.get('Id')),
                    safe_int(row.get('MarinaLocationId')),
                    parse_datetime(row.get('CreationTime')),
                    safe_int(row.get('InvoiceId')),
                    safe_int(row.get('TransactionTypeId')),
                    safe_int(row.get('TransactionMethodId')),
                    safe_float(row.get('Value')),
                    safe_bool_as_int(row.get('IsRefunded')),
                    safe_string(row.get('CustomerIPAddress'), 1000),
                    safe_string(row.get('CustomerDevice'), 1000),
                    safe_string(row.get('RefundReason'), 1000),
                    safe_string(row.get('Aux'), 1000),
                    safe_string(row.get('CheckNumber'), 1000),
                    safe_string(row.get('CCType'), 1000),
                    safe_int(row.get('InvoiceItemId')),
                    safe_bool_as_int(row.get('SentToXero')),
                    safe_string(row.get('OverpaymentID'), 1000),
                    safe_bool_as_int(row.get('PaymentCollectedOffline')),
                    safe_bool_as_int(row.get('PartOfOverpayment')),
                    safe_string(row.get('PrepaymentID'), 1000),
                    safe_int(row.get('AccountTransaction_Transaction_Id')),
                    safe_int(row.get('Payment_Id')),
                    parse_datetime(row.get('CreationDate')),
                    safe_string(row.get('AspNetUser_Id'), 256),
                    safe_string(row.get('HashID'), 1000),
                    safe_int(row.get('CustomTransactionMethodsId')),
                    safe_string(row.get('Reference'), 1000),
                    safe_bool_as_int(row.get('IsVoid')),
                    safe_float(row.get('AmountRefunded')),
                    safe_int(row.get('StripeTransactionDataId')),
                    safe_string(row.get('PaymentIntentId'), 1000),
                    safe_bool_as_int(row.get('SentToPayout')),
                    safe_int(row.get('StripeAuthorizations_Id')),
                    safe_int(row.get('StripeResponse_Id')),
                    safe_string(row.get('StripeReaderSerialNumber'), 1000),
                    safe_string(row.get('StripeTerminalId'), 1000),
                    safe_bool_as_int(row.get('CreatedOnMobile')),
                    safe_float(row.get('OnlinePercentFee')),
                    safe_float(row.get('OnlineFeeAmount')),
                    safe_bool_as_int(row.get('ScheduledForOnlineFeeCron')),
                    safe_int(row.get('OnlinePaymentFee_Id')),
                    safe_string(row.get('BankName'), 1000),
                    safe_string(row.get('Last4'), 1000),
                    safe_int(row.get('StripeBankAccountId')),
                    safe_int(row.get('StripeBatchId')),
                    safe_string(row.get('RoutingNumber'), 1000),
                    safe_bool_as_int(row.get('FullyRefunded')),
                    parse_datetime(row.get('LastUpdated')),
                    safe_string(row.get('PaymentSource'), 1000)
                )
                
                parsed_data.append(parsed_row)
                
            except Exception as e:
                logger.warning(f"Skipping invalid transaction row: {e}")
                continue
                
        logger.info(f"Successfully parsed {len(parsed_data)} transaction records")
        return parsed_data
        
    except Exception as e:
        logger.error(f"Failed to parse transactions data: {e}")
        return []


def parse_item_masters_data(csv_content):
    """
    Parse ItemMasters CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing item master data for database insertion
    """
    def safe_int(value, default=0):
        """Safely convert value to integer with robust error handling."""
        if value is None or str(value).strip() in ['', 'NULL', 'N/A', 'NONE']:
            return default
        try:
            # Handle "1.0" format integers
            if isinstance(value, str) and '.' in value:
                float_val = float(value.strip())
                if float_val.is_integer():
                    return int(float_val)
            return int(str(value).strip())
        except (ValueError, TypeError):
            return default

    def safe_float(value, default=0.0):
        """Safely convert value to float with robust error handling."""
        if value is None or str(value).strip() in ['', 'NULL', 'N/A', 'NONE']:
            return default
        try:
            return float(str(value).strip())
        except (ValueError, TypeError):
            return default

    def safe_bool_as_int(value, default=0):
        """Safely convert boolean value to integer (1/0) with robust error handling."""
        if value is None or str(value).strip() in ['', 'NULL', 'N/A', 'NONE']:
            return default
        str_value = str(value).strip().upper()
        if str_value in ['TRUE', '1', 'YES', 'Y']:
            return 1
        elif str_value in ['FALSE', '0', 'NO', 'N']:
            return 0
        else:
            return default

    def safe_string(value, max_length=None, allow_null=False):
        """Safely convert value to string with optional length limit and NULL handling."""
        if value is None or str(value).strip() in ['', 'NULL', 'N/A', 'NONE']:
            return None if allow_null else ''
        result = str(value).strip()
        return result[:max_length] if max_length else result

    def safe_datetime(value):
        """Safely convert value to datetime with robust error handling."""
        if value is None or str(value).strip() in ['', 'NULL', 'N/A', 'NONE']:
            return None
        try:
            from datetime import datetime
            str_value = str(value).strip()
            # Try parsing common datetime formats (including MM/DD/YYYY for Molo exports)
            for fmt in [
                '%m/%d/%Y %H:%M:%S',  # MM/DD/YYYY HH:MM:SS (Molo format)
                '%Y-%m-%d %H:%M:%S',  # YYYY-MM-DD HH:MM:SS
                '%Y-%m-%dT%H:%M:%S',  # ISO format
                '%m/%d/%Y',           # MM/DD/YYYY date only
                '%Y-%m-%d'            # YYYY-MM-DD date only
            ]:
                try:
                    return datetime.strptime(str_value, fmt)
                except ValueError:
                    continue
            return None
        except Exception:
            return None

    item_masters = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            item_master_data = (
                safe_string(row.get('Id'), allow_null=False) or '',  # 1
                safe_string(row.get('Name'), 255, allow_null=False) or 'Unknown Item',  # 2
                safe_float(row.get('Amount'), 0.0),  # 3
                safe_int(row.get('ItemChargeMethodId'), 0),  # 4
                safe_bool_as_int(row.get('Taxable'), 0),  # 5
                safe_bool_as_int(row.get('AvailableAsAddOn'), 0),  # 6
                safe_int(row.get('MarinaLocationId'), 0),  # 7
                safe_float(row.get('Price'), 0.0),  # 8
                safe_float(row.get('Tax'), 0.0),  # 9
                safe_bool_as_int(row.get('Single'), 0),  # 10
                safe_string(row.get('ChargeCategory'), 100, allow_null=False) or '',  # 11
                safe_bool_as_int(row.get('AmountIsDecimal'), 0),  # 12
                safe_int(row.get('NumberOfDecimals'), 0),  # 13
                safe_string(row.get('ItemShortName'), 100, allow_null=False) or '',  # 14
                safe_string(row.get('ItemCode'), 100, allow_null=False) or '',  # 15
                safe_bool_as_int(row.get('TrackedInventory'), 0),  # 16
                safe_float(row.get('QuantityOnHand'), 0.0),  # 17
                safe_float(row.get('PurchasePrice'), 0.0),  # 18
                safe_string(row.get('FirstTrackingCategory'), 255, allow_null=False) or '',  # 19
                safe_string(row.get('SecondTrackingCategory'), 255, allow_null=False) or '',  # 20
                safe_string(row.get('XeroID'), 255, allow_null=False) or '',  # 21
                safe_float(row.get('SaleFrequency'), 0.0),  # 22
                safe_float(row.get('LowQuantityWarning'), 0.0),  # 23
                safe_int(row.get('MarinaLocation1Id'), None),  # 24
                safe_int(row.get('MarinaLocation2Id'), None),  # 25
                safe_int(row.get('PedestalId'), None),  # 26
                safe_int(row.get('Pedestal1Id'), None),  # 27
                safe_int(row.get('QbItemId'), None),  # 28
                safe_int(row.get('XeroItemId'), None),  # 29
                safe_string(row.get('Barcode'), 100, allow_null=True),  # 30
                safe_bool_as_int(row.get('DistributeToOwners'), 0),  # 31
                safe_string(row.get('FuelCloudProductId'), 100, allow_null=True),  # 32
                safe_string(row.get('HashId'), 50, allow_null=False) or '',  # 33
                safe_bool_as_int(row.get('RequiresAgeVerification'), 0),  # 34
                safe_int(row.get('MinimumAge'), None),  # 35
                safe_datetime(row.get('CreationDateTime')),  # 36 - DATETIME FIELD!
                safe_string(row.get('CreationAspNetUserId'), 256, allow_null=True),  # 37
                safe_int(row.get('RecordStatusId'), 0),  # 38
                safe_string(row.get('UpdateHash'), 100, allow_null=True),  # 39
                safe_bool_as_int(row.get('SubletItem'), 0),  # 40
                safe_int(row.get('InternalRevenueXeroAccountId'), None),  # 41
                safe_int(row.get('InternalCogsXeroAccountId'), None),  # 42
                safe_int(row.get('WipXeroAccountId'), None),  # 43
                safe_int(row.get('InventoryRevaluationId'), None),  # 44
                safe_int(row.get('MarinaLocation6Id'), None),  # 45
                safe_string(row.get('FinaleProductUrl'), 500, allow_null=True),  # 46
                safe_string(row.get('RevenueGLCode'), 50, allow_null=True),  # 47
                safe_string(row.get('CogsGLCode'), 50, allow_null=True),  # 48
                safe_string(row.get('InventoryGLCode'), 50, allow_null=True),  # 49
                safe_string(row.get('ARGLCode'), 50, allow_null=True),  # 50
                safe_string(row.get('SalesTaxGLCode'), 50, allow_null=True),  # 51
                safe_bool_as_int(row.get('OnlyUseLast2Average'), 0),  # 52
                safe_bool_as_int(row.get('DeferredRevenueRecognition'), 0),  # 53
                safe_string(row.get('DeferredRecognitionGLCode'), 50, allow_null=True),  # 54
                safe_string(row.get('TrackingCode'), 100, allow_null=True),  # 55
                safe_bool_as_int(row.get('AddDescriptionToInvoiceNote'), 0),  # 56
                safe_int(row.get('MarinaLocation7Id'), None),  # 57
                safe_bool_as_int(row.get('IgnoreInventoryQoh'), 0),  # 58
                safe_float(row.get('QohCommitted'), 0.0),  # 59
                safe_float(row.get('QohOnOrder'), 0.0),  # 60
                safe_bool_as_int(row.get('AllowTotalPriceEntry'), 0),  # 61
                safe_int(row.get('MarinaLocation9Id'), None),  # 62
                safe_bool_as_int(row.get('AllowPostingToNonIncomeAccounts'), 0),  # 63
                safe_int(row.get('OrderColumn'), None),  # 64
                safe_bool_as_int(row.get('EnableNegativeInventory'), 0),  # 65
                safe_float(row.get('Wip'), 0.0)  # 66
            )
            
            # DEBUG: Log CreationDateTime for first 3 rows
            if len(item_masters) < 3:
                creation_dt_raw = row.get('CreationDateTime')
                creation_dt_parsed = item_master_data[35]  # Position 36 (0-indexed)
                logger.info(f"DEBUG ItemMaster ID={row.get('Id')}: CreationDateTime raw='{creation_dt_raw}' parsed={creation_dt_parsed}")
            
            item_masters.append(item_master_data)
        except Exception as e:
            logger.warning(f"Error parsing item master row {row.get('Id', 'Unknown')}: {e}")
            logger.warning(f"Problematic row data: {row}")
            continue
    
    return item_masters


def parse_seasonal_prices_data(csv_content):
    """
    Parse SeasonalPrices CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing seasonal price data for database insertion
    """
    seasonal_prices = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            seasonal_price_data = (
                parse_int(row.get('Id', '')),
                row.get('SeasonName', '').strip()[:255],
                parse_datetime(row.get('StartDate', '')),
                parse_datetime(row.get('EndDate', '')),
                parse_int(row.get('SeasonalChargeMethodId', '')),
                parse_float(row.get('PricePerFoot', '0')),
                parse_float(row.get('FlatRate', '0')),
                parse_boolean(row.get('Taxable', '')),  # Convert TRUE/FALSE to 1/0
                parse_int(row.get('MarinaLocationId', '')),
                parse_boolean(row.get('Active', '')),  # Convert TRUE/FALSE to 1/0
                parse_float(row.get('Tax', '0')),
                row.get('RateDetails', '').strip()[:500] if row.get('RateDetails') else '',
                row.get('RateShortName', '').strip()[:100],
                row.get('OnlinePaymentPlaceholder', '').strip()[:255],
                row.get('XeroItemCode', '').strip()[:100],
                row.get('XeroId', '').strip()[:255],
                row.get('FirstTrackingCategory', '').strip()[:255],
                row.get('SecondTrackingCategory', '').strip()[:255],
                parse_int(row.get('SeasonalInvoicingMethod_Id', '')),
                parse_datetime(row.get('CreationDateTime', '')),
                row.get('AspNetUser_Id', '').strip()[:255],
                row.get('CheckInTerms', '').strip()[:2000] if row.get('CheckInTerms') else '',
                row.get('CheckOutTerms', '').strip()[:2000] if row.get('CheckOutTerms') else '',
                row.get('OnlinePaymentCompletion', '').strip()[:500],
                parse_int(row.get('DueDateDays', '')),
                parse_int(row.get('DueDateSettings_Id', '')),
                row.get('ChargeCategory', '').strip()[:100],
                row.get('IntroText', '').strip()[:1000] if row.get('IntroText') else '',
                row.get('RevenueGLCode', '').strip()[:50],
                row.get('ARGLCode', '').strip()[:50],
                row.get('SalesTaxGLCode', '').strip()[:50]
            )
            seasonal_prices.append(seasonal_price_data)
        except Exception as e:
            logger.warning(f"Error parsing seasonal price row: {e}")
            continue
    
    return seasonal_prices


def parse_transient_prices_data(csv_content):
    """
    Parse TransientPrices CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing transient price data for database insertion
    """
    # Helper function to safely parse datetime values with MM/DD/YYYY format support
    def safe_datetime(value):
        """Parse datetime with multiple format support including MM/DD/YYYY for Molo exports."""
        if not value or value in ('', 'NULL', None):
            return None
        try:
            # Try multiple datetime formats, including MM/DD/YYYY for Molo exports
            for fmt in [
                '%m/%d/%Y %H:%M:%S',  # MM/DD/YYYY HH:MM:SS (Molo format)
                '%Y-%m-%d %H:%M:%S',  # YYYY-MM-DD HH:MM:SS
                '%Y-%m-%dT%H:%M:%S',  # ISO format
                '%m/%d/%Y',           # MM/DD/YYYY date only
                '%Y-%m-%d'            # YYYY-MM-DD date only
            ]:
                try:
                    return datetime.strptime(str(value).strip(), fmt)
                except ValueError:
                    continue
            return None
        except:
            return None
    
    transient_prices = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            transient_price_data = (
                parse_int(row.get('Id', '')),                                           # 1
                safe_datetime(row.get('StartDate')),                                    # 2
                safe_datetime(row.get('EndDate')),                                      # 3
                parse_float(row.get('Fee', '0')),                                       # 4
                row.get('RateName', '').strip()[:1000] if row.get('RateName') else '',  # 5
                parse_int(row.get('TransientChargeMethodId', '')),                      # 6
                parse_int(row.get('MarinaLocationId', '')),                             # 7
                parse_boolean(row.get('Taxable', '')),                                  # 8
                parse_float(row.get('Tax', '0')),                                       # 9
                row.get('RateDetails', '').strip()[:1000] if row.get('RateDetails') else '',  # 10
                row.get('RateShortName', '').strip()[:1000] if row.get('RateShortName') else '',  # 11
                row.get('OnlinePaymentPlaceholder', '').strip()[:1000] if row.get('OnlinePaymentPlaceholder') else '',  # 12
                row.get('XeroItemCode', '').strip()[:1000] if row.get('XeroItemCode') else '',  # 13
                row.get('XeroID', '').strip()[:1000] if row.get('XeroID') else '',      # 14
                parse_int(row.get('FirstTrackingCategory', '')),                        # 15
                parse_int(row.get('SecondTrackingCategory', '')),                       # 16
                parse_int(row.get('TransientInvoicingMethod_Id', '')),                  # 17
                parse_int(row.get('SV_InventoryCategory_Id', '')),                      # 18
                parse_int(row.get('SV_InventorySubCategory_Id', '')),                   # 19
                safe_datetime(row.get('CreationDateTime')),                             # 20
                row.get('AspNetUser_Id', '').strip()[:256] if row.get('AspNetUser_Id') else '',  # 21
                row.get('CheckInTerms', '').strip()[:1000] if row.get('CheckInTerms') else '',  # 22
                row.get('CheckOutTerms', '').strip()[:1000] if row.get('CheckOutTerms') else '',  # 23
                row.get('OnlinePaymentCompletion', '').strip()[:1000] if row.get('OnlinePaymentCompletion') else '',  # 24
                parse_int(row.get('DueDateDays', '')),                                  # 25
                parse_int(row.get('DueDateSettings_Id', '')),                           # 26
                row.get('HourlyCalculation', '').strip()[:1000] if row.get('HourlyCalculation') else '',  # 27
                parse_int(row.get('RoundMinutes', '')),                                 # 28
                parse_int(row.get('MinimumHours', '')),                                 # 29
                parse_int(row.get('NumHoursBlock', '')),                                # 30
                row.get('ChargeCategory', '').strip()[:1000] if row.get('ChargeCategory') else '',  # 31
                row.get('IntroText', '').strip()[:1000] if row.get('IntroText') else '',  # 32
                row.get('RevenueGLCode', '').strip()[:1000] if row.get('RevenueGLCode') else '',  # 33
                row.get('ARGLCode', '').strip()[:1000] if row.get('ARGLCode') else '',  # 34
                row.get('SalesTaxGLCode', '').strip()[:1000] if row.get('SalesTaxGLCode') else '',  # 35
                safe_datetime(row.get('DeletionDatetime')),                             # 36
                row.get('DeletionAspNetUser_Id', '').strip()[:256] if row.get('DeletionAspNetUser_Id') else '',  # 37
                parse_int(row.get('RecordStatus_Id', '')),                              # 38
                parse_int(row.get('RecurringInvoiceOptions_Id', '')),                   # 39
                parse_boolean(row.get('Recurring', '')),                                # 40
                row.get('TrackingCode', '').strip()[:1000] if row.get('TrackingCode') else '',  # 41
                row.get('AlternateReservationName', '').strip()[:1000] if row.get('AlternateReservationName') else '',  # 42
                parse_boolean(row.get('ResourceRate', '')),                             # 43
                parse_int(row.get('QuantityCap', '')),                                  # 44
                parse_boolean(row.get('AllowPostingToNonIncomeAccounts', ''))           # 45
            )
            transient_prices.append(transient_price_data)
        except Exception as e:
            logger.warning(f"Error parsing transient price row: {e}")
            continue
    
    return transient_prices


def parse_record_status_data(csv_content):
    """
    Parse RecordStatusSet CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing record status data for database insertion
    """
    record_statuses = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            record_status_data = (
                row.get('Id', '').strip(),
                row.get('Name', '').strip()[:255]
            )
            record_statuses.append(record_status_data)
        except Exception as e:
            logger.warning(f"Error parsing record status row: {e}")
            continue
    
    return record_statuses


def parse_boat_types_data(csv_content):
    """
    Parse BoatTypes CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing boat type data for database insertion
    """
    boat_types = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            boat_type_data = (
                row.get('Id', '').strip(),
                row.get('Name', '').strip()[:255]
            )
            boat_types.append(boat_type_data)
        except Exception as e:
            logger.warning(f"Error parsing boat type row: {e}")
            continue
    
    return boat_types


def parse_power_needs_data(csv_content):
    """
    Parse PowerNeeds CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing power need data for database insertion
    """
    power_needs = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            power_need_data = (
                row.get('Id', '').strip(),
                row.get('Name', '').strip()[:255]
            )
            power_needs.append(power_need_data)
        except Exception as e:
            logger.warning(f"Error parsing power need row: {e}")
            continue
    
    return power_needs


def parse_reservation_status_data(csv_content):
    """
    Parse ReservationStatus CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing reservation status data for database insertion
    """
    reservation_statuses = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            reservation_status_data = (
                row.get('Id', '').strip(),
                row.get('Name', '').strip()[:255]
            )
            reservation_statuses.append(reservation_status_data)
        except Exception as e:
            logger.warning(f"Error parsing reservation status row: {e}")
            continue
    
    return reservation_statuses


def parse_reservation_types_data(csv_content):
    """
    Parse ReservationTypes CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing reservation type data for database insertion
    """
    reservation_types = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            reservation_type_data = (
                row.get('Id', '').strip(),
                row.get('Name', '').strip()[:255]
            )
            reservation_types.append(reservation_type_data)
        except Exception as e:
            logger.warning(f"Error parsing reservation type row: {e}")
            continue
    
    return reservation_types


def parse_contact_types_data(csv_content):
    """
    Parse ContactTypes CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing contact type data for database insertion
    """
    contact_types = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            contact_type_data = (
                row.get('Id', '').strip(),
                row.get('Name', '').strip()[:255]
            )
            contact_types.append(contact_type_data)
        except Exception as e:
            logger.warning(f"Error parsing contact type row: {e}")
            continue
    
    return contact_types


def parse_invoice_status_data(csv_content):
    """
    Parse InvoiceStatusSet CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing invoice status data for database insertion
    """
    invoice_statuses = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            invoice_status_data = (
                row.get('Id', '').strip(),
                row.get('Name', '').strip()[:255]
            )
            invoice_statuses.append(invoice_status_data)
        except Exception as e:
            logger.warning(f"Error parsing invoice status row: {e}")
            continue
    
    return invoice_statuses


def parse_invoice_types_data(csv_content):
    """
    Parse InvoiceTypeSet CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing invoice type data for database insertion
    """
    invoice_types = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            invoice_type_data = (
                row.get('Id', '').strip(),
                row.get('Name', '').strip()[:255]
            )
            invoice_types.append(invoice_type_data)
        except Exception as e:
            logger.warning(f"Error parsing invoice type row: {e}")
            continue
    
    return invoice_types


def parse_transaction_types_data(csv_content):
    """
    Parse TransactionTypeSet CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing transaction type data for database insertion
    """
    transaction_types = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            transaction_type_data = (
                row.get('Id', '').strip(),
                row.get('Name', '').strip()[:255]
            )
            transaction_types.append(transaction_type_data)
        except Exception as e:
            logger.warning(f"Error parsing transaction type row: {e}")
            continue
    
    return transaction_types


def parse_transaction_methods_data(csv_content):
    """
    Parse TransactionMethodSet CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing transaction method data for database insertion
    """
    transaction_methods = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            transaction_method_data = (
                row.get('Id', '').strip(),
                row.get('Name', '').strip()[:255]
            )
            transaction_methods.append(transaction_method_data)
        except Exception as e:
            logger.warning(f"Error parsing transaction method row: {e}")
            continue
    
    return transaction_methods


def parse_insurance_data(csv_content):
    """
    Parse InsuranceSet CSV content into database-ready format.
    Maps to STG_MOLO_INSURANCE table structure.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing insurance data for database insertion
    """
    insurance_records = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            insurance_data = (
                row.get('Id', '').strip()[:10] if row.get('Id', '').strip() else None,
                row.get('Provider', '').strip()[:1000] if row.get('Provider', '').strip() else None,
                row.get('ListedIndividual', '').strip()[:1000] if row.get('ListedIndividual', '').strip() else None,
                row.get('AccountNumber', '').strip()[:1000] if row.get('AccountNumber', '').strip() else None,
                row.get('PolicyNumber', '').strip()[:1000] if row.get('PolicyNumber', '').strip() else None,
                row.get('GroupNumber', '').strip()[:1000] if row.get('GroupNumber', '').strip() else None,
                float(row.get('LiabilityMaximum')) if row.get('LiabilityMaximum', '').strip() else None,
                parse_datetime(row.get('EffectiveDate', '')),
                parse_datetime(row.get('ExpirationDate', '')),
                row.get('Notes', '').strip()[:1000] if row.get('Notes', '').strip() else None,
                row.get('CreationUser', '').strip()[:1000] if row.get('CreationUser', '').strip() else None,
                parse_datetime(row.get('CreationDateTime', '')),
                row.get('LastEditUser', '').strip()[:1000] if row.get('LastEditUser', '').strip() else None,
                parse_datetime(row.get('LastEditDateTime', '')),
                row.get('DeleteUser', '').strip()[:1000] if row.get('DeleteUser', '').strip() else None,
                parse_datetime(row.get('DeleteDateTime', '')),
                row.get('InsuranceStatus_Id', '').strip()[:10] if row.get('InsuranceStatus_Id', '').strip() else None,
                row.get('Boat_Id', '').strip()[:10] if row.get('Boat_Id', '').strip() else None,
                row.get('HashID', '').strip()[:1000] if row.get('HashID', '').strip() else None
            )
            insurance_records.append(insurance_data)
        except Exception as e:
            logger.warning(f"Error parsing insurance row: {e}")
            continue
    
    return insurance_records


def parse_equipment_data(csv_content):
    """
    Parse Equipment CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing equipment data for database insertion
    """
    equipment_records = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            equipment_data = (
                row.get('Id', '').strip()[:10],
                row.get('Name', '').strip()[:100],
                row.get('Description', '').strip()[:500],
                row.get('EquipmentTypeId', '').strip()[:10],
                row.get('FuelTypeId', '').strip()[:10],
                row.get('Model', '').strip()[:50],
                row.get('Manufacturer', '').strip()[:50],
                int(row.get('YearBuilt', 0)) if row.get('YearBuilt') else None,
                row.get('SerialNumber', '').strip()[:50],
                row.get('Location', '').strip()[:100]
            )
            equipment_records.append(equipment_data)
        except Exception as e:
            logger.warning(f"Error parsing equipment row: {e}")
            continue
    
    return equipment_records


def parse_account_status_data(csv_content):
    """
    Parse AccountStatusSet CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing account status data for database insertion
    """
    account_statuses = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            status_data = (
                row.get('Id', '').strip()[:10],
                row.get('Name', '').strip()[:100]
            )
            account_statuses.append(status_data)
        except Exception as e:
            logger.warning(f"Error parsing account status row: {e}")
            continue
    
    return account_statuses


def parse_contact_auto_charge_data(csv_content):
    """
    Parse ContactAutoChargeSet CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing contact auto charge data for database insertion
    """
    auto_charges = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            charge_data = (
                row.get('Id', '').strip()[:10],
                row.get('Name', '').strip()[:100]
            )
            auto_charges.append(charge_data)
        except Exception as e:
            logger.warning(f"Error parsing contact auto charge row: {e}")
            continue
    
    return auto_charges


def parse_statements_preference_data(csv_content):
    """
    Parse StatementsPreferenceSet CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing statements preference data for database insertion
    """
    preferences = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            preference_data = (
                row.get('Id', '').strip()[:10],
                row.get('Name', '').strip()[:100]
            )
            preferences.append(preference_data)
        except Exception as e:
            logger.warning(f"Error parsing statements preference row: {e}")
            continue
    
    return preferences


def parse_invoice_item_types_data(csv_content):
    """
    Parse InvoiceItemTypes CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing invoice item types data for database insertion
    """
    item_types = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            type_data = (
                row.get('Id', '').strip()[:10],
                row.get('Name', '').strip()[:100]
            )
            item_types.append(type_data)
        except Exception as e:
            logger.warning(f"Error parsing invoice item type row: {e}")
            continue
    
    return item_types


def parse_payment_methods_data(csv_content):
    """
    Parse PaymentMethods CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing payment methods data for database insertion
    """
    payment_methods = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            method_data = (
                row.get('Id', '').strip()[:10],
                row.get('Name', '').strip()[:100]
            )
            payment_methods.append(method_data)
        except Exception as e:
            logger.warning(f"Error parsing payment method row: {e}")
            continue
    
    return payment_methods


def parse_seasonal_charge_methods_data(csv_content):
    """
    Parse SeasonalChargeMethods CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing seasonal charge methods data for database insertion
    """
    charge_methods = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            method_data = (
                row.get('Id', '').strip()[:10],
                row.get('Name', '').strip()[:100]
            )
            charge_methods.append(method_data)
        except Exception as e:
            logger.warning(f"Error parsing seasonal charge method row: {e}")
            continue
    
    return charge_methods


def parse_seasonal_invoicing_methods_data(csv_content):
    """
    Parse SeasonalInvoicingMethods CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing seasonal invoicing methods data for database insertion
    """
    invoicing_methods = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            method_data = (
                row.get('Id', '').strip()[:10],
                row.get('Name', '').strip()[:100]
            )
            invoicing_methods.append(method_data)
        except Exception as e:
            logger.warning(f"Error parsing seasonal invoicing method row: {e}")
            continue
    
    return invoicing_methods


def parse_transient_charge_methods_data(csv_content):
    """
    Parse TransientChargeMethods CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing transient charge methods data for database insertion
    """
    charge_methods = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            method_data = (
                row.get('Id', '').strip()[:10],
                row.get('Name', '').strip()[:100]
            )
            charge_methods.append(method_data)
        except Exception as e:
            logger.warning(f"Error parsing transient charge method row: {e}")
            continue
    
    return charge_methods


def parse_transient_invoicing_methods_data(csv_content):
    """
    Parse TransientInvoicingMethods CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing transient invoicing methods data for database insertion
    """
    invoicing_methods = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            method_data = (
                row.get('Id', '').strip()[:10],
                row.get('Name', '').strip()[:100]
            )
            invoicing_methods.append(method_data)
        except Exception as e:
            logger.warning(f"Error parsing transient invoicing method row: {e}")
            continue
    
    return invoicing_methods


def parse_recurring_invoice_options_data(csv_content):
    """
    Parse RecurringInvoiceOptions CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing recurring invoice options data for database insertion
    """
    options = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            option_data = (
                row.get('Id', '').strip()[:10],
                row.get('Name', '').strip()[:100]
            )
            options.append(option_data)
        except Exception as e:
            logger.warning(f"Error parsing recurring invoice option row: {e}")
            continue
    
    return options


def parse_due_date_settings_data(csv_content):
    """
    Parse DueDateSettings CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing due date settings data for database insertion
    """
    settings = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            setting_data = (
                row.get('Id', '').strip()[:10],
                row.get('Name', '').strip()[:100]
            )
            settings.append(setting_data)
        except Exception as e:
            logger.warning(f"Error parsing due date setting row: {e}")
            continue
    
    return settings


def parse_item_charge_methods_data(csv_content):
    """
    Parse ItemChargeMethods CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing item charge methods data for database insertion
    """
    charge_methods = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            method_data = (
                row.get('Id', '').strip()[:10],
                row.get('Name', '').strip()[:100]
            )
            charge_methods.append(method_data)
        except Exception as e:
            logger.warning(f"Error parsing item charge method row: {e}")
            continue
    
    return charge_methods


def parse_insurance_status_data(csv_content):
    """
    Parse InsuranceStatusSet CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing insurance status data for database insertion
    """
    statuses = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            status_data = (
                row.get('Id', '').strip()[:10],
                row.get('Name', '').strip()[:100]
            )
            statuses.append(status_data)
        except Exception as e:
            logger.warning(f"Error parsing insurance status row: {e}")
            continue
    
    return statuses


def parse_equipment_types_data(csv_content):
    """
    Parse EquipmentTypes CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing equipment types data for database insertion
    """
    types = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            type_data = (
                row.get('Id', '').strip()[:10],
                row.get('Name', '').strip()[:100]
            )
            types.append(type_data)
        except Exception as e:
            logger.warning(f"Error parsing equipment type row: {e}")
            continue
    
    return types


def parse_equipment_fuel_types_data(csv_content):
    """
    Parse EquipmentFuelTypes CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing equipment fuel types data for database insertion
    """
    fuel_types = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            fuel_type_data = (
                row.get('Id', '').strip()[:10],
                row.get('Name', '').strip()[:100]
            )
            fuel_types.append(fuel_type_data)
        except Exception as e:
            logger.warning(f"Error parsing equipment fuel type row: {e}")
            continue
    
    return fuel_types


def parse_vessel_engine_class_data(csv_content):
    """
    Parse VesselEngineClass CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing vessel engine class data for database insertion
    """
    engine_classes = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            class_data = (
                row.get('Id', '').strip()[:10],
                row.get('Name', '').strip()[:100]
            )
            engine_classes.append(class_data)
        except Exception as e:
            logger.warning(f"Error parsing vessel engine class row: {e}")
            continue
    
    return engine_classes


def parse_cities_data(csv_content):
    """
    Parse Cities CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing cities data for database insertion
    """
    cities = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            city_data = (
                row.get('Id', '').strip()[:10],
                row.get('Name', '').strip()[:100]
            )
            cities.append(city_data)
        except Exception as e:
            logger.warning(f"Error parsing city row: {e}")
            continue
    
    return cities


def parse_countries_data(csv_content):
    """
    Parse Countries CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing countries data for database insertion
    """
    countries = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            country_data = (
                row.get('Id', '').strip()[:10],
                row.get('Name', '').strip()[:100]
            )
            countries.append(country_data)
        except Exception as e:
            logger.warning(f"Error parsing country row: {e}")
            continue
    
    return countries


def parse_currencies_data(csv_content):
    """
    Parse Currencies CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing currencies data for database insertion
    """
    currencies = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            currency_data = (
                row.get('Id', '').strip()[:10],
                row.get('Name', '').strip()[:100],
                row.get('Code', '').strip()[:10] if row.get('Code') else None,
                row.get('Symbol', '').strip()[:10] if row.get('Symbol') else None
            )
            currencies.append(currency_data)
        except Exception as e:
            logger.warning(f"Error parsing currency row: {e}")
            continue
    
    return currencies


def parse_phone_types_data(csv_content):
    """
    Parse PhoneTypes CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing phone types data for database insertion
    """
    phone_types = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            type_data = (
                row.get('Id', '').strip()[:10],
                row.get('Name', '').strip()[:100]
            )
            phone_types.append(type_data)
        except Exception as e:
            logger.warning(f"Error parsing phone type row: {e}")
            continue
    
    return phone_types


def parse_address_types_data(csv_content):
    """
    Parse AddressTypes CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing address types data for database insertion
    """
    address_types = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            type_data = (
                row.get('Id', '').strip()[:10],
                row.get('Name', '').strip()[:100]
            )
            address_types.append(type_data)
        except Exception as e:
            logger.warning(f"Error parsing address type row: {e}")
            continue
    
    return address_types


def parse_installments_payment_methods_data(csv_content):
    """
    Parse InstallmentsPaymentMethods CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing installments payment methods data for database insertion
    """
    payment_methods = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            method_data = (
                row.get('Id', '').strip()[:10],
                row.get('Name', '').strip()[:100]
            )
            payment_methods.append(method_data)
        except Exception as e:
            logger.warning(f"Error parsing installments payment method row: {e}")
            continue
    
    return payment_methods


def parse_payments_provider_data(csv_content):
    """
    Parse PaymentsProvider CSV content into database-ready format.
    
    Args:
        csv_content (str): Raw CSV content as string
        
    Returns:
        list: List of tuples containing payments provider data for database insertion
    """
    providers = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            provider_data = (
                row.get('Id', '').strip()[:10],
                row.get('Name', '').strip()[:100]
            )
            providers.append(provider_data)
        except Exception as e:
            logger.warning(f"Error parsing payments provider row: {e}")
            continue
    
    return providers


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def parse_datetime(datetime_str):
    """
    Parse datetime string using multiple format attempts.
    
    Args:
        datetime_str (str): Date/time string in various formats
        
    Returns:
        datetime: Parsed datetime object, or None if parsing fails
    """
    if not datetime_str:
        return None
    
    datetime_formats = [
        '%m/%d/%Y %H:%M:%S',
        '%Y-%m-%d %H:%M:%S',
        '%m/%d/%Y %H:%M',
        '%Y-%m-%d %H:%M'
    ]
    
    for fmt in datetime_formats:
        try:
            return datetime.strptime(datetime_str, fmt)
        except ValueError:
            continue
    
    logger.warning(f"Could not parse datetime: {datetime_str}")
    return None


def parse_date(date_str):
    """
    Parse date string using multiple format attempts.
    
    Args:
        date_str (str): Date string in various formats
        
    Returns:
        datetime: Parsed datetime object, or None if parsing fails
    """
    if not date_str:
        return None
    
    date_formats = ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d', '%m-%d-%Y']
    
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    logger.warning(f"Could not parse date: {date_str}")
    return None


def parse_float(value_str):
    """
    Parse float value with error handling.
    
    Args:
        value_str (str): String representation of a float value
        
    Returns:
        float: Parsed float value, or 0.0 if parsing fails
    """
    if not value_str:
        return 0.0
    
    try:
        return float(value_str)
    except ValueError:
        logger.warning(f"Could not parse float: {value_str}")
        return 0.0


def parse_int(value_str):
    """
    Parse integer value with error handling.
    
    Args:
        value_str (str): String representation of an integer value
        
    Returns:
        int or None: Parsed integer value, or None if empty/invalid
    """
    if not value_str or not value_str.strip():
        return None
    
    try:
        return int(value_str)
    except ValueError:
        logger.warning(f"Could not parse int: {value_str}")
        return None


def parse_boolean(value_str):
    """
    Parse boolean value from CSV (TRUE/FALSE strings) to database format (1/0).
    
    Args:
        value_str (str): String representation of a boolean ("TRUE", "FALSE", "1", "0", etc.)
        
    Returns:
        int or None: 1 for true, 0 for false, None if empty/invalid
    """
    if not value_str or not value_str.strip():
        return None
    
    value_upper = value_str.strip().upper()
    
    if value_upper in ('TRUE', 'T', '1', 'YES', 'Y'):
        return 1
    elif value_upper in ('FALSE', 'F', '0', 'NO', 'N'):
        return 0
    else:
        logger.warning(f"Could not parse boolean: {value_str}")
        return None


# =============================================================================
# S3 OPERATIONS
# =============================================================================

def find_latest_zip_in_s3(s3_client, bucket, prefix=None):
    """
    Find the most recently modified .zip file in an S3 bucket.
    
    Args:
        s3_client: Boto3 S3 client instance
        bucket (str): S3 bucket name
        prefix (str, optional): S3 key prefix to search within
        
    Returns:
        str: Key of the latest .zip file, or None if none found
    """
    if prefix:
        logger.info(f"Searching for latest .zip file in s3://{bucket}/{prefix}...")
    else:
        logger.info(f"Searching for latest .zip file in entire bucket s3://{bucket}...")
    
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        if prefix:
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        else:
            pages = paginator.paginate(Bucket=bucket)
        
        latest_zip_file = None
        total_files_checked = 0
        zip_files_found = 0
        
        for page in pages:
            if "Contents" in page:
                for obj in page['Contents']:
                    total_files_checked += 1
                    if obj['Key'].lower().endswith('.zip'):
                        zip_files_found += 1
                        if (latest_zip_file is None or 
                            obj['LastModified'] > latest_zip_file['LastModified']):
                            latest_zip_file = obj
        
        logger.info(
            f"Checked {total_files_checked} total files, "
            f"found {zip_files_found} .zip files"
        )
        
        if latest_zip_file:
            logger.info(
                f"Found latest file: {latest_zip_file['Key']} "
                f"(Last Modified: {latest_zip_file['LastModified']})"
            )
            return latest_zip_file['Key']
        else:
            logger.warning(f"No .zip files found in bucket '{bucket}'.")
            return None
            
    except ClientError as e:
        logger.exception(f"An error occurred accessing S3: {e}")
        return None


# =============================================================================
# VALIDATION HELPER FUNCTION
# =============================================================================

def perform_table_validation(
    validator,
    csv_content,
    table_name,
    staging_table,
    dw_table,
    id_column,
    key_columns,
    validate_fields,
    validate_merge_changes,
    validation_sample_size,
    record_count=None
):
    """
    Perform field-level validation on imported table data.
    
    Args:
        validator: DataValidator instance
        csv_content: Raw CSV content
        table_name: Display name for logging
        staging_table: Staging table name (STG_MOLO_*)
        dw_table: Data warehouse table name (DW_MOLO_*)
        id_column: Primary key column name
        key_columns: List of important columns to validate
        validate_fields: Enable CSV vs DB validation
        validate_merge_changes: Enable staging vs DW validation
        validation_sample_size: Number of records to sample
        record_count: Number of records inserted (optional)
    """
    if not validator:
        return
    
    validation_issues = []
    
    # Log staging insert summary
    if record_count:
        try:
            # Get staging table count
            staging_count = validator._get_row_count(staging_table)
            logger.info(
                f"   📥 Staging: {record_count} records inserted "
                f"({staging_count} total in {staging_table})"
            )
        except Exception as e:
            logger.debug(f"Could not get staging count for {table_name}: {e}")
    
    # Validate CSV vs Database field values
    if validate_fields:
        try:
            passed, issues = validator.validate_field_values(
                csv_content=csv_content,
                dw_table=dw_table,
                id_column=id_column,
                columns_to_check=key_columns,
                sample_size=validation_sample_size
            )
            if not passed:
                logger.warning(f"⚠️  Field validation issues in {table_name}:")
                for issue in issues[:5]:  # Show first 5 issues
                    logger.warning(f"    {issue}")
                if len(issues) > 5:
                    logger.warning(f"    ... and {len(issues) - 5} more issues")
                validation_issues.extend(issues)
        except Exception as e:
            logger.warning(f"Field validation failed for {table_name}: {e}")
    
    # Validate staging vs DW after merge
    if validate_merge_changes:
        try:
            passed, issues, stats = validator.validate_field_changes(
                staging_table=staging_table,
                dw_table=dw_table,
                id_column=id_column,
                columns_to_check=key_columns,
                sample_size=validation_sample_size
            )
            if not passed:
                logger.warning(
                    f"⚠️  Merge validation for {table_name}: "
                    f"{stats['records_with_changes']} records changed, "
                    f"{stats['fields_changed']} fields modified"
                )
                for issue in issues[:3]:  # Show first 3 issues
                    logger.warning(f"    {issue}")
                if len(issues) > 3:
                    logger.warning(f"    ... and {len(issues) - 3} more issues")
                validation_issues.extend(issues)
        except Exception as e:
            logger.warning(f"Merge validation failed for {table_name}: {e}")
    
    return len(validation_issues) == 0


# =============================================================================
# MAIN PROCESSING FUNCTION
# =============================================================================

def read_s3_zip_and_insert_to_db(
    bucket,
    s3_prefix,
    region,
    db_user,
    db_password,
    db_dsn,
    aws_access_key_id=None,
    aws_secret_access_key=None,
    validate_fields=False,
    validate_merge_changes=False,
    validation_sample_size=10
):
    """
    Main processing function: Download latest ZIP from S3, extract target CSVs,
    and synchronize data with Oracle database using INSERT operations into staging tables.
    
    This function:
    1. Finds and downloads the latest ZIP file from S3
    2. Extracts only the target CSV files (marina-related data)
    3. Parses each CSV file into database-ready format
    4. Uses INSERT operations into staging tables to synchronize data without duplication
    5. Optionally validates field-level data integrity (if enabled)
    
    Args:
        bucket (str): S3 bucket name
        s3_prefix (str): S3 prefix (unused - searches entire bucket)
        region (str): AWS region
        db_user (str): Oracle database username
        db_password (str): Oracle database password
        db_dsn (str): Oracle database DSN
        aws_access_key_id (str, optional): AWS access key
        aws_secret_access_key (str, optional): AWS secret key
        validate_fields (bool): Enable CSV vs DB field validation
        validate_merge_changes (bool): Enable staging vs DW merge validation
        validation_sample_size (int): Number of records to sample for validation
    """
    latest_zip_key = None
    
    try:
        # Initialize S3 client with appropriate credentials
        if aws_access_key_id and aws_secret_access_key:
            logger.info("Using AWS credentials provided from OCI Vault.")
            s3_client = boto3.client(
                's3',
                region_name=region,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            )
        else:
            logger.info(
                "Using Boto3's default credential discovery "
                "(~/.aws/credentials or IAM roles)."
            )
            s3_client = boto3.client('s3', region_name=region)

        # Find the latest ZIP file in the bucket
        latest_zip_key = find_latest_zip_in_s3(s3_client, bucket)
        if not latest_zip_key:
            return

        # Download the ZIP file from S3
        logger.info(f"Connecting to S3 in region: {region}...")
        logger.info(
            f"Attempting to download '{latest_zip_key}' from bucket '{bucket}'..."
        )

        response = s3_client.get_object(Bucket=bucket, Key=latest_zip_key)
        zip_content = response['Body'].read()
        logger.info(
            f"Successfully downloaded '{latest_zip_key}' from S3 bucket '{bucket}'."
        )

        # Extract target CSV files from the ZIP archive
        extracted_csv_data = {}
        
        with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
            logger.info(f"Archive contents: {z.namelist()}")
            
            for filename in z.namelist():
                if filename.lower().endswith('.csv'):
                    # Extract base filename without extension for matching
                    base_name = os.path.splitext(os.path.basename(filename))[0]
                    
                    # Check if this CSV file is one of our targets
                    if base_name in TARGET_CSV_FILES:
                        logger.info(
                            f"Found target CSV file '{filename}' in the zip archive."
                        )
                        csv_content = z.read(filename).decode('utf-8')
                        extracted_csv_data[base_name] = csv_content
                    else:
                        logger.info(
                            f"Skipping CSV file '{filename}' (not in target list)."
                        )

        # Validate that we found the expected CSV files
        if not extracted_csv_data:
            logger.warning(
                "None of the target CSV files were found in the zip archive."
            )
            logger.info(f"Looking for: {TARGET_CSV_FILES}")
            return

        logger.info(
            f"Successfully extracted {len(extracted_csv_data)} target CSV files: "
            f"{list(extracted_csv_data.keys())}"
        )
        
        # Connect to Oracle database for all operations
        db = OracleConnector(db_user, db_password, db_dsn)
        
        # Initialize validator if validation is enabled
        validator = None
        if (validate_fields or validate_merge_changes) and VALIDATION_AVAILABLE:
            validator = DataValidator(db, logger)
            logger.info(f"✅ DataValidator initialized (sample size: {validation_sample_size})")
        elif (validate_fields or validate_merge_changes) and not VALIDATION_AVAILABLE:
            logger.warning("⚠️  Validation requested but DataValidator not available")
        
        # STEP 1: Truncate all staging tables before loading new data
        logger.info("\n" + "="*70)
        logger.info("STEP 1: TRUNCATING STAGING TABLES")
        logger.info("="*70)
        db.truncate_staging_tables()
        logger.info("✅ All staging tables truncated successfully\n")
        
        # STEP 2: Process each CSV file and insert into staging tables
        logger.info("="*70)
        logger.info("STEP 2: LOADING DATA INTO STAGING TABLES")
        logger.info("="*70)
        
        # Initialize processing counters
        processed_count = 0
        skipped_count = 0
        error_count = 0
        table_record_counts = {}  # Track records per table like Stellar does
        
        for csv_name, csv_content in extracted_csv_data.items():
            logger.info(f"\n--- Processing {csv_name}.csv ---")
            
            try:
                # Route to appropriate parser and MERGE function
                if csv_name == 'MarinaLocations':
                    parsed_data = parse_marina_locations_data(csv_content)
                    db.insert_marina_locations(parsed_data)
                    table_record_counts['MARINA_LOCATIONS'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} marina location records")
                    processed_count += 1
                    
                elif csv_name == 'Piers':
                    parsed_data = parse_piers_data(csv_content)
                    db.insert_piers(parsed_data)
                    table_record_counts['PIERS'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} pier records")
                    processed_count += 1
                    
                elif csv_name == 'SlipTypes':
                    parsed_data = parse_slip_types_data(csv_content)
                    db.insert_slip_types(parsed_data)
                    table_record_counts['SLIP_TYPES'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} slip type records")
                    processed_count += 1
                    
                elif csv_name == 'Slips':
                    parsed_data = parse_slips_data(csv_content)
                    db.insert_slips(parsed_data)
                    table_record_counts['SLIPS'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} slip records")
                    processed_count += 1
                    
                elif csv_name == 'Reservations':
                    parsed_data = parse_reservations_data(csv_content)
                    db.insert_reservations(parsed_data)
                    table_record_counts['RESERVATIONS'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} reservation records")
                    processed_count += 1
                    
                elif csv_name == 'Companies':
                    parsed_data = parse_companies_data(csv_content)
                    db.insert_companies(parsed_data)
                    table_record_counts['COMPANIES'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} company records")
                    processed_count += 1
                    
                elif csv_name == 'Contacts':
                    parsed_data = parse_contacts_data(csv_content)
                    db.insert_contacts(parsed_data)
                    table_record_counts['CONTACTS'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} contact records")
                    processed_count += 1
                    
                elif csv_name == 'Boats':
                    parsed_data = parse_boats_data(csv_content)
                    db.insert_boats(parsed_data)
                    table_record_counts['BOATS'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} boat records")
                    
                    # Validate boats data if enabled
                    if validator:
                        perform_table_validation(
                            validator, csv_content, 'BOATS',
                            'STG_MOLO_BOATS', 'DW_MOLO_BOATS', 'BOAT_ID',
                            ['BOAT_NAME', 'LENGTH', 'WIDTH', 'BOAT_TYPE_ID'],
                            validate_fields, validate_merge_changes,
                            validation_sample_size, len(parsed_data)
                        )
                    
                    processed_count += 1
                elif csv_name == 'Accounts':
                    parsed_data = parse_accounts_data(csv_content)
                    db.insert_accounts(parsed_data)
                    table_record_counts['ACCOUNTS'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} account records")
                    
                    processed_count += 1
                elif csv_name == 'InvoiceSet':
                    parsed_data = parse_invoices_data(csv_content)
                    db.insert_invoices(parsed_data)
                    table_record_counts['INVOICES'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} invoice records")
                    
                    # Validate invoices data
                    if validator:
                        perform_table_validation(
                            validator, csv_content, 'INVOICES',
                            'STG_MOLO_INVOICES', 'DW_MOLO_INVOICES',
                            'INVOICE_ID',
                            ['INVOICE_NUMBER', 'TOTAL_AMOUNT', 'INVOICE_DATE',
                             'INVOICE_STATUS_ID'],
                            validate_fields, validate_merge_changes,
                            validation_sample_size, len(parsed_data)
                        )
                    
                    processed_count += 1
                elif csv_name == 'InvoiceItemSet':
                    parsed_data = parse_invoice_items_data(csv_content)
                    db.insert_invoice_items(parsed_data)
                    table_record_counts['INVOICE_ITEMS'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} invoice item records")
                    
                    processed_count += 1
                elif csv_name == 'Transactions':
                    parsed_data = parse_transactions_data(csv_content)
                    db.insert_transactions(parsed_data)
                    table_record_counts['TRANSACTIONS'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} transaction records")
                    
                    processed_count += 1
                elif csv_name == 'ItemMasters':
                    parsed_data = parse_item_masters_data(csv_content)
                    db.insert_item_masters(parsed_data)
                    table_record_counts['ITEM_MASTERS'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} item master records")
                    
                    # Validate item masters data (especially datetime fields)
                    if validator:
                        perform_table_validation(
                            validator, csv_content, 'ITEM_MASTERS',
                            'STG_MOLO_ITEM_MASTERS', 'DW_MOLO_ITEM_MASTERS',
                            'ITEM_MASTER_ID',
                            ['DESCRIPTION', 'ITEM_TYPE', 'UNIT_PRICE',
                             'CREATION_DATE_TIME'],
                            validate_fields, validate_merge_changes,
                            validation_sample_size, len(parsed_data)
                        )
                    
                    processed_count += 1
                elif csv_name == 'SeasonalPrices':
                    parsed_data = parse_seasonal_prices_data(csv_content)
                    db.insert_seasonal_prices(parsed_data)
                    table_record_counts['SEASONAL_PRICES'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} seasonal price records")
                    
                    processed_count += 1
                elif csv_name == 'TransientPrices':
                    parsed_data = parse_transient_prices_data(csv_content)
                    db.insert_transient_prices(parsed_data)
                    table_record_counts['TRANSIENT_PRICES'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} transient price records")
                    
                    processed_count += 1
                elif csv_name == 'RecordStatusSet':
                    parsed_data = parse_record_status_data(csv_content)
                    db.insert_record_status(parsed_data)
                    table_record_counts['RECORD_STATUS'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} record status records")
                    
                    processed_count += 1
                elif csv_name == 'BoatTypes':
                    parsed_data = parse_boat_types_data(csv_content)
                    db.insert_boat_types(parsed_data)
                    table_record_counts['BOAT_TYPES'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} boat type records")
                    
                    processed_count += 1
                elif csv_name == 'PowerNeeds':
                    parsed_data = parse_power_needs_data(csv_content)
                    db.insert_power_needs(parsed_data)
                    table_record_counts['POWER_NEEDS'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} power need records")
                    
                    processed_count += 1
                elif csv_name == 'ReservationStatus':
                    parsed_data = parse_reservation_status_data(csv_content)
                    db.insert_reservation_status(parsed_data)
                    table_record_counts['RESERVATION_STATUS'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} reservation status records")
                    
                    processed_count += 1
                elif csv_name == 'ReservationTypes':
                    parsed_data = parse_reservation_types_data(csv_content)
                    db.insert_reservation_types(parsed_data)
                    table_record_counts['RESERVATION_TYPES'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} reservation type records")
                    
                    processed_count += 1
                elif csv_name == 'ContactTypes':
                    parsed_data = parse_contact_types_data(csv_content)
                    db.insert_contact_types(parsed_data)
                    table_record_counts['CONTACT_TYPES'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} contact type records")
                    
                    processed_count += 1
                elif csv_name == 'InvoiceStatusSet':
                    parsed_data = parse_invoice_status_data(csv_content)
                    db.insert_invoice_status(parsed_data)
                    table_record_counts['INVOICE_STATUS'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} invoice status records")
                    
                    processed_count += 1
                elif csv_name == 'InvoiceTypeSet':
                    parsed_data = parse_invoice_types_data(csv_content)
                    db.insert_invoice_types(parsed_data)
                    table_record_counts['INVOICE_TYPES'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} invoice type records")
                    
                    processed_count += 1
                elif csv_name == 'TransactionTypeSet':
                    parsed_data = parse_transaction_types_data(csv_content)
                    db.insert_transaction_types(parsed_data)
                    table_record_counts['TRANSACTION_TYPES'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} transaction type records")
                    
                    processed_count += 1
                elif csv_name == 'TransactionMethodSet':
                    parsed_data = parse_transaction_methods_data(csv_content)
                    db.insert_transaction_methods(parsed_data)
                    table_record_counts['TRANSACTION_METHODS'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} transaction method records")
                    
                    processed_count += 1
                elif csv_name == 'InsuranceSet':
                    parsed_data = parse_insurance_data(csv_content)
                    db.insert_insurance(parsed_data)
                    table_record_counts['INSURANCE_STATUS'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} insurance records")
                    
                    processed_count += 1
                elif csv_name == 'EquipmentSet':
                    parsed_data = parse_equipment_data(csv_content)
                    db.insert_equipment(parsed_data)
                    table_record_counts['EQUIPMENT'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} equipment records")
                    
                    processed_count += 1
                elif csv_name == 'AccountStatus':
                    parsed_data = parse_account_status_data(csv_content)
                    db.insert_account_status(parsed_data)
                    table_record_counts['ACCOUNT_STATUS'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} account status records")
                    
                    processed_count += 1
                elif csv_name == 'ContactAutoChargeSet':
                    parsed_data = parse_contact_auto_charge_data(csv_content)
                    db.insert_contact_auto_charge(parsed_data)
                    table_record_counts['CONTACT_AUTO_CHARGE'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} contact auto charge records")
                    
                    processed_count += 1
                elif csv_name == 'StatementsPreferenceSet':
                    parsed_data = parse_statements_preference_data(csv_content)
                    db.insert_statements_preference(parsed_data)
                    table_record_counts['STATEMENTS_PREFERENCE'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} statements preference records")
                    
                    processed_count += 1
                elif csv_name == 'InvoiceItemTypeSet':
                    parsed_data = parse_invoice_item_types_data(csv_content)
                    db.insert_invoice_item_types(parsed_data)
                    table_record_counts['INVOICE_ITEM_TYPES'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} invoice item type records")
                    
                    processed_count += 1
                elif csv_name == 'PaymentMethods':
                    parsed_data = parse_payment_methods_data(csv_content)
                    db.insert_payment_methods(parsed_data)
                    table_record_counts['PAYMENT_METHODS'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} payment method records")
                    
                    processed_count += 1
                elif csv_name == 'SeasonalChargeMethods':
                    parsed_data = parse_seasonal_charge_methods_data(csv_content)
                    db.insert_seasonal_charge_methods(parsed_data)
                    table_record_counts['SEASONAL_CHARGE_METHODS'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} seasonal charge method records")
                    
                    processed_count += 1
                elif csv_name == 'SeasonalInvoicingMethodSet':
                    parsed_data = parse_seasonal_invoicing_methods_data(csv_content)
                    db.insert_seasonal_invoicing_methods(parsed_data)
                    table_record_counts['SEASONAL_INVOICING_METHODS'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} seasonal invoicing method records")
                    
                    processed_count += 1
                elif csv_name == 'TransientChargeMethods':
                    parsed_data = parse_transient_charge_methods_data(csv_content)
                    db.insert_transient_charge_methods(parsed_data)
                    table_record_counts['TRANSIENT_CHARGE_METHODS'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} transient charge method records")
                    
                    processed_count += 1
                elif csv_name == 'TransientInvoicingMethodSet':
                    parsed_data = parse_transient_invoicing_methods_data(csv_content)
                    db.insert_transient_invoicing_methods(parsed_data)
                    table_record_counts['TRANSIENT_INVOICING_METHODS'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} transient invoicing method records")
                    
                    processed_count += 1
                elif csv_name == 'RecurringInvoiceOptionsSet':
                    parsed_data = parse_recurring_invoice_options_data(csv_content)
                    db.insert_recurring_invoice_options(parsed_data)
                    table_record_counts['RECURRING_INVOICE_OPTIONS'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} recurring invoice option records")
                    
                    processed_count += 1
                elif csv_name == 'DueDateSettingsSet':
                    parsed_data = parse_due_date_settings_data(csv_content)
                    db.insert_due_date_settings(parsed_data)
                    table_record_counts['DUE_DATE_SETTINGS'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} due date setting records")
                    
                    processed_count += 1
                elif csv_name == 'ItemChargeMethods':
                    parsed_data = parse_item_charge_methods_data(csv_content)
                    db.insert_item_charge_methods(parsed_data)
                    table_record_counts['ITEM_CHARGE_METHODS'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} item charge method records")
                    
                    processed_count += 1
                elif csv_name == 'InsuranceStatusSet':
                    parsed_data = parse_insurance_status_data(csv_content)
                    db.insert_insurance_status(parsed_data)
                    table_record_counts['INSURANCE_STATUS_ALT'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} insurance status records")
                    
                    processed_count += 1
                elif csv_name == 'EquipmentTypeSet':
                    parsed_data = parse_equipment_types_data(csv_content)
                    db.insert_equipment_types(parsed_data)
                    table_record_counts['EQUIPMENT_TYPES'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} equipment type records")
                    
                    processed_count += 1
                elif csv_name == 'EquipmentFuelTypeSet':
                    parsed_data = parse_equipment_fuel_types_data(csv_content)
                    db.insert_equipment_fuel_types(parsed_data)
                    table_record_counts['EQUIPMENT_FUEL_TYPES'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} equipment fuel type records")
                    
                    processed_count += 1
                elif csv_name == 'VesselEngineClassSet':
                    parsed_data = parse_vessel_engine_class_data(csv_content)
                    db.insert_vessel_engine_class(parsed_data)
                    table_record_counts['VESSEL_ENGINE_CLASS'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} vessel engine class records")
                    
                    processed_count += 1
                elif csv_name == 'Cities':
                    parsed_data = parse_cities_data(csv_content)
                    db.insert_cities(parsed_data)
                    table_record_counts['CITIES'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} city records")
                    
                    processed_count += 1
                elif csv_name == 'Countries':
                    parsed_data = parse_countries_data(csv_content)
                    db.insert_countries(parsed_data)
                    table_record_counts['COUNTRIES'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} country records")
                    
                    processed_count += 1
                elif csv_name == 'CurrenciesSet':
                    parsed_data = parse_currencies_data(csv_content)
                    db.insert_currencies(parsed_data)
                    table_record_counts['CURRENCIES'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} currency records")
                    
                    processed_count += 1
                elif csv_name == 'PhoneTypes':
                    parsed_data = parse_phone_types_data(csv_content)
                    db.insert_phone_types(parsed_data)
                    table_record_counts['PHONE_TYPES'] = len(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} phone type records")
                    
                    processed_count += 1
                elif csv_name == 'AddressTypeSet':
                    parsed_data = parse_address_types_data(csv_content)
                    db.insert_address_types(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} address type records")
                    
                    processed_count += 1
                elif csv_name == 'InstalmentsPaymentMethodSet':
                    parsed_data = parse_installments_payment_methods_data(csv_content)
                    db.insert_installments_payment_methods(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} installments payment method records")
                    
                    processed_count += 1
                elif csv_name == 'PaymentsProviderSet':
                    parsed_data = parse_payments_provider_data(csv_content)
                    db.insert_payments_provider(parsed_data)
                    logger.info(f"✅ Processed {len(parsed_data)} payments provider records")
                    processed_count += 1
                    
                else:
                    logger.warning(f"⚠️  No parser available for {csv_name}.csv, skipping...")
                    skipped_count += 1
                    
            except Exception as e:
                logger.error(f"❌ Error processing {csv_name}.csv: {e}")
                logger.exception(e)
                error_count += 1
                continue
        
        # STEP 3: Run stored procedures to merge staging data into data warehouse
        logger.info("\n" + "="*70)
        logger.info("STEP 3: MERGING STAGING DATA TO DATA WAREHOUSE")
        logger.info("="*70)
        logger.info("Calling stored procedures to merge STG_MOLO_* → DW_MOLO_*...")
        
        # Run all merge procedures
        merge_stats = db.run_all_merges()
        logger.info("✅ All merge stored procedures completed successfully")
        
        # Log merge statistics (inserts vs updates)
        if merge_stats:
            logger.info("\n📊 Data Warehouse Merge Summary:")
            logger.info("   (Showing results from stored procedures with OUT parameters)")
            for table in sorted(merge_stats.keys()):
                try:
                    stats = merge_stats[table]
                    inserted = stats['inserted']
                    updated = stats['updated']
                    
                    # Build clear message
                    if inserted > 0 and updated > 0:
                        logger.info(
                            f"   ✓ {table}: "
                            f"{inserted:,} new record(s) inserted, "
                            f"{updated:,} existing record(s) updated"
                        )
                    elif inserted > 0:
                        logger.info(
                            f"   ✓ {table}: "
                            f"{inserted:,} new record(s) inserted"
                        )
                    elif updated > 0:
                        logger.info(
                            f"   ✓ {table}: "
                            f"{updated:,} existing record(s) updated"
                        )
                    else:
                        logger.info(
                            f"   • {table}: No changes (data already up to date)"
                        )
                except Exception as e:
                    logger.warning(f"   ⚠ {table}: Could not retrieve statistics - {e}")
        
        logger.info("")  # Blank line for readability
        
        # Clean up database connection
        db.close()
        
        # Summary report
        logger.info("\n" + "="*70)
        logger.info("MOLO DATA PROCESSING SUMMARY")
        logger.info("="*70)
        logger.info(f"✅ Successfully processed: {processed_count} files")
        logger.info(f"⚠️  Skipped: {skipped_count} files")
        logger.info(f"❌ Errors: {error_count} files")
        
        # Display table-level statistics
        if table_record_counts:
            total_records = sum(table_record_counts.values())
            logger.info(f"📊 Total records loaded: {total_records:,}")
            logger.info("")
            logger.info("✅ Successfully processed tables:")
            for table_name, record_count in sorted(table_record_counts.items()):
                table_display = f"DW_MOLO_{table_name}"
                logger.info(f"   ✅ {table_display:<35} → {record_count:>6,} records")
        
        logger.info("="*70)
        
        if skipped_count > 0:
            logger.warning(f"\n⚠️  {skipped_count} file(s) were skipped - no parser available")
        
        if error_count == 0:
            logger.info("\n✅ All MOLO CSV files processed successfully!")
        else:
            logger.warning(f"\n⚠️  Processing completed with {error_count} error(s)")
        
        # Return processing statistics
        return {
            'processed_count': processed_count,
            'skipped_count': skipped_count,
            'error_count': error_count,
            'files_processed': list(extracted_csv_data.keys()),
            'zip_file': latest_zip_key,
            'table_record_counts': table_record_counts  # Add table-level stats
        }
    
    except NoCredentialsError:
        logger.error("AWS credentials not found.")
        logger.error(
            "Please configure your AWS credentials in config.json file."
        )
        return None
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucket':
            logger.error(f"The bucket '{bucket}' does not exist.")
        elif e.response['Error']['Code'] == 'NoSuchKey':
            logger.error(
                f"The file '{latest_zip_key}' was not found in "
                f"the bucket '{bucket}'."
            )
        else:
            logger.exception(f"An S3 client error occurred: {e}")
        return None
    except zipfile.BadZipFile:
        logger.error("The downloaded file is not a valid ZIP file.")
        return None
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
        return None


# =============================================================================
# EMAIL NOTIFICATION FUNCTIONS
# =============================================================================

def send_email_notification(config, subject, summary_data, zip_file_paths=None, is_error=False):
    """
    Send HTML email notification with formatted results and ZIP file attachments.
    
    Args:
        config (dict): Configuration dictionary with email settings
        subject (str): Email subject line
        summary_data (dict): Dictionary containing processing statistics
        zip_file_paths (list): List of paths to ZIP files to attach (optional)
        is_error (bool): Whether this is an error notification
    """
    # Check if email is enabled
    if not config.get("email", {}).get("enabled", False):
        logger.info("Email notifications disabled in config")
        return
    
    try:
        email_config = config["email"]
        
        # Create message
        msg = MIMEMultipart('mixed')
        msg['From'] = email_config["from_email"]
        msg['To'] = ", ".join(email_config["to_emails"])
        
        # Add prefix to subject
        subject_prefix = email_config.get("subject_prefix", "[Marina ETL]")
        status = "ERROR" if is_error else "SUCCESS"
        full_subject = f"{subject_prefix} {status} - {subject}"
        msg['Subject'] = full_subject
        
        # Create alternative part for HTML and plain text
        msg_alternative = MIMEMultipart('alternative')
        
        # Create HTML email content
        html_content = create_html_report(summary_data, is_error)
        
        # Create plain text fallback
        plain_text = create_plain_text_report(summary_data, is_error)
        
        # Attach both HTML and plain text
        part1 = MIMEText(plain_text, 'plain')
        part2 = MIMEText(html_content, 'html')
        
        msg_alternative.attach(part1)
        msg_alternative.attach(part2)
        msg.attach(msg_alternative)
        
        # Attach ZIP files if provided
        if zip_file_paths:
            for zip_path in zip_file_paths:
                if os.path.exists(zip_path):
                    try:
                        with open(zip_path, 'rb') as attachment:
                            part = MIMEBase('application', 'zip')
                            part.set_payload(attachment.read())
                            encoders.encode_base64(part)
                            filename = os.path.basename(zip_path)
                            part.add_header(
                                'Content-Disposition',
                                f'attachment; filename= {filename}'
                            )
                            msg.attach(part)
                            logger.info(f"  📎 Attached file: {filename}")
                    except Exception as e:
                        logger.warning(f"Failed to attach {zip_path}: {e}")
                else:
                    logger.warning(f"ZIP file not found for attachment: {zip_path}")
        
        # Send email
        with smtplib.SMTP(email_config["smtp_server"], 
                         email_config["smtp_port"]) as server:
            server.starttls()
            server.login(email_config["username"], email_config["password"])
            text = msg.as_string()
            server.sendmail(email_config["from_email"], 
                          email_config["to_emails"], text)
        
        logger.info(f"✅ Email notification sent successfully to: "
                   f"{', '.join(email_config['to_emails'])}")
        
    except Exception as e:
        logger.error(f"Failed to send email notification: {e}", exc_info=True)


def create_html_report(summary_data, is_error=False):
    """Create HTML formatted email report"""
    status_icon = "❌" if is_error else "✅"
    status_text = "COMPLETED WITH ERRORS" if is_error else "SUCCESS"
    status_color = "#d32f2f" if is_error else "#388e3c"
    
    # Get data from summary
    start_time = summary_data.get('start_time', 'Unknown')
    end_time = summary_data.get('end_time', 'Unknown')
    duration = summary_data.get('duration', 'Unknown')
    molo_stats = summary_data.get('molo_stats', {})
    stellar_stats = summary_data.get('stellar_stats', {})
    errors = summary_data.get('errors', [])
    warnings = summary_data.get('warnings', [])
    
    # Calculate totals
    molo_records = sum(v for v in molo_stats.values() if isinstance(v, int))
    stellar_records = sum(v for v in stellar_stats.values() if isinstance(v, int))
    total_records = molo_records + stellar_records
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
            .container {{ background-color: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            .header {{ background-color: {status_color}; color: white; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
            .section {{ margin-bottom: 20px; }}
            .section-header {{ background-color: #e3f2fd; padding: 10px; border-left: 4px solid #2196f3; margin-bottom: 10px; font-weight: bold; }}
            .summary-table {{ width: 100%; border-collapse: collapse; margin-bottom: 20px; }}
            .summary-table th, .summary-table td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            .summary-table th {{ background-color: #f2f2f2; }}
            .data-table {{ width: 100%; border-collapse: collapse; margin-bottom: 15px; }}
            .data-table th {{ background-color: #2196f3; color: white; padding: 8px; }}
            .data-table td {{ padding: 8px; border: 1px solid #ddd; }}
            .data-table td:first-child {{ text-align: left; }}
            .data-table td:last-child {{ text-align: right; }}
            .error-section {{ background-color: #ffebee; border: 1px solid #f44336; border-radius: 4px; padding: 10px; margin: 10px 0; }}
            .warning-section {{ background-color: #fff3e0; border: 1px solid #ff9800; border-radius: 4px; padding: 10px; margin: 10px 0; }}
            .subsection {{ margin-left: 20px; margin-top: 10px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h2>{status_icon} Marina Data Processing Pipeline Report</h2>
            </div>
            
            <div class="section">
                <div class="section-header">📊 Summary</div>
                <table class="summary-table">
                    <tr><td><strong>Status</strong></td><td style="color: {status_color}; font-weight: bold;">{status_text}</td></tr>
                    <tr><td><strong>Start Time</strong></td><td>{start_time}</td></tr>
                    <tr><td><strong>End Time</strong></td><td>{end_time}</td></tr>
                    <tr><td><strong>Duration</strong></td><td>{duration}</td></tr>
                    <tr><td><strong>Total Records</strong></td><td>{total_records:,}</td></tr>
                    <tr><td><strong>Errors</strong></td><td>{len(errors)}</td></tr>
                    <tr><td><strong>Warnings</strong></td><td>{len(warnings)}</td></tr>
                </table>
            </div>"""
    
    # Add MOLO statistics
    if molo_stats:
        html += f"""
            <div class="section">
                <div class="section-header">🏢 MOLO System Data</div>
                <div class="subsection">
                    <p><strong>Total Records:</strong> {molo_records:,}</p>
                    <table class="data-table">
                        <tr><th>Table</th><th>Records Processed</th></tr>"""
        
        for table, count in sorted(molo_stats.items()):
            if isinstance(count, int):
                html += f'<tr><td>{table}</td><td>{count:,}</td></tr>'
        
        html += """
                    </table>
                </div>
            </div>"""
    
    # Add Stellar statistics
    if stellar_stats:
        html += f"""
            <div class="section">
                <div class="section-header">⭐ Stellar Business Data</div>
                <div class="subsection">
                    <p><strong>Total Records:</strong> {stellar_records:,}</p>
                    <table class="data-table">
                        <tr><th>Table</th><th>Records Processed</th></tr>"""
        
        for table, count in sorted(stellar_stats.items()):
            if isinstance(count, int):
                html += f'<tr><td>{table}</td><td>{count:,}</td></tr>'
        
        html += """
                    </table>
                </div>
            </div>"""
    
    # Add errors section if there are errors
    if errors:
        html += f"""
            <div class="section">
                <div class="section-header">❌ Errors ({len(errors)})</div>"""
        for i, error in enumerate(errors[:10]):  # Show first 10 errors
            html += f'<div class="error-section">• {error}</div>'
        if len(errors) > 10:
            html += f'<div class="error-section">... and {len(errors) - 10} more errors</div>'
        html += "</div>"
    
    # Add warnings section if there are warnings
    if warnings:
        html += f"""
            <div class="section">
                <div class="section-header">⚠️ Warnings ({len(warnings)})</div>"""
        for i, warning in enumerate(warnings[:10]):  # Show first 10 warnings
            html += f'<div class="warning-section">• {warning}</div>'
        if len(warnings) > 10:
            html += f'<div class="warning-section">... and {len(warnings) - 10} more warnings</div>'
        html += "</div>"
    
    html += """
            <div style="margin-top: 30px; padding: 15px; background-color: #f5f5f5; border-radius: 4px; font-size: 12px; color: #666;">
                This report was automatically generated by the Marina Data Processing Pipeline.<br>
                For technical support, please contact your system administrator.
            </div>
        </div>
    </body>
    </html>
    """
    
    return html


def create_plain_text_report(summary_data, is_error=False):
    """Create plain text fallback report"""
    status_text = "COMPLETED WITH ERRORS" if is_error else "SUCCESS"
    
    # Get data from summary
    start_time = summary_data.get('start_time', 'Unknown')
    end_time = summary_data.get('end_time', 'Unknown')
    duration = summary_data.get('duration', 'Unknown')
    molo_stats = summary_data.get('molo_stats', {})
    stellar_stats = summary_data.get('stellar_stats', {})
    errors = summary_data.get('errors', [])
    warnings = summary_data.get('warnings', [])
    
    # Calculate totals
    molo_records = sum(v for v in molo_stats.values() if isinstance(v, int))
    stellar_records = sum(v for v in stellar_stats.values() if isinstance(v, int))
    total_records = molo_records + stellar_records
    
    text = f"""
Marina Data Processing Pipeline Report
{'='*70}

Status: {status_text}
Start Time: {start_time}
End Time: {end_time}
Duration: {duration}
Total Records: {total_records:,}
Errors: {len(errors)}
Warnings: {len(warnings)}

"""
    
    if molo_stats:
        text += f"""
MOLO System Data
{'-'*70}
Total Records: {molo_records:,}

Table                                    Records
{'-'*70}
"""
        for table, count in sorted(molo_stats.items()):
            if isinstance(count, int):
                text += f"{table:<40} {count:>10,}\n"
    
    if stellar_stats:
        text += f"""

Stellar Business Data
{'-'*70}
Total Records: {stellar_records:,}

Table                                    Records
{'-'*70}
"""
        for table, count in sorted(stellar_stats.items()):
            if isinstance(count, int):
                text += f"{table:<40} {count:>10,}\n"
    
    if errors:
        text += f"\n\nErrors ({len(errors)}):\n{'-'*70}\n"
        for error in errors[:10]:
            text += f"• {error}\n"
        if len(errors) > 10:
            text += f"... and {len(errors) - 10} more errors\n"
    
    if warnings:
        text += f"\n\nWarnings ({len(warnings)}):\n{'-'*70}\n"
        for warning in warnings[:10]:
            text += f"• {warning}\n"
        if len(warnings) > 10:
            text += f"... and {len(warnings) - 10} more warnings\n"
    
    text += f"\n\n{'='*70}\n"
    text += "This report was automatically generated by the Marina Data Processing Pipeline.\n"
    
    return text


# =============================================================================
# COMMAND LINE INTERFACE
# =============================================================================

if __name__ == "__main__":
    # Initialize logging first
    setup_logging()

    # Set up command line argument parsing
    parser = argparse.ArgumentParser(
        description=(
            "Marina Data Processing Pipeline: Download and process data from "
            "MOLO (ZIP files) and Stellar Business (gzipped DATA files) systems, "
            "synchronizing with Oracle database using INSERT operations into staging tables."
        )
    )
    
    # Define command line arguments with environment variable fallbacks
    parser.add_argument(
        "--bucket",
        default=os.getenv("S3_BUCKET", "cnxtestbucket"),
        help="The name of the S3 bucket. (Env: S3_BUCKET)"
    )
    parser.add_argument(
        "--s3-prefix",
        default=os.getenv("S3_PREFIX", ""),
        help=(
            "(UNUSED) S3 prefix - script now searches entire bucket. "
            "(Env: S3_PREFIX)"
        )
    )
    parser.add_argument(
        "--region",
        default=os.getenv("S3_REGION", "us-east-1"),
        help="The AWS region of the bucket. (Env: S3_REGION)"
    )
    parser.add_argument(
        "--db-user",
        default=os.getenv("DB_USER", "API_USER"),
        help="Oracle database username. (Env: DB_USER)"
    )
    parser.add_argument(
        "--db-password",
        default=os.getenv("DB_PASSWORD"),
        help="Oracle database password. (Env: DB_PASSWORD)"
    )
    parser.add_argument(
        "--db-dsn",
        default=os.getenv("DB_DSN", "oax4504110443_low"),
        help="Oracle database DSN. (Env: DB_DSN)"
    )
    parser.add_argument(
        "--process-molo",
        action="store_true",
        default=True,
        help="Process MOLO data from ZIP files (default: True)"
    )
    parser.add_argument(
        "--process-stellar",
        action="store_true",
        default=True,
        help="Process Stellar Business data from gzipped files (default: True)"
    )
    parser.add_argument(
        "--stellar-bucket",
        default=os.getenv("STELLAR_S3_BUCKET", "resilient-ims-backups"),
        help="S3 bucket for Stellar data (Env: STELLAR_S3_BUCKET)"
    )
    parser.add_argument(
        "--validate-fields",
        action="store_true",
        default=False,
        help="Enable field-level validation (CSV vs DB comparison)"
    )
    parser.add_argument(
        "--validate-merge-changes",
        action="store_true",
        default=False,
        help="Validate merge operations don't modify data unexpectedly"
    )
    parser.add_argument(
        "--validation-sample-size",
        type=int,
        default=10,
        help="Number of records to sample for field validation (default: 10)"
    )

    args = parser.parse_args()

    # Load credentials from config file or environment variables
    logger.info("=" * 80)
    logger.info("LOADING CONFIGURATION")
    logger.info("=" * 80)
    
    # Try loading from config file first (local development)
    config = load_config_file('config.json')
    
    if config:
        logger.info("✅ Loaded configuration from config.json")
        # Extract credentials from config file
        aws_access_key = config['aws'].get('access_key_id')
        aws_secret_key = config['aws'].get('secret_access_key')
        
        # Use config file for database credentials, allow command-line override
        db_user = args.db_user or config['database'].get('user', 'API_USER')
        db_password = args.db_password or config['database'].get('password')
        db_dsn = args.db_dsn or config['database'].get('dsn', 'oax4504110443_low')
        
        # Use config file for bucket names if not specified on command line
        bucket = args.bucket if args.bucket != 'cnxtestbucket' else config.get('s3', {}).get('molo_bucket', 'cnxtestbucket')
        stellar_bucket = args.stellar_bucket if args.stellar_bucket != 'resilient-ims-backups' else config.get('s3', {}).get('stellar_bucket', 'resilient-ims-backups')
    else:
        # Fallback to environment variables (for OCI/container deployments)
        logger.warning("⚠️  config.json not found, using environment variables")
        logger.info("📋 Loading credentials from environment variables or OCI Vault...")
        
        # Get OCI Vault secrets if in OCI environment
        vault_secrets = get_oci_vault_secrets()
        
        if vault_secrets:
            logger.info("✅ Loaded credentials from OCI Vault")
            aws_access_key = vault_secrets.get('aws_access_key_id')
            aws_secret_key = vault_secrets.get('aws_secret_access_key')
            db_password = vault_secrets.get('db_password')
        else:
            logger.info("� Using environment variables for credentials")
            aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
            aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
            db_password = os.getenv('DB_PASSWORD')
        
        # Use environment variables or command-line args
        db_user = args.db_user or os.getenv('DB_USER', 'API_USER')
        db_dsn = args.db_dsn or os.getenv('DB_DSN', 'oax4504110443_low')
        bucket = args.bucket or os.getenv('S3_BUCKET', 'cnxtestbucket')
        stellar_bucket = args.stellar_bucket or os.getenv('STELLAR_S3_BUCKET', 'resilient-ims-backups')
    
    logger.info(f"�📂 MOLO S3 Bucket: {bucket}")
    logger.info(f"📂 Stellar S3 Bucket: {stellar_bucket}")
    logger.info(f"🗄️  Database: {db_user}@{db_dsn}")
    logger.info("✅ Configuration loaded successfully")
    logger.info("")

    # Validate required database credentials
    if not all([db_user, db_password, db_dsn]):
        logger.critical(
            "❌ Error: Database credentials (DB_USER, DB_PASSWORD, DB_DSN) "
            "are required in config.json or environment variables"
        )
        exit(1)
        
    if not all([aws_access_key, aws_secret_key]):
        logger.critical(
            "❌ Error: AWS credentials (access_key_id, secret_access_key) "
            "are required in config.json or environment variables"
        )
        exit(1)
    
    logger.info("=" * 80)
    logger.info("MARINA DATA PROCESSING PIPELINE - MOLO & STELLAR")
    logger.info("=" * 80)
    logger.info(f"MOLO Processing: {'ENABLED' if args.process_molo else 'DISABLED'}")
    logger.info(f"Stellar Processing: {'ENABLED' if args.process_stellar else 'DISABLED'}")
    logger.info("")
    logger.info("Validation Options:")
    logger.info(f"  Field-level validation: {'ENABLED' if args.validate_fields else 'DISABLED'}")
    logger.info(f"  Merge change validation: {'ENABLED' if args.validate_merge_changes else 'DISABLED'}")
    if args.validate_fields or args.validate_merge_changes:
        logger.info(f"  Sample size: {args.validation_sample_size} records")
    logger.info("=" * 80)
    
    # Initialize processing tracking
    start_time = datetime.now()
    start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    errors = []
    warnings = []
    molo_stats = {}
    stellar_stats = {}
    zip_files_processed = []
    has_errors = False
    
    # Execute MOLO processing (ZIP files from main bucket)
    if args.process_molo:
        logger.info("\n" + "=" * 80)
        logger.info("STEP 1: Processing MOLO Data")
        logger.info("=" * 80)
        try:
            molo_results = read_s3_zip_and_insert_to_db(
                bucket,
                args.s3_prefix,
                args.region,
                db_user,
                db_password,
                db_dsn,
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                validate_fields=args.validate_fields,
                validate_merge_changes=args.validate_merge_changes,
                validation_sample_size=args.validation_sample_size
            )
            
            if molo_results:
                # Capture MOLO table-level statistics (like Stellar does)
                molo_stats = molo_results.get('table_record_counts', {})
                
                # Track ZIP file for email attachment
                if molo_results.get('zip_file'):
                    zip_files_processed.append(molo_results['zip_file'])
                
                logger.info("✅ MOLO data processing completed successfully")
            else:
                error_msg = "MOLO processing returned no results"
                errors.append(error_msg)
                has_errors = True
                
        except Exception as e:
            error_msg = f"Error processing MOLO data: {str(e)}"
            logger.exception(f"❌ {error_msg}")
            errors.append(error_msg)
            has_errors = True
    else:
        logger.info("\n⏭️  Skipping MOLO data processing (disabled)")
    
    # Execute Stellar processing (gzipped DATA files from resilient-ims-backups)
    if args.process_stellar:
        if STELLAR_AVAILABLE:
            logger.info("\n" + "=" * 80)
            logger.info("STEP 2: Processing Stellar Business Data")
            logger.info("=" * 80)
            logger.info("")
            logger.info("📊 Processing Stellar Business data from S3...")
            logger.info(f"   Bucket: {stellar_bucket}")
            logger.info(f"   Region: {args.region}")
            logger.info("")
            logger.info("📊 Expected tables in backup: 29 tables")
            logger.info("   (Reference data, bookings, boats, pricing, POS, etc.)")
            logger.info("")
            
            try:
                stellar_results = process_stellar_data_from_s3(
                    bucket=stellar_bucket,
                    region=args.region,
                    db_user=db_user,
                    db_password=db_password,
                    db_dsn=db_dsn,
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_key
                )
                
                # Capture Stellar statistics
                stellar_stats = stellar_results.get('successful_tables', {})
                
                # Display results dynamically
                logger.info("")
                logger.info("=" * 80)
                logger.info("✅ STELLAR DATA PROCESSING COMPLETED")
                logger.info("=" * 80)
                
                successful_count = len(stellar_results['successful_tables'])
                total_records = stellar_results['total_records']
                total_tables = stellar_results['total_tables']
                
                logger.info(f"📊 Successfully processed: {successful_count}/{total_tables} tables")
                logger.info(f"📊 Total records loaded: {total_records:,}")
                logger.info("")
                
                if stellar_results['successful_tables']:
                    logger.info("✅ Successfully processed tables:")
                    for table_name, record_count in sorted(stellar_results['successful_tables'].items()):
                        table_display = f"DW_STELLAR_{table_name.upper()}"
                        logger.info(f"   ✅ {table_display:<35} → {record_count:>6,} records")
                
                if stellar_results['failed_tables']:
                    logger.info("")
                    logger.info("⚠️  Failed/Missing tables:")
                    for table_name, reason in sorted(stellar_results['failed_tables'].items()):
                        table_display = f"DW_STELLAR_{table_name.upper()}"
                        logger.info(f"   ❌ {table_display:<35} → {reason}")
                        warnings.append(f"Failed to process {table_display}: {reason}")
                
                logger.info("")
                logger.info("=" * 80)
            except Exception as e:
                error_msg = f"Error processing Stellar data: {str(e)}"
                logger.exception(f"❌ {error_msg}")
                errors.append(error_msg)
                has_errors = True
        else:
            warning_msg = "Stellar processing module not available"
            logger.warning(f"\n⚠️  {warning_msg}. Install download_stellar_from_s3.py to enable Stellar processing.")
            warnings.append(warning_msg)
    else:
        logger.info("\n⏭️  Skipping Stellar data processing (disabled)")
    
    # Calculate end time and duration
    end_time = datetime.now()
    end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
    duration = end_time - start_time
    duration_str = str(duration).split('.')[0]  # Remove microseconds
    
    logger.info("\n" + "=" * 80)
    logger.info(f"✅ COMPLETE: All requested data processing finished")
    logger.info(f"Duration: {duration_str}")
    logger.info("=" * 80)
    
    # Display comprehensive summary
    logger.info("\n" + "=" * 80)
    logger.info("COMPREHENSIVE PROCESSING SUMMARY")
    logger.info("=" * 80)
    
    if molo_stats:
        logger.info("\n🏢 MOLO System:")
        total_molo_records = sum(v for v in molo_stats.values() if isinstance(v, int))
        logger.info(f"   Total Records: {total_molo_records:,}")
        logger.info(f"   Tables Processed: {len(molo_stats)}")
        for table, count in sorted(molo_stats.items())[:5]:  # Show first 5 tables
            if isinstance(count, int):
                logger.info(f"   • {table}: {count:,} records")
        if len(molo_stats) > 5:
            logger.info(f"   ... and {len(molo_stats) - 5} more tables")
    
    if stellar_stats:
        logger.info("\n⭐ Stellar Business System:")
        total_stellar_records = sum(v for v in stellar_stats.values() if isinstance(v, int))
        logger.info(f"   Total Records: {total_stellar_records:,}")
        logger.info(f"   Tables Processed: {len(stellar_stats)}")
        for table, count in sorted(stellar_stats.items())[:5]:  # Show first 5 tables
            if isinstance(count, int):
                logger.info(f"   • {table}: {count:,} records")
        if len(stellar_stats) > 5:
            logger.info(f"   ... and {len(stellar_stats) - 5} more tables")
    
    if errors:
        logger.info(f"\n❌ Errors: {len(errors)}")
        for error in errors[:3]:
            logger.info(f"   • {error}")
        if len(errors) > 3:
            logger.info(f"   ... and {len(errors) - 3} more errors")
    
    if warnings:
        logger.info(f"\n⚠️ Warnings: {len(warnings)}")
        for warning in warnings[:3]:
            logger.info(f"   • {warning}")
        if len(warnings) > 3:
            logger.info(f"   ... and {len(warnings) - 3} more warnings")
    
    logger.info("\n" + "=" * 80)
    
    # Send email notification if configured
    if config:
        summary_data = {
            'start_time': start_time_str,
            'end_time': end_time_str,
            'duration': duration_str,
            'molo_stats': molo_stats,
            'stellar_stats': stellar_stats,
            'errors': errors,
            'warnings': warnings
        }
        
        subject = f"Marina ETL Pipeline - {datetime.now().strftime('%Y-%m-%d')}"
        
        logger.info("\n📧 Sending email notification...")
        send_email_notification(
            config=config,
            subject=subject,
            summary_data=summary_data,
            zip_file_paths=zip_files_processed,
            is_error=has_errors
        )
