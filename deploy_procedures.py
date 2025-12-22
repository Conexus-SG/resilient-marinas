#!/usr/bin/env python3
"""
Deploy All Stored Procedures Script

This script reads all SQL files from the stored_procedures directory and
deploys them to the Oracle database. It provides detailed logging and
error handling for the deployment process.

Usage:
    python deploy_procedures.py [--skip-confirmation]
    
    The script will:
    1. Load configuration from config.json
    2. Connect to the Oracle database
    3. Read all *.sql files from stored_procedures directory
    4. Execute each SQL file to create/update stored procedures
    5. Log success/failure for each procedure
"""

import os
import sys
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime

# Try to import oracledb, handle gracefully if not installed
try:
    import oracledb
except ImportError:
    print("ERROR: oracledb module not found. Install it with: pip install oracledb")
    sys.exit(1)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('deploy_procedures.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ProcedureDeployer:
    """Handles deployment of stored procedures to Oracle database."""
    
    def __init__(self, user, password, dsn):
        """
        Initialize the procedure deployer with database connection.
        
        Args:
            user (str): Database username
            password (str): Database password
            dsn (str): Database DSN (connection string)
        """
        self.user = user
        self.password = password
        self.dsn = dsn
        self.connection = None
        self.cursor = None
        self.setup_oracle_environment()
        self.connect()
    
    def setup_oracle_environment(self):
        """Set up Oracle wallet and Instant Client environment."""
        # Determine wallet location
        wallet_path = os.path.join(os.path.dirname(__file__), 'wallet_demo')
        
        if os.path.exists(wallet_path):
            os.environ['TNS_ADMIN'] = wallet_path
            logger.info(f"✅ TNS_ADMIN set to: {wallet_path}")
        else:
            logger.warning(f"⚠️  Wallet directory not found: {wallet_path}")
        
        # Initialize Oracle Instant Client
        try:
            oracle_client_path = '/opt/oracle/instantclient'
            if os.path.exists(oracle_client_path):
                oracledb.init_oracle_client(lib_dir=oracle_client_path)
                logger.info(f"✅ Oracle Instant Client initialized from: {oracle_client_path}")
        except Exception as e:
            logger.warning(f"⚠️  Could not initialize Oracle Instant Client: {e}")
    
    def connect(self):
        """Establish connection to the Oracle database."""
        try:
            logger.info("Attempting to connect to Oracle database...")
            logger.info(f"   User: {self.user}")
            logger.info(f"   DSN: {self.dsn}")
            
            self.connection = oracledb.connect(
                user=self.user,
                password=self.password,
                dsn=self.dsn
            )
            self.cursor = self.connection.cursor()
            logger.info("✅ Oracle database connection successful!")
        except Exception as e:
            logger.error(f"❌ Failed to connect to database: {e}")
            raise
    
    def deploy_procedure(self, sql_file_path):
        """
        Deploy a single stored procedure from SQL file.
        
        Args:
            sql_file_path (Path): Path to the SQL file
            
        Returns:
            tuple: (success: bool, message: str)
        """
        try:
            with open(sql_file_path, 'r') as f:
                sql_content = f.read().strip()
            
            if not sql_content:
                return False, "File is empty"
            
            # Execute the SQL
            self.cursor.execute(sql_content)
            self.connection.commit()
            
            # Extract procedure name from filename
            proc_name = sql_file_path.stem
            logger.info(f"✅ Successfully deployed: {proc_name}")
            return True, "Deployed successfully"
            
        except oracledb.DatabaseError as e:
            error_code, error_message = e.args
            return False, f"Database error {error_code}: {error_message}"
        except Exception as e:
            return False, f"Error: {str(e)}"
    
    def deploy_all(self, procedures_dir):
        """
        Deploy all stored procedures from the specified directory.
        
        Args:
            procedures_dir (str|Path): Path to directory containing SQL files
            
        Returns:
            dict: Deployment results with success/failure counts
        """
        procedures_dir = Path(procedures_dir)
        
        if not procedures_dir.exists():
            logger.error(f"❌ Procedures directory not found: {procedures_dir}")
            return {
                'total': 0,
                'successful': 0,
                'failed': 0,
                'results': {}
            }
        
        # Find all .sql files, excluding the deployment scripts themselves
        sql_files = sorted([
            f for f in procedures_dir.glob('*.sql')
            if not f.name.startswith('deploy_')
        ])
        
        if not sql_files:
            logger.warning(f"⚠️  No SQL files found in {procedures_dir}")
            return {
                'total': 0,
                'successful': 0,
                'failed': 0,
                'results': {}
            }
        
        logger.info(f"Found {len(sql_files)} stored procedure files to deploy")
        logger.info("=" * 80)
        
        results = {
            'total': len(sql_files),
            'successful': 0,
            'failed': 0,
            'results': {}
        }
        
        # Deploy each procedure
        for sql_file in sql_files:
            proc_name = sql_file.stem
            success, message = self.deploy_procedure(sql_file)
            
            if success:
                results['successful'] += 1
                results['results'][proc_name] = {'status': 'SUCCESS', 'message': message}
            else:
                results['failed'] += 1
                results['results'][proc_name] = {'status': 'FAILED', 'message': message}
                logger.error(f"❌ Failed to deploy {proc_name}: {message}")
        
        logger.info("=" * 80)
        return results
    
    def close(self):
        """Close the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("✅ Database connection closed")


def load_config(config_file='config.json'):
    """
    Load database configuration from JSON file.
    
    Args:
        config_file (str): Path to configuration file
        
    Returns:
        dict: Configuration dictionary with database credentials
    """
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        logger.error(f"❌ Configuration file not found: {config_file}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"❌ Invalid JSON in configuration file: {e}")
        raise


def print_deployment_summary(results):
    """Print a summary of deployment results."""
    print("\n" + "=" * 80)
    print("DEPLOYMENT SUMMARY")
    print("=" * 80)
    print(f"Total procedures: {results['total']}")
    print(f"✅ Successful: {results['successful']}")
    print(f"❌ Failed: {results['failed']}")
    print("=" * 80)
    
    if results['failed'] > 0:
        print("\nFailed procedures:")
        for proc_name, result in results['results'].items():
            if result['status'] == 'FAILED':
                print(f"  • {proc_name}: {result['message']}")
    
    print("\n")


def main():
    """Main entry point for the deployment script."""
    parser = argparse.ArgumentParser(
        description='Deploy all stored procedures to Oracle database'
    )
    parser.add_argument(
        '--skip-confirmation',
        action='store_true',
        help='Skip confirmation prompt before deployment'
    )
    parser.add_argument(
        '--config',
        default='config.json',
        help='Path to configuration file (default: config.json)'
    )
    parser.add_argument(
        '--procedures-dir',
        default='stored_procedures',
        help='Path to stored procedures directory (default: stored_procedures)'
    )
    
    args = parser.parse_args()
    
    try:
        # Load configuration
        logger.info("Loading configuration...")
        config = load_config(args.config)
        
        # Extract database credentials
        molo_config = config.get('molo_db', {})
        db_user = molo_config.get('username')
        db_password = molo_config.get('password')
        db_dsn = molo_config.get('dsn')
        
        if not all([db_user, db_password, db_dsn]):
            logger.error("❌ Missing database credentials in configuration")
            sys.exit(1)
        
        # Get procedures directory path
        procedures_dir = Path(args.procedures_dir)
        if not procedures_dir.is_absolute():
            procedures_dir = Path(__file__).parent / procedures_dir
        
        # Print deployment info
        print("\n" + "=" * 80)
        print("STORED PROCEDURES DEPLOYMENT")
        print("=" * 80)
        print(f"Configuration file: {args.config}")
        print(f"Procedures directory: {procedures_dir}")
        print(f"Database user: {db_user}")
        print(f"Database DSN: {db_dsn}")
        print("=" * 80 + "\n")
        
        # Confirm deployment
        if not args.skip_confirmation:
            response = input("Do you want to proceed with deployment? (yes/no): ").lower().strip()
            if response not in ['yes', 'y']:
                logger.info("Deployment cancelled by user")
                sys.exit(0)
        
        # Create deployer and run deployment
        deployer = ProcedureDeployer(db_user, db_password, db_dsn)
        
        try:
            logger.info("Starting procedure deployment...")
            results = deployer.deploy_all(procedures_dir)
            
            # Print summary
            print_deployment_summary(results)
            
            # Exit with appropriate code
            sys.exit(0 if results['failed'] == 0 else 1)
            
        finally:
            deployer.close()
    
    except KeyboardInterrupt:
        logger.info("Deployment interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"❌ Deployment failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
