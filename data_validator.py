"""
Data Validation Module

Validates that CSV data matches database table data after import.
Performs row counts, sample data checks, and data integrity validation.
"""

import logging
from typing import List, Dict, Optional, Tuple
import csv
import io


class DataValidator:
    """Validates data consistency between CSV and database tables."""
    
    def __init__(self, db_connector, logger: logging.Logger):
        """
        Initialize validator.
        
        Args:
            db_connector: Oracle database connector
            logger: Logger instance
        """
        self.db = db_connector
        self.logger = logger
    
    def validate_table_import(
        self,
        csv_content: str,
        staging_table: str,
        dw_table: str,
        id_column: str = "ID"
    ) -> Tuple[bool, List[str]]:
        """
        Validate that CSV data matches staging and DW tables.
        
        Args:
            csv_content: Raw CSV content
            staging_table: Name of staging table (e.g., STG_MOLO_BOATS)
            dw_table: Name of data warehouse table (e.g., DW_MOLO_BOATS)
            id_column: Name of ID column for matching
            
        Returns:
            Tuple of (passed: bool, issues: List[str])
        """
        issues = []
        
        try:
            # Count CSV rows
            csv_reader = csv.DictReader(io.StringIO(csv_content))
            csv_rows = list(csv_reader)
            csv_count = len(csv_rows)
            
            if csv_count == 0:
                return True, []  # Empty CSV is valid
            
            # Count staging table rows
            staging_count = self._get_row_count(staging_table)
            
            # Count DW table rows
            dw_count = self._get_row_count(dw_table)
            
            # Validate row counts
            if csv_count != staging_count:
                issues.append(
                    f"Row count mismatch: CSV={csv_count:,}, "
                    f"Staging={staging_count:,} "
                    f"(difference: {abs(csv_count - staging_count):,})"
                )
            
            # Validate that staging data merged to DW
            if staging_count > 0 and dw_count < staging_count:
                issues.append(
                    f"DW has fewer rows than staging: "
                    f"Staging={staging_count:,}, DW={dw_count:,}"
                )
            
            # Sample validation: check if first few IDs exist
            if csv_count > 0:
                sample_ids = [
                    row.get(id_column) or row.get('Id')
                    for row in csv_rows[:5]
                    if row.get(id_column) or row.get('Id')
                ]
                
                if sample_ids:
                    missing_ids = self._check_ids_exist(
                        dw_table, id_column, sample_ids
                    )
                    if missing_ids:
                        issues.append(
                            f"Sample IDs not found in DW: {missing_ids}"
                        )
            
            # Check for NULL IDs in staging
            null_id_count = self._count_null_ids(staging_table, id_column)
            if null_id_count > 0:
                issues.append(
                    f"{null_id_count} records with NULL {id_column} "
                    f"in staging"
                )
            
            passed = len(issues) == 0
            return passed, issues
            
        except Exception as e:
            self.logger.exception(f"Validation error: {e}")
            issues.append(f"Validation exception: {str(e)}")
            return False, issues
    
    def validate_merge_operation(
        self,
        staging_table: str,
        dw_table: str,
        expected_updates: int = None,
        expected_inserts: int = None
    ) -> Tuple[bool, List[str]]:
        """
        Validate merge operation results.
        
        Args:
            staging_table: Staging table name
            dw_table: DW table name
            expected_updates: Expected number of updates
            expected_inserts: Expected number of inserts
            
        Returns:
            Tuple of (passed: bool, issues: List[str])
        """
        issues = []
        
        try:
            staging_count = self._get_row_count(staging_table)
            dw_count = self._get_row_count(dw_table)
            
            # Check if all staging records made it to DW
            if staging_count > dw_count:
                issues.append(
                    f"Not all staging records in DW: "
                    f"Staging={staging_count:,}, DW={dw_count:,}"
                )
            
            # Validate expected counts if provided
            if expected_updates is not None or expected_inserts is not None:
                expected_total = (expected_updates or 0) + (
                    expected_inserts or 0
                )
                if expected_total != staging_count:
                    issues.append(
                        f"Merge count mismatch: "
                        f"Expected={expected_total:,}, "
                        f"Staging={staging_count:,}"
                    )
            
            passed = len(issues) == 0
            return passed, issues
            
        except Exception as e:
            self.logger.exception(f"Merge validation error: {e}")
            issues.append(f"Merge validation exception: {str(e)}")
            return False, issues
    
    def _get_row_count(self, table_name: str) -> int:
        """Get row count for a table."""
        try:
            query = f"SELECT COUNT(*) FROM {table_name}"
            self.db.cursor.execute(query)
            result = self.db.cursor.fetchone()
            return result[0] if result else 0
        except Exception as e:
            self.logger.warning(
                f"Could not get row count for {table_name}: {e}"
            )
            return 0
    
    def _check_ids_exist(
        self,
        table_name: str,
        id_column: str,
        ids: List[str]
    ) -> List[str]:
        """
        Check if IDs exist in table.
        
        Returns list of missing IDs.
        """
        try:
            if not ids:
                return []
            
            # Build parameterized query
            placeholders = ','.join([f":{i+1}" for i in range(len(ids))])
            query = f"""
                SELECT {id_column} 
                FROM {table_name} 
                WHERE {id_column} IN ({placeholders})
            """
            
            self.db.cursor.execute(query, ids)
            found_ids = {str(row[0]) for row in self.db.cursor.fetchall()}
            
            # Find missing IDs
            missing = [
                str(id_val) for id_val in ids
                if str(id_val) not in found_ids
            ]
            
            return missing
            
        except Exception as e:
            self.logger.warning(f"Could not check IDs in {table_name}: {e}")
            return []
    
    def _count_null_ids(self, table_name: str, id_column: str) -> int:
        """Count records with NULL ID."""
        try:
            query = f"SELECT COUNT(*) FROM {table_name} WHERE {id_column} IS NULL"
            self.db.cursor.execute(query)
            result = self.db.cursor.fetchone()
            return result[0] if result else 0
        except Exception as e:
            self.logger.warning(
                f"Could not count NULL IDs in {table_name}: {e}"
            )
            return 0
    
    def validate_data_types(
        self,
        table_name: str,
        column_checks: Dict[str, type]
    ) -> Tuple[bool, List[str]]:
        """
        Validate data types in table columns.
        
        Args:
            table_name: Name of table to check
            column_checks: Dict of {column_name: expected_python_type}
            
        Returns:
            Tuple of (passed: bool, issues: List[str])
        """
        issues = []
        
        try:
            for column, expected_type in column_checks.items():
                query = f"""
                    SELECT {column} 
                    FROM {table_name} 
                    WHERE {column} IS NOT NULL 
                    AND ROWNUM <= 10
                """
                
                self.db.cursor.execute(query)
                rows = self.db.cursor.fetchall()
                
                for row in rows:
                    value = row[0]
                    if value is not None:
                        if not isinstance(value, expected_type):
                            issues.append(
                                f"{table_name}.{column}: "
                                f"Expected {expected_type.__name__}, "
                                f"got {type(value).__name__}"
                            )
                            break  # Only report once per column
            
            passed = len(issues) == 0
            return passed, issues
            
        except Exception as e:
            self.logger.exception(f"Data type validation error: {e}")
            issues.append(f"Data type validation exception: {str(e)}")
            return False, issues
    
    def check_referential_integrity(
        self,
        child_table: str,
        parent_table: str,
        foreign_key: str,
        parent_key: str = "ID"
    ) -> Tuple[bool, List[str]]:
        """
        Check referential integrity between tables.
        
        Args:
            child_table: Child table name
            parent_table: Parent table name
            foreign_key: Foreign key column in child table
            parent_key: Primary key column in parent table
            
        Returns:
            Tuple of (passed: bool, issues: List[str])
        """
        issues = []
        
        try:
            query = f"""
                SELECT COUNT(*) 
                FROM {child_table} c
                WHERE c.{foreign_key} IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 
                    FROM {parent_table} p 
                    WHERE p.{parent_key} = c.{foreign_key}
                )
            """
            
            self.db.cursor.execute(query)
            result = self.db.cursor.fetchone()
            orphan_count = result[0] if result else 0
            
            if orphan_count > 0:
                issues.append(
                    f"{orphan_count} orphaned records in {child_table} "
                    f"({foreign_key} references missing {parent_table}.{parent_key})"
                )
            
            passed = len(issues) == 0
            return passed, issues
            
        except Exception as e:
            self.logger.warning(
                f"Referential integrity check failed: {e}"
            )
            issues.append(
                f"Referential integrity check exception: {str(e)}"
            )
            return False, issues
