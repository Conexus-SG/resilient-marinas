"""
Error Handler for Data Import Operations

Provides consistent error handling, retry logic, and error reporting
for both staging inserts and stored procedure merges.
"""

import logging
from typing import Callable, Any, Optional, List
from functools import wraps
import oracledb


class DataImportError(Exception):
    """Base exception for data import errors."""
    pass


class StagingInsertError(DataImportError):
    """Error during staging table insertion."""
    pass


class MergeOperationError(DataImportError):
    """Error during merge stored procedure."""
    pass


class ValidationError(DataImportError):
    """Error during data validation."""
    pass


class ErrorHandler:
    """
    Handles errors during data import with retry and reporting.
    """
    
    def __init__(self, logger: logging.Logger, max_retries: int = 3):
        """
        Initialize error handler.
        
        Args:
            logger: Logger instance
            max_retries: Maximum number of retries for operations
        """
        self.logger = logger
        self.max_retries = max_retries
        self.error_log: List[dict] = []
    
    def handle_staging_insert(
        self,
        table_name: str,
        operation: Callable,
        data_rows: List,
        *args,
        **kwargs
    ) -> tuple[int, int]:
        """
        Handle staging table insertion with error handling.
        
        Args:
            table_name: Name of staging table
            operation: Insert function to execute
            data_rows: Data to insert
            *args, **kwargs: Additional args for operation
            
        Returns:
            Tuple of (successful_count, error_count)
        """
        if not data_rows:
            self.logger.info(f"{table_name}: No data to insert")
            return 0, 0
        
        try:
            # Try full batch insert first
            operation(data_rows, *args, **kwargs)
            self.logger.info(
                f"{table_name}: Inserted {len(data_rows):,} records"
            )
            return len(data_rows), 0
            
        except oracledb.DatabaseError as e:
            error_obj, = e.args
            self.logger.warning(
                f"{table_name}: Batch insert failed: "
                f"{error_obj.message}. Trying row-by-row..."
            )
            
            # Fall back to row-by-row insertion
            return self._insert_row_by_row(
                table_name, operation, data_rows, *args, **kwargs
            )
        
        except Exception as e:
            self.logger.exception(
                f"{table_name}: Unexpected error during insert: {e}"
            )
            self._log_error(
                "staging_insert",
                table_name,
                str(e),
                {"row_count": len(data_rows)}
            )
            raise StagingInsertError(
                f"Failed to insert into {table_name}: {e}"
            ) from e
    
    def _insert_row_by_row(
        self,
        table_name: str,
        operation: Callable,
        data_rows: List,
        *args,
        **kwargs
    ) -> tuple[int, int]:
        """
        Insert rows one at a time, tracking errors.
        
        Returns:
            Tuple of (successful_count, error_count)
        """
        success_count = 0
        error_count = 0
        
        for idx, row in enumerate(data_rows):
            try:
                operation([row], *args, **kwargs)
                success_count += 1
                
            except Exception as e:
                error_count += 1
                
                # Log detailed error for first few failures
                if error_count <= 5:
                    self.logger.error(
                        f"{table_name}: Row {idx + 1} failed: {e}"
                    )
                    self._log_error(
                        "staging_insert",
                        table_name,
                        str(e),
                        {"row_index": idx, "row_data": str(row)[:200]}
                    )
        
        if error_count > 5:
            self.logger.error(
                f"{table_name}: ... and {error_count - 5} more errors"
            )
        
        self.logger.info(
            f"{table_name}: Inserted {success_count:,} records, "
            f"{error_count:,} errors"
        )
        
        return success_count, error_count
    
    def handle_merge_operation(
        self,
        procedure_name: str,
        db_connector,
        table_name: str = None
    ) -> dict:
        """
        Handle stored procedure merge with error handling.
        
        Args:
            procedure_name: Name of stored procedure
            db_connector: Database connector with cursor
            table_name: Optional table name for logging
            
        Returns:
            Dict with merge results {matched, inserted, updated, errors}
        """
        display_name = table_name or procedure_name
        
        try:
            # Execute stored procedure
            self.logger.info(f"{display_name}: Running merge...")
            
            db_connector.cursor.execute(
                f"BEGIN {procedure_name}; END;"
            )
            
            # Get row count affected
            row_count = db_connector.cursor.rowcount
            
            db_connector.connection.commit()
            
            self.logger.info(
                f"{display_name}: Merge completed ({row_count:,} rows affected)"
            )
            
            return {
                "matched": row_count,
                "inserted": 0,  # Can't distinguish without procedure changes
                "updated": 0,
                "errors": 0,
                "success": True
            }
            
        except oracledb.DatabaseError as e:
            error_obj, = e.args
            error_msg = error_obj.message
            
            self.logger.error(
                f"{display_name}: Merge failed: {error_msg}"
            )
            
            self._log_error(
                "merge_operation",
                procedure_name,
                error_msg,
                {"table_name": table_name}
            )
            
            db_connector.connection.rollback()
            
            return {
                "matched": 0,
                "inserted": 0,
                "updated": 0,
                "errors": 1,
                "success": False,
                "error_message": error_msg
            }
            
        except Exception as e:
            self.logger.exception(
                f"{display_name}: Unexpected merge error: {e}"
            )
            
            self._log_error(
                "merge_operation",
                procedure_name,
                str(e),
                {"table_name": table_name}
            )
            
            db_connector.connection.rollback()
            
            raise MergeOperationError(
                f"Failed to merge {display_name}: {e}"
            ) from e
    
    def handle_validation(
        self,
        validation_func: Callable,
        *args,
        **kwargs
    ) -> tuple[bool, List[str]]:
        """
        Handle validation with error handling.
        
        Args:
            validation_func: Validation function to execute
            *args, **kwargs: Args for validation function
            
        Returns:
            Tuple of (passed: bool, issues: List[str])
        """
        try:
            return validation_func(*args, **kwargs)
            
        except Exception as e:
            self.logger.exception(f"Validation error: {e}")
            self._log_error(
                "validation",
                str(validation_func),
                str(e),
                {"args": str(args)[:200]}
            )
            return False, [f"Validation exception: {str(e)}"]
    
    def _log_error(
        self,
        error_type: str,
        context: str,
        message: str,
        metadata: dict = None
    ):
        """Log error to internal error log."""
        error_entry = {
            "type": error_type,
            "context": context,
            "message": message,
            "metadata": metadata or {}
        }
        self.error_log.append(error_entry)
    
    def get_error_summary(self) -> dict:
        """Get summary of all errors."""
        error_counts = {}
        for error in self.error_log:
            error_type = error["type"]
            error_counts[error_type] = error_counts.get(error_type, 0) + 1
        
        return {
            "total_errors": len(self.error_log),
            "by_type": error_counts,
            "errors": self.error_log
        }
    
    def clear_errors(self):
        """Clear error log."""
        self.error_log.clear()


def with_error_handling(error_handler: ErrorHandler):
    """
    Decorator for automatic error handling.
    
    Args:
        error_handler: ErrorHandler instance
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            try:
                return func(*args, **kwargs)
            except DataImportError:
                # Re-raise known errors
                raise
            except Exception as e:
                error_handler.logger.exception(
                    f"Unexpected error in {func.__name__}: {e}"
                )
                raise DataImportError(
                    f"Error in {func.__name__}: {e}"
                ) from e
        return wrapper
    return decorator
