"""
Unified Data Import Logger and Validator

Provides consistent logging and validation for MOLO and Stellar data imports.
Tracks all stages: CSV extraction, staging insertion, merge operations, and validation.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum


class ImportStage(Enum):
    """Stages of the data import process."""
    CSV_EXTRACTION = "CSV Extraction"
    STAGING_INSERT = "Staging Insert"
    MERGE_OPERATION = "Merge to DW"
    VALIDATION = "Validation"


class ImportStatus(Enum):
    """Status of import operations."""
    SUCCESS = "✅ Success"
    WARNING = "⚠️  Warning"
    ERROR = "❌ Error"
    SKIPPED = "⏭️  Skipped"


@dataclass
class TableImportMetrics:
    """Metrics for a single table import."""
    table_name: str
    csv_file: str
    csv_row_count: int = 0
    staging_inserted: int = 0
    staging_errors: int = 0
    merge_matched: int = 0
    merge_inserted: int = 0
    merge_updated: int = 0
    merge_errors: int = 0
    validation_passed: bool = False
    validation_issues: List[str] = field(default_factory=list)
    stage_errors: Dict[ImportStage, List[str]] = field(default_factory=dict)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    def add_error(self, stage: ImportStage, error_msg: str):
        """Add an error message for a specific stage."""
        if stage not in self.stage_errors:
            self.stage_errors[stage] = []
        self.stage_errors[stage].append(error_msg)
    
    def get_duration(self) -> Optional[float]:
        """Get duration in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    def get_status(self) -> ImportStatus:
        """Determine overall status."""
        if self.stage_errors:
            if any(self.stage_errors.values()):
                return ImportStatus.ERROR
        if self.validation_issues:
            return ImportStatus.WARNING
        if self.staging_inserted == 0:
            return ImportStatus.SKIPPED
        return ImportStatus.SUCCESS


@dataclass
class SystemImportSummary:
    """Summary for an entire system (MOLO or Stellar) import."""
    system_name: str  # "MOLO" or "Stellar"
    tables: Dict[str, TableImportMetrics] = field(default_factory=dict)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    total_csv_rows: int = 0
    total_staging_inserted: int = 0
    total_merge_updates: int = 0
    total_merge_inserts: int = 0
    total_errors: int = 0
    total_warnings: int = 0
    
    def add_table(self, metrics: TableImportMetrics):
        """Add table metrics to summary."""
        self.tables[metrics.table_name] = metrics
        self.total_csv_rows += metrics.csv_row_count
        self.total_staging_inserted += metrics.staging_inserted
        self.total_merge_updates += metrics.merge_updated
        self.total_merge_inserts += metrics.merge_inserted
        
        # Count errors
        for errors in metrics.stage_errors.values():
            self.total_errors += len(errors)
        self.total_warnings += len(metrics.validation_issues)
    
    def get_duration(self) -> Optional[float]:
        """Get total duration in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    def get_success_rate(self) -> float:
        """Get success rate percentage."""
        if not self.tables:
            return 0.0
        successful = sum(1 for t in self.tables.values() 
                        if t.get_status() == ImportStatus.SUCCESS)
        return (successful / len(self.tables)) * 100


class DataImportLogger:
    """Unified logger for data import operations."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.molo_summary = SystemImportSummary(system_name="MOLO")
        self.stellar_summary = SystemImportSummary(system_name="Stellar")
        self.current_metrics: Optional[TableImportMetrics] = None
    
    def start_system_import(self, system_name: str):
        """Start tracking a system import (MOLO or Stellar)."""
        summary = self._get_summary(system_name)
        summary.start_time = datetime.now()
        
        self.logger.info("")
        self.logger.info("=" * 80)
        self.logger.info(f"{system_name.upper()} DATA IMPORT STARTED")
        self.logger.info("=" * 80)
    
    def end_system_import(self, system_name: str):
        """End tracking a system import."""
        summary = self._get_summary(system_name)
        summary.end_time = datetime.now()
        
        self._print_system_summary(summary)
    
    def start_table_import(self, system_name: str, table_name: str, 
                          csv_file: str, csv_row_count: int = 0):
        """Start tracking a table import."""
        self.current_metrics = TableImportMetrics(
            table_name=table_name,
            csv_file=csv_file,
            csv_row_count=csv_row_count,
            start_time=datetime.now()
        )
        
        self.logger.info("")
        self.logger.info(f"{'─' * 80}")
        self.logger.info(f"📊 Processing: {table_name}")
        self.logger.info(f"   Source: {csv_file}")
        if csv_row_count > 0:
            self.logger.info(f"   CSV Rows: {csv_row_count:,}")
    
    def end_table_import(self, system_name: str):
        """End tracking a table import."""
        if not self.current_metrics:
            return
        
        self.current_metrics.end_time = datetime.now()
        summary = self._get_summary(system_name)
        summary.add_table(self.current_metrics)
        
        # Log table summary
        status = self.current_metrics.get_status()
        duration = self.current_metrics.get_duration()
        
        self.logger.info(f"   {status.value}")
        self.logger.info(f"   Staging: {self.current_metrics.staging_inserted:,} inserted, "
                        f"{self.current_metrics.staging_errors:,} errors")
        
        if self.current_metrics.merge_matched > 0:
            self.logger.info(f"   Merge: {self.current_metrics.merge_updated:,} updated, "
                           f"{self.current_metrics.merge_inserted:,} inserted")
        
        if duration:
            self.logger.info(f"   Duration: {duration:.2f}s")
        
        if self.current_metrics.validation_issues:
            self.logger.warning(f"   ⚠️  Validation Issues: "
                              f"{len(self.current_metrics.validation_issues)}")
            for issue in self.current_metrics.validation_issues[:3]:
                self.logger.warning(f"      • {issue}")
        
        if self.current_metrics.stage_errors:
            for stage, errors in self.current_metrics.stage_errors.items():
                self.logger.error(f"   ❌ {stage.value} Errors: {len(errors)}")
                for error in errors[:3]:  # Show first 3 errors
                    self.logger.error(f"      • {error}")
                if len(errors) > 3:
                    self.logger.error(f"      ... and {len(errors) - 3} more")
        
        self.current_metrics = None
    
    def log_staging_insert(self, inserted_count: int, error_count: int = 0):
        """Log staging insertion results."""
        if self.current_metrics:
            self.current_metrics.staging_inserted = inserted_count
            self.current_metrics.staging_errors = error_count
    
    def log_merge_operation(self, matched: int = 0, inserted: int = 0, 
                           updated: int = 0, error_count: int = 0):
        """Log merge operation results."""
        if self.current_metrics:
            self.current_metrics.merge_matched = matched
            self.current_metrics.merge_inserted = inserted
            self.current_metrics.merge_updated = updated
            self.current_metrics.merge_errors = error_count
    
    def log_validation_result(self, passed: bool, issues: List[str] = None):
        """Log validation results."""
        if self.current_metrics:
            self.current_metrics.validation_passed = passed
            if issues:
                self.current_metrics.validation_issues.extend(issues)
    
    def add_error(self, stage: ImportStage, error_msg: str):
        """Add an error for the current table."""
        if self.current_metrics:
            self.current_metrics.add_error(stage, error_msg)
    
    def _get_summary(self, system_name: str) -> SystemImportSummary:
        """Get the appropriate summary object."""
        if system_name.upper() == "MOLO":
            return self.molo_summary
        elif system_name.upper() == "STELLAR":
            return self.stellar_summary
        else:
            raise ValueError(f"Unknown system: {system_name}")
    
    def _print_system_summary(self, summary: SystemImportSummary):
        """Print detailed summary for a system."""
        duration = summary.get_duration()
        success_rate = summary.get_success_rate()
        
        self.logger.info("")
        self.logger.info("=" * 80)
        self.logger.info(f"{summary.system_name.upper()} IMPORT SUMMARY")
        self.logger.info("=" * 80)
        self.logger.info(f"Tables Processed: {len(summary.tables)}")
        self.logger.info(f"Success Rate: {success_rate:.1f}%")
        self.logger.info(f"Total CSV Rows: {summary.total_csv_rows:,}")
        self.logger.info(f"Total Staged: {summary.total_staging_inserted:,}")
        self.logger.info(f"Total Merged: {summary.total_merge_inserts:,} inserted, "
                        f"{summary.total_merge_updates:,} updated")
        
        if duration:
            self.logger.info(f"Duration: {duration:.1f}s ({duration/60:.1f} minutes)")
        
        if summary.total_errors > 0:
            self.logger.error(f"❌ Total Errors: {summary.total_errors}")
        
        if summary.total_warnings > 0:
            self.logger.warning(f"⚠️  Total Warnings: {summary.total_warnings}")
        
        # Show failed tables
        failed_tables = [
            name for name, metrics in summary.tables.items()
            if metrics.get_status() == ImportStatus.ERROR
        ]
        if failed_tables:
            self.logger.error(f"❌ Failed Tables ({len(failed_tables)}):")
            for table in failed_tables[:10]:
                self.logger.error(f"   • {table}")
        
        # Show tables with warnings
        warning_tables = [
            name for name, metrics in summary.tables.items()
            if metrics.get_status() == ImportStatus.WARNING
        ]
        if warning_tables:
            self.logger.warning(f"⚠️  Tables with Warnings ({len(warning_tables)}):")
            for table in warning_tables[:10]:
                self.logger.warning(f"   • {table}")
        
        self.logger.info("=" * 80)
    
    def print_final_summary(self):
        """Print final summary for all systems."""
        self.logger.info("")
        self.logger.info("=" * 80)
        self.logger.info("COMPLETE DATA IMPORT SUMMARY")
        self.logger.info("=" * 80)
        
        for summary in [self.molo_summary, self.stellar_summary]:
            if summary.tables:
                success_rate = summary.get_success_rate()
                status_emoji = "✅" if summary.total_errors == 0 else "❌"
                
                self.logger.info(f"{status_emoji} {summary.system_name}:")
                self.logger.info(f"   Tables: {len(summary.tables)}")
                self.logger.info(f"   Success Rate: {success_rate:.1f}%")
                self.logger.info(f"   Records: {summary.total_staging_inserted:,} staged, "
                               f"{summary.total_merge_inserts + summary.total_merge_updates:,} merged")
                
                if summary.total_errors > 0:
                    self.logger.info(f"   Errors: {summary.total_errors}")
                if summary.total_warnings > 0:
                    self.logger.info(f"   Warnings: {summary.total_warnings}")
        
        # Overall status
        total_errors = self.molo_summary.total_errors + self.stellar_summary.total_errors
        total_warnings = self.molo_summary.total_warnings + self.stellar_summary.total_warnings
        
        self.logger.info("")
        if total_errors == 0:
            self.logger.info("✅ ALL IMPORTS COMPLETED SUCCESSFULLY")
        else:
            self.logger.error(f"❌ IMPORTS COMPLETED WITH {total_errors} ERRORS")
        
        if total_warnings > 0:
            self.logger.warning(f"⚠️  {total_warnings} validation warnings detected")
        
        self.logger.info("=" * 80)
    
    def export_summary_to_dict(self) -> Dict:
        """Export summary as dictionary for JSON logging."""
        return {
            "molo": self._summary_to_dict(self.molo_summary),
            "stellar": self._summary_to_dict(self.stellar_summary),
            "timestamp": datetime.now().isoformat()
        }
    
    def _summary_to_dict(self, summary: SystemImportSummary) -> Dict:
        """Convert summary to dictionary."""
        return {
            "system_name": summary.system_name,
            "tables_processed": len(summary.tables),
            "success_rate": summary.get_success_rate(),
            "total_csv_rows": summary.total_csv_rows,
            "total_staging_inserted": summary.total_staging_inserted,
            "total_merge_inserts": summary.total_merge_inserts,
            "total_merge_updates": summary.total_merge_updates,
            "total_errors": summary.total_errors,
            "total_warnings": summary.total_warnings,
            "duration_seconds": summary.get_duration(),
            "tables": {
                name: {
                    "csv_file": m.csv_file,
                    "csv_rows": m.csv_row_count,
                    "staging_inserted": m.staging_inserted,
                    "merge_updated": m.merge_updated,
                    "merge_inserted": m.merge_inserted,
                    "status": m.get_status().value,
                    "errors": sum(len(e) for e in m.stage_errors.values()),
                    "warnings": len(m.validation_issues)
                }
                for name, m in summary.tables.items()
            }
        }
