# src/core/types.py

"""
Type definitions and data classes for the application.

Provides structured data types for passing data between components,
replacing ad-hoc dictionaries with typed objects.
"""

from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, List, Any
from pathlib import Path
from datetime import datetime
import pandas as pd

from .constants import ValidationIssueType, ProcessingStatus


@dataclass
class ValidationIssue:
    """Represents a single validation issue found during processing."""
    
    type: ValidationIssueType
    line_number: int
    sku: Optional[str] = None
    description: Optional[str] = None
    message: str = ""
    severity: str = "warning"  # warning, error, info
    corrected: bool = False
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "type": self.type.value if isinstance(self.type, ValidationIssueType) else self.type,
            "line_number": self.line_number,
            "sku": self.sku,
            "description": self.description,
            "message": self.message,
            "severity": self.severity,
            "corrected": self.corrected
        }


@dataclass
class ProcessingStatistics:
    """Statistics about a processing operation."""
    
    total_lines: int = 0
    valid_lines: int = 0
    invalid_lines: int = 0
    corrected_lines: int = 0
    unauthorized_products: int = 0
    warnings: int = 0
    errors: int = 0
    processing_time: float = 0.0
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return asdict(self)
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage."""
        if self.total_lines == 0:
            return 0.0
        return (self.valid_lines / self.total_lines) * 100


@dataclass
class HeaderInfo:
    """Information extracted from document header."""
    
    customer_name: Optional[str] = None
    customer_code: Optional[str] = None
    po_number: Optional[str] = None
    delivery_date: Optional[str] = None
    entity_code: Optional[str] = None
    entity_name: Optional[str] = None
    order_date: Optional[str] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return asdict(self)
    
    def is_complete(self) -> bool:
        """Check if all required fields are present."""
        return bool(self.customer_name and self.po_number)


@dataclass
class ProcessingResult:
    """
    Complete result of processing a purchase order file.
    
    This is the main data structure returned by the processing pipeline.
    """
    
    # Basic info
    file_path: Path
    file_name: str
    customer_format: str
    status: ProcessingStatus
    
    # Processing outputs
    success: bool
    processed_df: Optional[pd.DataFrame] = None
    clean_df: Optional[pd.DataFrame] = None
    faulty_df: Optional[pd.DataFrame] = None
    
    # Metadata
    header_info: Optional[HeaderInfo] = None
    stats: Optional[ProcessingStatistics] = None
    issues: List[ValidationIssue] = field(default_factory=list)
    
    # Error handling
    error_message: Optional[str] = None
    error_traceback: Optional[str] = None
    
    # Timestamps
    started_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary (JSON-serializable)."""
        return {
            "file_path": str(self.file_path),
            "file_name": self.file_name,
            "customer_format": self.customer_format,
            "status": self.status.value if isinstance(self.status, ProcessingStatus) else self.status,
            "success": self.success,
            "header_info": self.header_info.to_dict() if self.header_info else None,
            "stats": self.stats.to_dict() if self.stats else None,
            "issues": [issue.to_dict() for issue in self.issues],
            "error_message": self.error_message,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "processing_time": self.stats.processing_time if self.stats else 0.0,
        }
    
    @property
    def lines(self) -> List[Dict]:
        """Get extracted lines as list of dictionaries."""
        if self.processed_df is not None:
            return self.processed_df.to_dict('records')
        return []
    
    def mark_completed(self):
        """Mark the result as completed with current timestamp."""
        self.completed_at = datetime.now()
        if self.stats and self.started_at:
            duration = (self.completed_at - self.started_at).total_seconds()
            self.stats.processing_time = duration


@dataclass
class BatchProcessingResult:
    """Result of processing multiple files in batch."""
    
    customer_format: str
    total_files: int
    successful: int
    failed: int
    results: List[ProcessingResult] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "customer_format": self.customer_format,
            "total_files": self.total_files,
            "successful": self.successful,
            "failed": self.failed,
            "total_lines": sum(len(r.lines) for r in self.results if r.success),
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "files": [
                {
                    "name": r.file_name,
                    "success": r.success,
                    "lines": len(r.lines),
                    "error": r.error_message if not r.success else None
                }
                for r in self.results
            ]
        }
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage."""
        if self.total_files == 0:
            return 0.0
        return (self.successful / self.total_files) * 100
    
    def mark_completed(self):
        """Mark batch as completed."""
        self.completed_at = datetime.now()


@dataclass
class CustomerAssignmentRule:
    """Rule for assigning customers to Mercuriales."""
    
    id: Optional[int]
    field: str
    operator: str
    value: str
    mercuriale_name: str
    priority: int = 99
    required: bool = False
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return asdict(self)


@dataclass
class FormatConfiguration:
    """Configuration for extracting data from a specific customer format."""
    
    format_name: str
    po_number_fuzzy: str
    delivery_date_regex: str
    entity_code_regex: str
    entity_name_regex: str
    header_fuzzy: str
    skip_footer_keywords: str
    min_columns: int
    fuzzy_threshold: float
    column_description: str
    column_sku: str
    column_quantity: str
    column_unit: str
    customer_matching_strategies: List[str] = field(default_factory=list)
    company_name_patterns: List[str] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return asdict(self)


@dataclass
class DatabaseStatistics:
    """Statistics about database contents."""
    
    products: int = 0
    customers: int = 0
    mercuriales: int = 0
    orders: int = 0
    last_update: Optional[datetime] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        result = asdict(self)
        if self.last_update:
            result['last_update'] = self.last_update.isoformat()
        return result