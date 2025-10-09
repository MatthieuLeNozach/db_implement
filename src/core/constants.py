# src/core/constants.py

"""
Application-wide constants and enumerations.

Defines standard column names, validation types, and other immutable values
used throughout the application.
"""

from enum import Enum
from typing import List


class FileFormat(Enum):
    """Supported file formats."""
    PDF = "pdf"
    CSV = "csv"
    XLSX = "xlsx"
    XLS = "xls"


class ValidationIssueType(Enum):
    """Types of validation issues that can occur during processing."""
    
    # Product issues
    UNAUTHORIZED_PRODUCT = "unauthorized_product"
    PRODUCT_NOT_FOUND = "product_not_found"
    INVALID_SKU = "invalid_sku"
    
    # Description issues
    DESCRIPTION_CORRECTED = "description_corrected"
    DESCRIPTION_MISSING = "description_missing"
    
    # Quantity issues
    QUANTITY_CORRECTED = "quantity_corrected"
    QUANTITY_INVALID = "quantity_invalid"
    QUANTITY_MISSING = "quantity_missing"
    
    # Unit issues
    CARTON_UNIT = "carton_unit"
    UNIT_MISMATCH = "unit_mismatch"
    UNIT_MISSING = "unit_missing"
    
    # General issues
    DUPLICATE_LINE = "duplicate_line"
    INCOMPLETE_DATA = "incomplete_data"


class ProcessingStatus(Enum):
    """Status of a processing operation."""
    PENDING = "pending"
    PROCESSING = "processing"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL_SUCCESS = "partial_success"


class MatchingOperator(Enum):
    """Operators for customer assignment rules."""
    EQUALS = "equals"
    CONTAINS = "contains"
    NOT_EQUALS = "not_equals"
    STARTS_WITH = "startswith"
    ENDS_WITH = "endswith"
    REGEX = "regex"


class StandardColumns:
    """
    Standard column names used across the application.
    
    These are the normalized column names used internally,
    regardless of the input format.
    """
    
    # Purchase Order columns
    TYPE = "Type"
    SKU = "N°"
    DESCRIPTION = "Description"
    COMMENT = "Comment"
    QUANTITY = "Quantité (pièce)"
    UNIT = "Unité"
    
    # Customer columns
    CUSTOMER_NUMBER = "customer_number"
    CUSTOMER_NAME = "name"
    CUSTOMER_NAME2 = "name2"
    DELIVERY_ZONE = "delivery_zone"
    POSTAL_CODE = "postal_code"
    CITY = "city"
    REQUIRED_RANGE = "required_range"
    CLIENT_TYPE = "client_type"
    SUB_CLIENT_TYPE = "sub_client_type"
    
    # Product columns
    PRODUCT_SKU = "sku"
    PRODUCT_DESCRIPTION = "description"
    SUPPLIER_NUMBER = "supplier_number"
    PRODUCT_FAMILY = "product_family"
    SUB_FAMILY = "sub_family"
    SUB_SUB_FAMILY = "sub_sub_family"
    SUB_SUB_SUB_FAMILY = "sub_sub_sub_family"
    BRAND = "brand"
    
    @classmethod
    def get_po_columns(cls) -> List[str]:
        """Get list of standard PO columns."""
        return [cls.TYPE, cls.SKU, cls.DESCRIPTION, cls.COMMENT, cls.QUANTITY, cls.UNIT]
    
    @classmethod
    def get_customer_columns(cls) -> List[str]:
        """Get list of customer columns."""
        return [
            cls.CUSTOMER_NUMBER, cls.CUSTOMER_NAME, cls.CUSTOMER_NAME2,
            cls.DELIVERY_ZONE, cls.POSTAL_CODE, cls.CITY,
            cls.REQUIRED_RANGE, cls.CLIENT_TYPE, cls.SUB_CLIENT_TYPE
        ]
    
    @classmethod
    def get_product_columns(cls) -> List[str]:
        """Get list of product columns."""
        return [
            cls.PRODUCT_SKU, cls.PRODUCT_DESCRIPTION, cls.SUPPLIER_NUMBER,
            cls.PRODUCT_FAMILY, cls.SUB_FAMILY, cls.SUB_SUB_FAMILY,
            cls.SUB_SUB_SUB_FAMILY, cls.BRAND
        ]


class ErrorMessages:
    """Standard error messages."""
    
    # File errors
    FILE_NOT_FOUND = "File not found: {path}"
    FILE_TOO_LARGE = "File exceeds maximum size of {max_size}MB"
    INVALID_FILE_FORMAT = "Invalid file format. Expected: {expected}"
    FILE_READ_ERROR = "Error reading file: {error}"
    
    # Processing errors
    EXTRACTION_FAILED = "Failed to extract data from file: {error}"
    VALIDATION_FAILED = "Validation failed: {error}"
    DATABASE_ERROR = "Database operation failed: {error}"
    
    # Configuration errors
    MISSING_CONFIG = "Missing configuration: {config_name}"
    INVALID_CONFIG = "Invalid configuration value for {config_name}: {value}"
    
    # Customer errors
    CUSTOMER_NOT_FOUND = "Customer not found: {customer_id}"
    INVALID_CUSTOMER_FORMAT = "Invalid customer format: {format}"
    
    # Product errors
    PRODUCT_NOT_FOUND = "Product not found: SKU {sku}"
    UNAUTHORIZED_PRODUCT = "Product {sku} not authorized for customer {customer}"
    
    @classmethod
    def format(cls, message: str, **kwargs) -> str:
        """Format error message with parameters."""
        return message.format(**kwargs)


class SuccessMessages:
    """Standard success messages."""
    
    FILE_PROCESSED = "✅ Successfully processed: {filename}"
    LINES_EXTRACTED = "📦 Extracted {count} lines"
    DATABASE_SAVED = "💾 Saved to database"
    BATCH_COMPLETE = "✅ Batch processing complete: {success}/{total} files"
    
    @classmethod
    def format(cls, message: str, **kwargs) -> str:
        """Format success message with parameters."""
        return message.format(**kwargs)


class Limits:
    """Application limits and constraints."""
    
    # File size limits (in bytes)
    MAX_FILE_SIZE = 16 * 1024 * 1024  # 16MB
    MAX_UPLOAD_SIZE = 50 * 1024 * 1024  # 50MB for batch
    
    # Processing limits
    MAX_LINES_PER_ORDER = 1000
    MAX_BATCH_FILES = 100
    MAX_CONCURRENT_PROCESSES = 4
    
    # Timeout limits (in seconds)
    PDF_PROCESSING_TIMEOUT = 60
    DATABASE_QUERY_TIMEOUT = 30
    
    # Fuzzy matching thresholds (0-100)
    MIN_FUZZY_THRESHOLD = 60
    DEFAULT_FUZZY_THRESHOLD = 80
    MAX_FUZZY_THRESHOLD = 95
    
    # String length limits
    MAX_SKU_LENGTH = 50
    MAX_DESCRIPTION_LENGTH = 500
    MAX_CUSTOMER_NAME_LENGTH = 200


class DefaultValues:
    """Default values for various fields."""
    
    # Processing defaults
    DEFAULT_UNIT = "pièce"
    DEFAULT_QUANTITY = 1
    DEFAULT_PRIORITY = 99
    
    # Mercuriale defaults
    DEFAULT_MERCURIALE = "mercuriale_medelys"
    
    # Fuzzy matching defaults
    DEFAULT_FUZZY_THRESHOLD = 80.0
    DEFAULT_SKU_THRESHOLD = 85.0


# Backward compatibility aliases
class BCColumns(StandardColumns):
    """Alias for backward compatibility."""
    pass


class ValidationIssue(ValidationIssueType):
    """Alias for backward compatibility."""
    pass


# Export commonly used constants
__all__ = [
    "FileFormat",
    "ValidationIssueType",
    "ProcessingStatus",
    "MatchingOperator",
    "StandardColumns",
    "ErrorMessages",
    "SuccessMessages",
    "Limits",
    "DefaultValues",
    # Backward compatibility
    "BCColumns",
    "ValidationIssue",
]