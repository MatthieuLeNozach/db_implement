# src/core/exceptions.py

"""
Custom exceptions for the application.

Provides specific exception types for different error scenarios,
making error handling more precise and informative.
"""


class AppException(Exception):
    """Base exception for all application errors."""
    
    def __init__(self, message: str, details: dict = None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)
    
    def to_dict(self) -> dict:
        """Convert exception to dictionary for JSON responses."""
        return {
            "error": self.__class__.__name__,
            "message": self.message,
            "details": self.details
        }


# File-related exceptions
class FileError(AppException):
    """Base class for file-related errors."""
    pass


class FileNotFoundError(FileError):
    """File not found error."""
    pass


class InvalidFileFormatError(FileError):
    """Invalid file format error."""
    pass


class FileTooLargeError(FileError):
    """File exceeds size limit."""
    pass


class FileReadError(FileError):
    """Error reading file."""
    pass


# Processing exceptions
class ProcessingError(AppException):
    """Base class for processing errors."""
    pass


class ExtractionError(ProcessingError):
    """Error during data extraction."""
    pass


class ValidationError(ProcessingError):
    """Error during data validation."""
    pass


class TransformationError(ProcessingError):
    """Error during data transformation."""
    pass


# Database exceptions
class DatabaseError(AppException):
    """Base class for database errors."""
    pass


class RecordNotFoundError(DatabaseError):
    """Requested record not found in database."""
    pass


class DuplicateRecordError(DatabaseError):
    """Attempted to create duplicate record."""
    pass


class DatabaseConnectionError(DatabaseError):
    """Error connecting to database."""
    pass


# Configuration exceptions
class ConfigurationError(AppException):
    """Base class for configuration errors."""
    pass


class MissingConfigError(ConfigurationError):
    """Required configuration is missing."""
    pass


class InvalidConfigError(ConfigurationError):
    """Configuration value is invalid."""
    pass


# Business logic exceptions
class BusinessRuleError(AppException):
    """Base class for business rule violations."""
    pass


class UnauthorizedProductError(BusinessRuleError):
    """Product not authorized for customer."""
    pass


class CustomerNotFoundError(BusinessRuleError):
    """Customer not found."""
    pass


class MercurialeNotFoundError(BusinessRuleError):
    """Mercuriale not found."""
    pass


# Authentication exceptions
class AuthenticationError(AppException):
    """Base class for authentication errors."""
    pass


class InvalidCredentialsError(AuthenticationError):
    """Invalid username or password."""
    pass


class SessionExpiredError(AuthenticationError):
    """User session has expired."""
    pass


class UnauthorizedAccessError(AuthenticationError):
    """User not authorized to access resource."""
    pass