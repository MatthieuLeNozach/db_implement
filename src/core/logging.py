"""
Logging configuration module for Purchase Order Processing System.

This module provides clean, configurable logging setup for both technical
and business users of the system.
"""

import logging
import sys
import time
from contextlib import contextmanager
from typing import Optional, Generator
from enum import Enum


class LogLevel(Enum):
    """Enumeration of available logging levels."""

    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL


class UserMode(Enum):
    """User mode enumeration for different logging configurations."""

    TECHNICAL = "technical"
    BUSINESS = "business"
    SILENT = "silent"


class LoggingConfig:
    """
    Centralized logging configuration class.

    Provides different logging setups for different types of users:
    - Technical users: Detailed logs with timestamps and module names
    - Business users: Clean, minimal logs focused on results
    - Silent mode: Only critical errors
    """

    # Predefined configurations for different user types
    CONFIGURATIONS = {
        UserMode.TECHNICAL: {
            "level": LogLevel.INFO,
            "format": "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
            "description": "🔧 Technical mode: Detailed logging enabled",
        },
        UserMode.BUSINESS: {
            "level": LogLevel.WARNING,
            "format": "%(levelname)s: %(message)s",
            "description": "👔 Business user mode: Clean, minimal logging",
        },
        UserMode.SILENT: {
            "level": LogLevel.ERROR,
            "format": "ERROR: %(message)s",
            "description": "🔇 Silent mode: Only critical errors shown",
        },
    }

    def __init__(self, mode: UserMode = UserMode.BUSINESS):
        """
        Initialize logging configuration.

        Args:
            mode: User mode (TECHNICAL, BUSINESS, or SILENT)
        """
        self.mode = mode
        self.config = self.CONFIGURATIONS[mode]
        self._is_configured = False

    def setup_logging(self, force_reconfigure: bool = True) -> logging.Logger:
        """
        Set up logging based on the configured mode.

        Args:
            force_reconfigure: Whether to force reconfiguration of existing loggers

        Returns:
            Configured logger instance
        """
        if force_reconfigure:
            # Clear existing handlers
            for handler in logging.root.handlers[:]:
                logging.root.removeHandler(handler)

        # Configure basic logging
        logging.basicConfig(
            level=self.config["level"].value,
            format=self.config["format"],
            handlers=[logging.StreamHandler(sys.stdout)],
            force=force_reconfigure,
        )

        # Suppress noisy third-party loggers
        self._suppress_noisy_loggers()

        # Create main logger
        logger = logging.getLogger("purchase_order_processor")
        logger.setLevel(self.config["level"].value)

        # Show mode description
        if self.config["level"].value <= LogLevel.INFO.value:
            logger.info(self.config["description"])
        else:
            print(self.config["description"])

        self._is_configured = True
        return logger

    def _suppress_noisy_loggers(self):
        """Suppress verbose logging from third-party libraries."""
        noisy_loggers = [
            "pdfminer",
            "pdfplumber",
            "PIL",
            "matplotlib",
            "urllib3",
            "requests",
        ]

        for logger_name in noisy_loggers:
            logging.getLogger(logger_name).setLevel(logging.CRITICAL)

    def get_logger(self, name: str) -> logging.Logger:
        """
        Get a logger with the configured settings.

        Args:
            name: Logger name (usually __name__)

        Returns:
            Configured logger instance
        """
        if not self._is_configured:
            raise RuntimeError("Logging not configured. Call setup_logging() first.")

        logger = logging.getLogger(name)
        logger.setLevel(self.config["level"].value)
        return logger

    @classmethod
    def quick_setup(cls, mode: UserMode = UserMode.BUSINESS) -> logging.Logger:
        """
        Quick setup method for immediate use.

        Args:
            mode: User mode for logging configuration

        Returns:
            Configured logger ready to use
        """
        config = cls(mode)
        return config.setup_logging()


# Convenience functions for easy import and use
def setup_technical_logging() -> logging.Logger:
    """Set up logging for technical users with detailed output."""
    return LoggingConfig.quick_setup(UserMode.TECHNICAL)


def setup_business_logging() -> logging.Logger:
    """Set up logging for business users with clean, minimal output."""
    return LoggingConfig.quick_setup(UserMode.BUSINESS)


def setup_silent_logging() -> logging.Logger:
    """Set up logging in silent mode (errors only)."""
    return LoggingConfig.quick_setup(UserMode.SILENT)


# Default configuration function
def setup_logging(verbose: bool = False) -> logging.Logger:
    """
    Set up logging with simple verbose/quiet toggle.

    Args:
        verbose: If True, use technical mode; if False, use business mode

    Returns:
        Configured logger
    """
    mode = UserMode.TECHNICAL if verbose else UserMode.BUSINESS
    return LoggingConfig.quick_setup(mode)


class ExecutionTimer:
    """
    Context manager and utility class for timing code execution.

    Can be used as a context manager or as a decorator.
    Integrates with the logging system to show timing information.
    """

    def __init__(
        self,
        name: str = "Operation",
        logger: Optional[logging.Logger] = None,
        show_start: bool = True,
        show_end: bool = True,
    ):
        """
        Initialize the timer.

        Args:
            name: Name of the operation being timed
            logger: Logger instance to use (if None, uses print)
            show_start: Whether to show start message
            show_end: Whether to show completion message
        """
        self.name = name
        self.logger = logger
        self.show_start = show_start
        self.show_end = show_end
        self.start_time = None
        self.end_time = None

    def __enter__(self):
        """Enter the context manager."""
        self.start_time = time.time()
        if self.show_start:
            self._log(f"⏱️  Starting: {self.name}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager."""
        self.end_time = time.time()
        elapsed = self.end_time - self.start_time

        if exc_type is None:
            # Success
            if self.show_end:
                self._log(
                    f"✅ Completed: {self.name} in {self._format_duration(elapsed)}"
                )
        else:
            # Error occurred
            self._log(f"❌ Failed: {self.name} after {self._format_duration(elapsed)}")

    def _log(self, message: str):
        """Log a message using logger or print."""
        if self.logger:
            self.logger.info(message)
        else:
            print(message)

    def _format_duration(self, seconds: float) -> str:
        """Format duration in a human-readable way."""
        if seconds < 1:
            return f"{seconds * 1000:.0f}ms"
        elif seconds < 60:
            return f"{seconds:.2f}s"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            remaining_seconds = seconds % 60
            return f"{minutes}m {remaining_seconds:.1f}s"
        else:
            hours = int(seconds // 3600)
            remaining_minutes = int((seconds % 3600) // 60)
            return f"{hours}h {remaining_minutes}m"

    @property
    def elapsed_time(self) -> Optional[float]:
        """Get elapsed time if timing is complete."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None


@contextmanager
def time_operation(
    name: str,
    logger: Optional[logging.Logger] = None,
    show_start: bool = True,
    show_end: bool = True,
) -> Generator[ExecutionTimer, None, None]:
    """
    Context manager for timing operations.

    Args:
        name: Name of the operation
        logger: Logger to use for output
        show_start: Whether to show start message
        show_end: Whether to show completion message

    Yields:
        ExecutionTimer instance

    Example:
        with time_operation("Processing files", logger) as timer:
            # Your code here
            pass
        # Automatically shows completion time
    """
    timer = ExecutionTimer(name, logger, show_start, show_end)
    with timer:
        yield timer


# Export commonly used items
__all__ = [
    "LoggingConfig",
    "UserMode",
    "LogLevel",
    "ExecutionTimer",
    "time_operation",
    "setup_technical_logging",
    "setup_business_logging",
    "setup_silent_logging",
    "setup_logging",
]
