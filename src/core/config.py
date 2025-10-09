# src/core/config.py

"""
Application configuration management.

Loads settings from environment variables with sensible defaults.
Provides both database and application-level configuration.
"""

import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class PathConfig:
    """File and directory path configuration."""
    
    # Project root
    PROJECT_ROOT = Path(__file__).parent.parent.parent
    
    # Data directories
    DATA_DIR = PROJECT_ROOT / "db_files"
    RULES_DIR = DATA_DIR / "rules"
    MERCURIALES_DIR = DATA_DIR / "mercuriales"
    UPLOAD_DIR = PROJECT_ROOT / "uploads"
    
    # Static files
    STATIC_DIR = PROJECT_ROOT / "src" / "static"
    CSS_DIR = STATIC_DIR / "css"
    JS_DIR = STATIC_DIR / "js"
    
    # CSV file paths
    PRODUCT_CSV_PATH = os.getenv(
        "PRODUCT_CSV_PATH",
        str(DATA_DIR / "products.csv")
    )
    CUSTOMER_CSV_PATH = os.getenv(
        "CUSTOMER_CSV_PATH",
        str(DATA_DIR / "customers.csv")
    )
    ASSIGNMENT_RULES_CSV_PATH = os.getenv(
        "ASSIGNMENT_RULES_CSV_PATH",
        str(RULES_DIR / "assignment_conditions.csv")
    )
    FORMAT_CONFIG_CSV_PATH = os.getenv(
        "FORMAT_CONFIG_CSV_PATH",
        str(RULES_DIR / "format_config.csv")
    )
    EXTRACTION_RULES_CSV_PATH = os.getenv(
        "EXTRACTION_RULES_CSV_PATH",
        str(RULES_DIR / "extraction_rules.csv")
    )
    
    # Purchase order directory
    PO_DIRECTORY = Path(os.getenv("PO_DIRECTORY", PROJECT_ROOT / "purchase_orders"))
    
    @classmethod
    def ensure_directories(cls):
        """Create necessary directories if they don't exist."""
        for attr_name in dir(cls):
            attr = getattr(cls, attr_name)
            if isinstance(attr, Path) and attr_name.endswith("_DIR"):
                attr.mkdir(parents=True, exist_ok=True)
        
        # Add static directories
        cls.STATIC_DIR.mkdir(parents=True, exist_ok=True)
        cls.CSS_DIR.mkdir(parents=True, exist_ok=True)
        cls.JS_DIR.mkdir(parents=True, exist_ok=True)


class DatabaseConfig:
    """Database connection and settings."""
    
    DATABASE_URL = os.getenv(
        "DATABASE_URL",
        "sqlite:///contract_data.db"
    )
    
    # Database engine options
    ECHO_SQL = os.getenv("DB_ECHO", "false").lower() == "true"
    POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "5"))
    MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "10"))
    
    @classmethod
    def get_engine_options(cls) -> dict:
        """Get SQLAlchemy engine options."""
        options = {
            "echo": cls.ECHO_SQL,
            "future": True,
        }
        
        # Only add pooling options for non-SQLite databases
        if not cls.DATABASE_URL.startswith("sqlite"):
            options.update({
                "pool_size": cls.POOL_SIZE,
                "max_overflow": cls.MAX_OVERFLOW,
                "pool_pre_ping": True,
            })
        
        return options


class AppConfig:
    """Application-level configuration."""
    
    # Environment
    ENV = os.getenv("FLASK_ENV", "production")
    DEBUG = os.getenv("DEBUG", "false").lower() == "true"
    
    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    VERBOSE = os.getenv("VERBOSE", "false").lower() == "true"
    
    # Security
    SECRET_KEY = os.getenv("SECRET_KEY", "change-me-in-production")
    AUTH_ENABLED = os.getenv("AUTH_ENABLED", "true").lower() == "true"
    DEFAULT_USER_PWD = os.getenv("DEFAULT_USER_PWD", "admin123")
    
    # File upload settings
    MAX_CONTENT_LENGTH = int(os.getenv("MAX_UPLOAD_SIZE", 16 * 1024 * 1024))  # 16MB default
    ALLOWED_EXTENSIONS = {"pdf"}
    
    # Session settings
    SESSION_TYPE = "filesystem"
    SESSION_PERMANENT = False
    SESSION_USE_SIGNER = True
    
    @classmethod
    def validate(cls):
        """Validate critical configuration."""
        if cls.ENV == "production" and cls.SECRET_KEY == "change-me-in-production":
            raise ValueError(
                "SECRET_KEY must be set in production environment. "
                "Set the SECRET_KEY environment variable."
            )
        
        if cls.AUTH_ENABLED and cls.DEFAULT_USER_PWD == "admin123":
            import warnings
            warnings.warn(
                "Using default password 'admin123'. "
                "Set DEFAULT_USER_PWD environment variable for security."
            )


class ProcessingConfig:
    """Purchase order processing configuration."""
    
    # PDF processing
    PDF_DPI = int(os.getenv("PDF_DPI", "300"))
    PDF_TIMEOUT = int(os.getenv("PDF_TIMEOUT", "60"))  # seconds
    
    # Fuzzy matching thresholds
    FUZZY_THRESHOLD = float(os.getenv("FUZZY_THRESHOLD", "80.0"))
    SKU_MATCH_THRESHOLD = float(os.getenv("SKU_MATCH_THRESHOLD", "85.0"))
    
    # Processing limits
    MAX_LINES_PER_ORDER = int(os.getenv("MAX_LINES_PER_ORDER", "1000"))
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
    
    # Validation rules
    VALIDATE_SKUS = os.getenv("VALIDATE_SKUS", "true").lower() == "true"
    VALIDATE_QUANTITIES = os.getenv("VALIDATE_QUANTITIES", "true").lower() == "true"
    AUTO_CORRECT = os.getenv("AUTO_CORRECT", "true").lower() == "true"


class Config:
    """
    Unified configuration class combining all config sections.
    
    Usage:
        from src.core.config import Config
        
        db_url = Config.database.DATABASE_URL
        product_path = Config.paths.PRODUCT_CSV_PATH
        log_level = Config.app.LOG_LEVEL
    """
    
    paths = PathConfig
    database = DatabaseConfig
    app = AppConfig
    processing = ProcessingConfig
    
    @classmethod
    def initialize(cls):
        """Initialize configuration and validate settings."""
        cls.paths.ensure_directories()
        cls.app.validate()
    
    @classmethod
    def get_flask_config(cls) -> dict:
        """Get configuration dict for Flask app."""
        return {
            "SECRET_KEY": cls.app.SECRET_KEY,
            "DEBUG": cls.app.DEBUG,
            "UPLOAD_FOLDER": str(cls.paths.UPLOAD_DIR),
            "MAX_CONTENT_LENGTH": cls.app.MAX_CONTENT_LENGTH,
            "SESSION_TYPE": cls.app.SESSION_TYPE,
            "SESSION_PERMANENT": cls.app.SESSION_PERMANENT,
            "SESSION_USE_SIGNER": cls.app.SESSION_USE_SIGNER,
            "AUTH_ENABLED": cls.app.AUTH_ENABLED,
            "VERBOSE": cls.app.VERBOSE,
            # Additional custom configs
            "RULES_CSV_PATH": cls.paths.EXTRACTION_RULES_CSV_PATH,
            "PO_DIRECTORY": str(cls.paths.PO_DIRECTORY),
        }
    
    @classmethod
    def summary(cls) -> str:
        """Get configuration summary for logging."""
        return f"""
Configuration Summary:
  Environment: {cls.app.ENV}
  Debug: {cls.app.DEBUG}
  Auth Enabled: {cls.app.AUTH_ENABLED}
  Database: {cls.database.DATABASE_URL}
  Upload Directory: {cls.paths.UPLOAD_DIR}
  PO Directory: {cls.paths.PO_DIRECTORY}
  Log Level: {cls.app.LOG_LEVEL}
        """.strip()


# Backward compatibility (can be removed after full migration)
PRODUCT_CSV_PATH = PathConfig.PRODUCT_CSV_PATH
CUSTOMER_CSV_PATH = PathConfig.CUSTOMER_CSV_PATH
ASSIGNMENT_RULES_CSV_PATH = PathConfig.ASSIGNMENT_RULES_CSV_PATH
DATABASE_URL = DatabaseConfig.DATABASE_URL
LOG_LEVEL = AppConfig.LOG_LEVEL