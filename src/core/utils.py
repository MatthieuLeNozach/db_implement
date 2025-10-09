# src/core/utils.py

"""
Utility functions used across the application.

Provides helper functions for file handling, validation, formatting, etc.
"""

import os
import tempfile
import shutil
from pathlib import Path
from typing import Optional, Tuple, List
from werkzeug.utils import secure_filename
from werkzeug.datastructures import FileStorage

from .constants import FileFormat, Limits
from .exceptions import InvalidFileFormatError, FileTooLargeError


class FileValidator:
    """Validates uploaded files."""
    
    ALLOWED_EXTENSIONS = {FileFormat.PDF.value}
    
    @classmethod
    def is_allowed_file(cls, filename: str) -> bool:
        """
        Check if file extension is allowed.
        
        Args:
            filename: Name of the file
            
        Returns:
            True if file type is allowed
        """
        return '.' in filename and \
               filename.rsplit