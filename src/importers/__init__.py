# src/importers/__init__.py

"""
Importers package v2.0

Simplified, unified import system with better separation of concerns.
"""

from .import_manager import ImportManager
from .product_importer import ProductImporter
from .customer_importer import CustomerImporter
from .assignment_importer import AssignmentImporter
from .mercuriale_importer import MercurialeImporter
from .base_importer import BaseImporter, CSVReader, HeaderNormalizer, SKUNormalizer

__all__ = [
    "ImportManager",
    "ProductImporter",
    "CustomerImporter",
    "AssignmentImporter",
    "MercurialeImporter",
    "BaseImporter",
    "CSVReader",
    "HeaderNormalizer",
    "SKUNormalizer",
]

__version__ = "2.0.0"
