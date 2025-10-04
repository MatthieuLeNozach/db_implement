from enum import Enum
from dataclasses import dataclass
from typing import Optional, Dict  # , List
from pathlib import Path
import pandas as pd


class BCColumns:
    TYPE = "Type"
    SKU = "N°"  # This is the fixed column name for SKU in BC format
    DESCRIPTION = "Description"
    COMMENT = "Comment"
    QUANTITY = "Quantité (pièce)"  # fixed col name for Quantity in BC format

    @classmethod
    def get_columns(cls):
        return [cls.TYPE, cls.SKU, cls.DESCRIPTION, cls.COMMENT, cls.QUANTITY]


class ValidationIssue(Enum):
    UNAUTHORIZED_PRODUCT = "unauthorized_product"
    DESCRIPTION_CORRECTED = "description_corrected"
    QUANTITY_CORRECTED = "quantity_corrected"
    CARTON_UNIT = "carton_unit"  # New validation issue type for carton units


@dataclass
class ProcessingResult:
    customer: str
    file_path: Path
    success: bool
    processed_df: Optional[pd.DataFrame] = None
    clean_df: Optional[pd.DataFrame] = None
    faulty_df: Optional[pd.DataFrame] = None
    stats: Optional[Dict] = None
    error_message: Optional[str] = None
    header_info: Optional[Dict] = None
