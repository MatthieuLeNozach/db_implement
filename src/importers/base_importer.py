# src/importers/base_importer.py

import logging
import pandas as pd
from typing import List, Set, Optional, Dict, Any
from pathlib import Path
from ftfy import fix_text
from unidecode import unidecode

logger = logging.getLogger(__name__)


class CSVReader:
    """Unified CSV reading with encoding/delimiter detection and normalization."""
    
    ENCODINGS = ["utf-8", "iso-8859-1", "latin-1"]
    DELIMITERS = [";", ","]
    
    @staticmethod
    def read_csv(
        path: str,
        dtype: str = "str",
        delimiter: Optional[str] = None,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """
        Read CSV with automatic encoding/delimiter detection.
        
        Args:
            path: CSV file path
            dtype: Data type for columns
            delimiter: Specific delimiter or None for auto-detect
            **kwargs: Additional pandas read_csv arguments
        
        Returns:
            DataFrame or None if reading fails
        """
        delimiters = [delimiter] if delimiter else CSVReader.DELIMITERS
        
        for delim in delimiters:
            for encoding in CSVReader.ENCODINGS:
                try:
                    df = pd.read_csv(
                        path,
                        sep=delim,
                        dtype=dtype,
                        encoding=encoding,
                        skipinitialspace=True,
                        on_bad_lines="skip",
                        **kwargs
                    )
                    if df is not None and not df.empty:
                        logger.debug(f"✅ Read {path} with delimiter='{delim}', encoding={encoding}")
                        return df
                except (UnicodeDecodeError, pd.errors.ParserError, pd.errors.EmptyDataError):
                    continue
                except Exception as e:
                    logger.debug(f"Failed reading {path} with {delim}/{encoding}: {e}")
        
        logger.error(f"❌ Could not read {path} with any encoding/delimiter combination")
        return None


class HeaderNormalizer:
    """Normalize and map CSV headers to model fields."""
    
    @staticmethod
    def normalize_header(header: str, strip_digits: bool = True) -> str:
        """
        Clean a single CSV header.
        
        Args:
            header: Raw header string
            strip_digits: Whether to remove digits (default True)
        
        Returns:
            Normalized header string
        """
        h = fix_text(str(header).strip())
        h = unidecode(h)
        h = h.replace("*", " ").replace("-", " ").replace("_", " ")
        
        if strip_digits:
            h = ''.join(c if not c.isdigit() else " " for c in h)
        
        h = ' '.join(h.split()).lower()
        return h
    
    @staticmethod
    def apply_header_mapping(
        df: pd.DataFrame,
        header_map: Dict[str, str],
        strip_digits: bool = True
    ) -> pd.DataFrame:
        """
        Apply header normalization and mapping to DataFrame.
        
        Args:
            df: Input DataFrame
            header_map: Mapping of normalized headers to model fields
            strip_digits: Whether to strip digits during normalization
        
        Returns:
            DataFrame with renamed columns
        """
        # Normalize headers
        normalized = [
            HeaderNormalizer.normalize_header(h, strip_digits)
            for h in df.columns
        ]
        
        # Map to model fields and handle duplicates
        new_columns = []
        seen = {}
        for norm_h, orig_h in zip(normalized, df.columns):
            mapped = header_map.get(norm_h, orig_h)
            if mapped in seen:
                count = seen[mapped] + 1
                mapped = f"{mapped}_{count}"
            seen[mapped] = seen.get(mapped, 0)
            new_columns.append(mapped)
        
        df.columns = new_columns
        return df


class SKUNormalizer:
    """Normalize SKU values for flexible matching."""
    
    @staticmethod
    def normalize_variants(skus: List[str]) -> Set[str]:
        """
        Generate SKU variants for matching.
        
        Creates variants by:
        - Stripping whitespace
        - Removing leading zeros
        - Zero-padding to 6 digits
        
        Args:
            skus: List of raw SKU strings
        
        Returns:
            Set of normalized SKU variants
        """
        variants = set()
        for sku in skus:
            if sku is None:
                continue
            
            s = str(sku).strip()
            if not s:
                continue
            
            # Original form
            variants.add(s)
            
            # No leading zeros
            no_zeros = s.lstrip("0")
            if no_zeros:
                variants.add(no_zeros)
            
            # Zero-padded 6-digit (common ERP format)
            if s.isdigit():
                variants.add(s.zfill(6))
        
        return variants


class BaseImporter:
    """Base class for all importers with common utilities."""
    
    def __init__(self, session):
        self.session = session
        self.csv_reader = CSVReader()
        self.header_normalizer = HeaderNormalizer()
        self.sku_normalizer = SKUNormalizer()
    
    def preview_csv(self, path: str, n_rows: int = 5) -> Optional[pd.DataFrame]:
        """Preview CSV structure for debugging."""
        df = self.csv_reader.read_csv(path, nrows=n_rows)
        if df is not None:
            print(f"\n📋 Preview of {path}:")
            print(f"Columns: {list(df.columns)}")
            print(f"Shape: {df.shape}")
            print("\nFirst few rows:")
            print(df.head())
        return df
    
    def safe_commit(self, operation_name: str):
        """Safe commit with error handling and rollback."""
        try:
            self.session.commit()
            logger.info(f"✅ {operation_name} committed successfully")
        except Exception as e:
            self.session.rollback()
            logger.error(f"❌ {operation_name} failed: {e}")
            raise
