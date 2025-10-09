# src/importers/mercuriale_importer.py

import logging
import os
from pathlib import Path
from typing import Set
from sqlalchemy import select
from src.models.models import Mercuriale, Product, CustomerAssignmentCondition
from .base_importer import BaseImporter

logger = logging.getLogger(__name__)


class MercurialeImporter(BaseImporter):
    """
    Manages Mercuriale operations:
    1. Create Mercuriales from assignment conditions
    2. Assign products to Mercuriales from CSV files
    3. Preprocess CSV files (delimiter normalization)
    """
    
    def __init__(self, session, mercuriale_folder: str = "db_files/mercuriales/"):
        super().__init__(session)
        self.mercuriale_folder = Path(mercuriale_folder)
    
    def populate_from_conditions(self):
        """Create Mercuriale records from CustomerAssignmentCondition table."""
        logger.info("🔹 Populating Mercuriale table from assignment conditions...")
        
        conditions = self.session.query(CustomerAssignmentCondition).all()
        if not conditions:
            logger.warning("⚠️ No assignment conditions found")
            return
        
        # Extract unique Mercuriale names
        mercuriale_names = {
            c.mercuriale_name.strip()
            for c in conditions
            if c.mercuriale_name and c.mercuriale_name.strip()
        }
        
        added = 0
        for name in sorted(mercuriale_names):
            existing = self.session.query(Mercuriale).filter_by(name=name).first()
            if not existing:
                self.session.add(Mercuriale(name=name))
                added += 1
                logger.info(f"➕ Added Mercuriale: {name}")
        
        self.safe_commit(f"Mercuriale population: {added} added")
        logger.info(f"✅ Mercuriales populated: {added} added")
    
    def preprocess_csv_files(self):
        """
        Normalize mercuriale CSV files to semicolon-delimited UTF-8.
        
        Converts comma-delimited files to semicolon format for consistency.
        """
        logger.info("🔧 Preprocessing mercuriale CSV files...")
        
        if not self.mercuriale_folder.is_dir():
            logger.warning(f"⚠️ Mercuriale folder not found: {self.mercuriale_folder}")
            return
        
        converted = 0
        for csv_file in self.mercuriale_folder.glob("*.csv"):
            try:
                # Read first line to detect delimiter
                with open(csv_file, "rb") as f:
                    raw = f.read(4096)
                
                # Detect encoding
                for encoding in ["utf-8", "iso-8859-1"]:
                    try:
                        head = raw.decode(encoding, errors="strict").splitlines()[0]
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    logger.warning(f"⚠️ Could not decode {csv_file.name}")
                    continue
                
                # Convert if comma-delimited
                if "," in head and ";" not in head:
                    logger.info(f"🔄 Converting {csv_file.name} to semicolon-delimited UTF-8")
                    df = self.csv_reader.read_csv(str(csv_file), delimiter=",")
                    if df is not None:
                        df.to_csv(csv_file, sep=";", index=False, encoding="utf-8")
                        converted += 1
                        logger.info(f"✅ Converted {csv_file.name}")
            
            except Exception as e:
                logger.warning(f"⚠️ Error preprocessing {csv_file.name}: {e}")
        
        logger.info(f"✅ CSV preprocessing complete: {converted} files converted")
    
    def populate_products(self, chunk_size: int = 1000):
        """
        Assign products to Mercuriales based on CSV files in mercuriale_folder.
        
        Each CSV should contain SKUs in the first column or a column named 'sku'.
        
        Args:
            chunk_size: Batch size for database queries (prevents huge IN clauses)
        """
        logger.info("🔹 Populating Mercuriale → Product associations...")
        
        if not self.mercuriale_folder.is_dir():
            logger.warning(f"⚠️ Mercuriale folder not found: {self.mercuriale_folder}")
            return
        
        for csv_file in sorted(self.mercuriale_folder.glob("*.csv")):
            mercuriale_name = csv_file.stem
            
            # Find Mercuriale in DB
            mercuriale = self.session.query(Mercuriale).filter_by(
                name=mercuriale_name
            ).first()
            
            if not mercuriale:
                logger.info(
                    f"⚪ CSV found for '{mercuriale_name}' but no DB entry — skipping"
                )
                continue
            
            # Read CSV
            df = self.csv_reader.read_csv(str(csv_file))
            if df is None or df.empty:
                logger.warning(f"⚠️ Could not read or empty: {csv_file.name}")
                continue
            
            # Find SKU column
            sku_col = self._find_sku_column(df)
            if sku_col is None:
                logger.warning(f"⚠️ No SKU column found in {csv_file.name}")
                continue
            
            # Extract and normalize SKUs
            raw_skus = df[sku_col].dropna().astype(str).str.strip().tolist()
            if not raw_skus:
                logger.warning(f"⚠️ No SKUs found in {csv_file.name}")
                continue
            
            sku_variants = self.sku_normalizer.normalize_variants(raw_skus)
            
            logger.info(
                f"📦 {csv_file.name}: {len(raw_skus)} SKUs → "
                f"{len(sku_variants)} variants"
            )
            
            # Query products in chunks
            found_products = self._find_products_by_skus(
                list(sku_variants), chunk_size
            )
            
            # Assign to Mercuriale
            mercuriale.products = found_products
            self.session.add(mercuriale)
            
            logger.info(
                f"✅ {len(found_products)} products assigned to {mercuriale_name}"
            )
        
        self.safe_commit("Mercuriale-Product associations")
        logger.info("✅ Mercuriale product associations complete")
    
    def _find_sku_column(self, df):
        """Find SKU column in DataFrame."""
        cols_lower = [c.lower().strip() for c in df.columns]
        
        # Try common SKU column names
        for candidate in ["sku", "skus", "s n", "s/no", "n", "no", "nbr", "code"]:
            if candidate in cols_lower:
                idx = cols_lower.index(candidate)
                return df.columns[idx]
        
        # Fallback to first column
        logger.debug(f"Using first column '{df.columns[0]}' as SKU")
        return df.columns[0]
    
    def _find_products_by_skus(self, sku_variants: list, chunk_size: int) -> list:
        """
        Query products by SKU variants in chunks.
        
        Args:
            sku_variants: List of SKU variants to search
            chunk_size: Number of SKUs per query
        
        Returns:
            List of unique Product objects
        """
        found_products = []
        
        for i in range(0, len(sku_variants), chunk_size):
            chunk = sku_variants[i : i + chunk_size]
            products = self.session.query(Product).filter(Product.sku.in_(chunk)).all()
            found_products.extend(products)
        
        # Deduplicate by SKU
        unique_products = {p.sku: p for p in found_products}
        return list(unique_products.values())