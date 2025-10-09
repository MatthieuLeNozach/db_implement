# src/importers/product_importer.py

import logging
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from src.models.models import Product
from .base_importer import BaseImporter

logger = logging.getLogger(__name__)


class ProductImporter(BaseImporter):
    """Import and update products from ERP CSV exports."""
    
    # Columns to drop by position (0-indexed)
    DROP_COLUMN_INDEXES = [2, 3, 4, 5, 6, 7, 8, 9, 10, 16, 17]
    
    # Mapping cleaned headers → model attributes
    HEADER_MAP = {
        "n": "sku",
        "description": "description",
        "n fournisseur": "supplier_number",
        "1 famille": "product_family",
        "ss famille": "sub_family",
        "ss ss famille": "sub_sub_family",
        "ss ss ss famille": "sub_sub_sub_family",
        "marque": "brand",
    }
    
    # Model fields to update
    UPDATE_FIELDS = [
        "description", "supplier_number", "product_family",
        "sub_family", "sub_sub_family", "sub_sub_sub_family", "brand"
    ]
    
    def import_from_csv(self, csv_file_path: str):
        """Import products from CSV file."""
        logger.info(f"📦 Importing products from: {csv_file_path}")
        
        # Read CSV
        df = self.csv_reader.read_csv(csv_file_path, delimiter=";")
        if df is None:
            logger.error(f"❌ Failed to read {csv_file_path}")
            return
        
        logger.info(f"Original columns: {df.columns.tolist()}")
        
        # Drop unwanted columns
        df = df.drop(df.columns[self.DROP_COLUMN_INDEXES], axis=1, errors="ignore")
        logger.debug(f"Columns after dropping: {df.columns.tolist()}")
        
        # Normalize and map headers
        df = self.header_normalizer.apply_header_mapping(
            df, self.HEADER_MAP, strip_digits=True
        )
        logger.info(f"Final columns: {df.columns.tolist()}")
        
        # Ensure SKU column exists
        if "sku" not in df.columns:
            first_col = df.columns[0]
            if first_col.lower().startswith("n"):
                df.rename(columns={first_col: "sku"}, inplace=True)
                logger.warning(f"⚠️ Using first column '{first_col}' as SKU")
            else:
                logger.error("❌ No SKU column found. Cannot import.")
                return
        
        # Clean and filter SKUs
        df["sku"] = df["sku"].astype(str).str.strip()
        df = df[df["sku"].notna() & (df["sku"] != "")]
        
        # Import products
        added, updated = 0, 0
        for _, row in df.iterrows():
            sku = row["sku"]
            
            # Find or create product
            product = self.session.query(Product).filter_by(sku=sku).first()
            if not product:
                product = Product(sku=sku)
                self.session.add(product)
                added += 1
            else:
                updated += 1
            
            # Update fields
            for field in self.UPDATE_FIELDS:
                value = row.get(field)
                if pd.notna(value) and str(value).strip():
                    setattr(product, field, str(value).strip())
        
        # Commit changes
        self.safe_commit(f"Products import: {added} added, {updated} updated")
        logger.info(f"✅ Products imported: Added={added}, Updated={updated}")
