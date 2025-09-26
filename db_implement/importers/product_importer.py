import pandas as pd
import logging
from sqlalchemy.exc import SQLAlchemyError
from models import Product
from .base import BaseCSVImporter

logger = logging.getLogger(__name__)


class ProductImporter(BaseCSVImporter):
    """
    Imports and cleans product CSV data from ERP exports.
    - Drops unwanted columns based on position (to handle unstable headers)
    - Renames remaining columns to clean PascalCase names
    - Handles UTF-8 encoding issues and messy column names
    """

    # Columns to DROP by their original order (0-based index)
    # (These positions come from the ERP extract you shared)
    DROP_COLUMN_INDEXES = [2, 3, 4, 5, 6, 7, 8, 9, 10, 16, 17]

    # Clean renaming mapping (after dropping)
    # We'll rename by fuzzy matching key substrings
    RENAME_MAP = {
        "n": "SKU",
        "description": "Description",
        "fournisseur": "SupplierNumber",
        "famille": "ProductFamily",
        "ss famille": "SubFamily",
        "ss ss famille": "SubSubFamily",
        "ss ss ss famille": "SubSubSubFamily",
        "marque": "Brand",
    }

    def _clean_headers(self, headers):
        """
        Clean messy headers (remove �, accents, lowercase) for easier matching.
        """
        clean_headers = []
        for h in headers:
            h = str(h).strip()
            h = h.replace("�", "e").replace("°", "o").replace("é", "e").replace("è", "e")
            h = h.replace("à", "a").replace("ç", "c")
            h = h.replace("_", " ").replace("-", " ").lower()
            clean_headers.append(h)
        return clean_headers

    def _rename_columns(self, df):
        """
        Rename columns based on substring matching against RENAME_MAP.
        """
        new_cols = []
        cleaned = self._clean_headers(df.columns)
        for original, clean in zip(df.columns, cleaned):
            renamed = None
            for key, target in self.RENAME_MAP.items():
                if key in clean:
                    renamed = target
                    break
            new_cols.append(renamed or original)
        df.columns = new_cols
        return df

    def import_from_csv(self, csv_file_path: str):
        """
        Main entrypoint: import, drop unnecessary columns, rename, and insert into DB.
        """
        logger.info(f"📦 Importing product data from: {csv_file_path}")

        # Load CSV with semicolon delimiter (standard ERP export)
        df = pd.read_csv(csv_file_path, delimiter=";", dtype=str, skipinitialspace=True)

        # 1️⃣ Drop unwanted columns by position
        original_cols = df.columns.tolist()
        keep_cols = [c for i, c in enumerate(original_cols) if i not in self.DROP_COLUMN_INDEXES]
        df = df[keep_cols]

        # 2️⃣ Clean + rename columns
        df = self._rename_columns(df)

        # 3️⃣ Strip spaces and fix SKU
        df["SKU"] = df["SKU"].astype(str).str.strip()
        df = df[df["SKU"] != ""]  # drop empty rows

        # 4️⃣ Insert into database
        added, skipped = 0, 0
        for _, row in df.iterrows():
            sku = row.get("SKU")
            if not sku:
                continue

            existing = self.session.query(Product).filter_by(sku=sku).first()
            if existing:
                skipped += 1
                continue

            product = Product(
                sku=sku,
                description=row.get("Description", ""),
                supplier_number=row.get("SupplierNumber", ""),
                product_family=row.get("ProductFamily", ""),
                sub_family=row.get("SubFamily", ""),
                sub_sub_family=row.get("SubSubFamily", ""),
                sub_sub_sub_family=row.get("SubSubSubFamily", ""),
                brand=row.get("Brand", ""),
            )
            self.session.add(product)
            added += 1

        try:
            self.session.commit()
            logger.info(f"✅ Imported {added} products. ⏩ Skipped {skipped} existing.")
        except SQLAlchemyError as e:
            self.session.rollback()
            logger.error(f"❌ DB commit failed: {e}")
            raise

