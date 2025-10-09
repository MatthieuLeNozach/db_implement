# src/importers/product_importer.py

import logging
import os
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from ftfy import fix_text
from unidecode import unidecode
from src.models import Product, Mercuriale
from .base import BaseCSVImporter

logger = logging.getLogger(__name__)

class ProductImporter(BaseCSVImporter):
    """
    Import products from ERP CSVs, clean headers, fix encoding,
    drop unwanted columns, map fields, and add/update products in DB.
    Also supports assigning products to Mercuriales.
    """

    # Columns to drop by position (0-indexed)
    DROP_COLUMN_INDEXES = [2,3,4,5,6,7,8,9,10,16,17]

    # Mapping cleaned headers -> model attributes
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

    def _clean_headers(self, headers):
        clean = []
        for h in headers:
            h = fix_text(str(h).strip())
            h = unidecode(h)
            h = h.replace("*", " ").replace("-", " ").replace("_", " ")
            h = ''.join(c if not c.isdigit() else " " for c in h)
            h = ' '.join(h.split()).lower()
            clean.append(h)
        return clean

    def import_from_csv(self, csv_file_path: str):
        logger.info(f"📦 Importing product data from {csv_file_path}")

        try:
            df = pd.read_csv(
                csv_file_path,
                delimiter=";",
                dtype=str,
                skipinitialspace=True,
                encoding="latin-1"
            )
        except FileNotFoundError:
            logger.error(f"❌ File not found: {csv_file_path}")
            return
        except Exception as e:
            logger.error(f"❌ Failed to read CSV: {e}")
            return

        logger.info(f"Original columns: {df.columns.tolist()}")

        # Drop unwanted columns
        df = df.drop(df.columns[self.DROP_COLUMN_INDEXES], axis=1, errors="ignore")
        logger.info(f"Columns after dropping unwanted ones: {df.columns.tolist()}")

        # Clean headers
        clean_headers = self._clean_headers(df.columns)
        df.columns = [self.HEADER_MAP.get(h, col) for h, col in zip(clean_headers, df.columns)]
        logger.info(f"Final columns after cleaning & mapping: {df.columns.tolist()}")

        # Ensure SKU exists
        if "sku" not in df.columns:
            first_col = df.columns[0]
            if first_col.lower().startswith("n"):
                df.rename(columns={first_col: "sku"}, inplace=True)
                logger.warning(f"No 'SKU' column found; using first column '{first_col}' as SKU")
            else:
                logger.error("❌ No SKU column found. Cannot import products.")
                return

        # Clean SKU values
        df["sku"] = df["sku"].astype(str).str.strip()
        df = df[df["sku"].notna() & (df["sku"] != "")]

        added, updated = 0, 0
        for _, row in df.iterrows():
            sku = row["sku"]
            product = self.session.query(Product).filter_by(sku=sku).first()
            if not product:
                product = Product(sku=sku)
                self.session.add(product)
                added += 1
            else:
                updated += 1

            # Assign fields dynamically
            for field in ["description", "supplier_number", "product_family",
                          "sub_family", "sub_sub_family", "sub_sub_sub_family", "brand"]:
                value = row.get(field)
                if pd.notna(value) and value != "":
                    setattr(product, field, value)

        try:
            self.session.commit()
            logger.info(f"✅ Products imported. Added: {added}, Updated: {updated}")
        except SQLAlchemyError as e:
            self.session.rollback()
            logger.error(f"❌ DB commit failed: {e}")
            raise

    # -------------------------
    # Mercuriale product assignment
    # -------------------------

    def assign_products_to_mercuriales(self):
        """
        Assign products to Mercuriales based on CSV files in db_files/mercuriales/.
        Handles encoding, delimiters, and cleans SKUs.
        """
        folder = "db_files/mercuriales/"
        for file in os.listdir(folder):
            if not file.endswith(".csv"):
                continue

            mercuriale_name = file.replace(".csv", "")
            mercuriale = self.session.query(Mercuriale).filter_by(name=mercuriale_name).first()
            if not mercuriale:
                continue

            path = os.path.join(folder, file)
            df = self._read_mercuriale_csv(path)
            skus = df.iloc[:, 0].astype(str).str.strip().apply(fix_text).tolist()

            products = self.session.query(Product).filter(Product.sku.in_(skus)).all()
            mercuriale.products = products
            self.session.add(mercuriale)
            logger.info(f"✅ {len(products)} products assigned to {mercuriale_name}")

        try:
            self.session.commit()
        except SQLAlchemyError as e:
            self.session.rollback()
            logger.error(f"❌ Failed to commit Mercuriale products: {e}")
            raise

    def _read_mercuriale_csv(self, path):
        """
        Read CSV handling:
        - UTF-8 / ISO-8859-1
        - Comma or semicolon delimiters
        - Bad lines skipped
        """
        try:
            return pd.read_csv(path, usecols=[0], dtype=str, on_bad_lines='skip')
        except UnicodeDecodeError:
            logger.warning(f"⚠️ UTF-8 decoding failed for {path}, trying ISO-8859-1")
            return pd.read_csv(path, usecols=[0], dtype=str, encoding="ISO-8859-1", on_bad_lines='skip')
        except pd.errors.ParserError:
            logger.warning(f"⚠️ ParserError for {path}, trying semicolon delimiter")
            return pd.read_csv(path, usecols=[0], dtype=str, sep=";", on_bad_lines='skip')
