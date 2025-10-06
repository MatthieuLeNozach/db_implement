import logging
import os
from typing import List, Set

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

from src.core.config import Config
from src.models import Base, Mercuriale, CustomerAssignmentCondition, Product
from src.importers.product_importer import ProductImporter
from src.importers.customer_importer import CustomerImporter
from src.importers.customer_assignment_importer import CustomerAssignmentImporter

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ImportManager:
    """
    High-level import pipeline manager.

    Responsibilities:
    - Wire up importers for products & customers
    - Populate mercuriales from DB-stored conditions
    - Assign customers to mercuriales
    - Populate mercuriale -> product associations from CSVs
    """

    def __init__(self, session):
        self.session = session
        self.product_importer = ProductImporter(session)
        self.customer_importer = CustomerImporter(session)
        self.assignment_importer = CustomerAssignmentImporter(session)

    # -------------------------
    # CSV preprocessing helper
    # -------------------------
    def preprocess_csv_files(self) -> None:
        """
        Lightweight preprocessing to convert obvious comma-delimited mercuriale CSVs
        to semicolon-delimited files (our downstream readers expect ';' often).
        This is optional — function is conservative (writes only when comma-only).
        """
        logger.info("🔧 Preprocessing mercuriale CSV files...")
        folder = "db_files/mercuriales/"

        if not os.path.isdir(folder):
            logger.warning(f"⚠️ Mercuriale folder not found: {folder}")
            return

        for fname in os.listdir(folder):
            if not fname.lower().endswith(".csv"):
                continue
            path = os.path.join(folder, fname)
            try:
                with open(path, "rb") as f:
                    raw = f.read(4096)
                # try to decode with utf-8 or latin1 fallback to inspect delimiter
                try:
                    head = raw.decode("utf-8", errors="strict").splitlines()[0]
                    encoding_used = "utf-8"
                except UnicodeDecodeError:
                    head = raw.decode("iso-8859-1", errors="replace").splitlines()[0]
                    encoding_used = "iso-8859-1"

                if ("," in head) and (";" not in head):
                    logger.info(f"🔄 Converting {fname} (detected comma-delimited, encoding={encoding_used})")
                    # read with best-effort encoding, then overwrite with semicolon delimiter
                    try:
                        df = pd.read_csv(path, sep=",", dtype=str, encoding=encoding_used)
                    except Exception:
                        df = pd.read_csv(path, sep=",", dtype=str, encoding="iso-8859-1", on_bad_lines="skip")
                    df.to_csv(path, sep=";", index=False, encoding="utf-8")
                    logger.info(f"✅ Converted {fname} to semicolon-delimited UTF-8")
            except Exception as exc:
                logger.warning(f"⚠️ Skipping preprocessing for {fname}: {exc}")

        logger.info("✅ CSV preprocessing complete.")

    # -------------------------
    # Product / Customer imports
    # -------------------------
    def update_products(self, product_csv_path: str):
        logger.info("📦 Updating products...")
        try:
            self.product_importer.import_from_csv(product_csv_path)
        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to update products: {e}")
            self.session.rollback()
            raise

    def update_customers(self, customer_csv_path: str):
        logger.info("👥 Updating customers...")
        try:
            self.customer_importer.import_from_csv(customer_csv_path)
        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to update customers: {e}")
            self.session.rollback()
            raise

    # -------------------------
    # Mercuriale population & assignment
    # -------------------------
    def populate_mercuriales_from_conditions(self):
        logger.info("🔹 Populating Mercuriale table from assignment conditions...")
        try:
            conditions = self.session.query(CustomerAssignmentCondition).all()
            if not conditions:
                logger.warning("⚠️ No assignment conditions found in the database.")
                return

            mercuriale_names = {c.mercuriale_name.strip() for c in conditions if c.mercuriale_name}
            for name in sorted(mercuriale_names):
                if not name:
                    continue
                if not self.session.query(Mercuriale).filter_by(name=name).first():
                    self.session.add(Mercuriale(name=name))
                    logger.info(f"✅ Added Mercuriale: {name}")

            self.session.commit()
        except SQLAlchemyError as e:
            self.session.rollback()
            logger.error(f"❌ Failed to populate Mercuriale table: {e}")
            raise

    def assign_customers_to_mercuriales(self):
        logger.info("🔹 Assigning customers to Mercuriales based on conditions...")
        try:
            self.assignment_importer.assign_mercuriale_from_conditions()
        except Exception as e:
            self.session.rollback()
            logger.error(f"❌ Failed to assign customers: {e}")
            raise

    # -------------------------
    # Mercuriale -> Product associations
    # -------------------------
    @staticmethod
    def _normalize_sku_variants(skus: List[str]) -> Set[str]:
        """
        Return a set of SKU variants to query for. Normalize common differences:
        - strip spaces
        - remove leading zeros variant (e.g. '000158' -> '158')
        - original form preserved
        This returns a set of unique variants to increase matching chance.
        """
        out = set()
        for s in skus:
            if s is None:
                continue
            s0 = str(s).strip()
            if s0 == "":
                continue
            out.add(s0)
            # add no-leading-zero variant if it changes
            nozeros = s0.lstrip("0")
            if nozeros:
                out.add(nozeros)
            # also add zero-padded 6-digit variant (common in some ERPs)
            if s0.isdigit():
                out.add(s0.zfill(6))
        return out

    def populate_mercuriale_products(self):
        """
        Read mercuriale CSVs and attach products to Mercuriale.products relationship.

        Strategy:
        - For each CSV file in db_files/mercuriales:
            * try to read using semicolon/utf-8, fallback encodings
            * find SKU column (sku / Sku / SKU or first column)
            * normalize SKUs to several variants
            * query Product table using Product.sku (string) and matched variants
            * assign found products to mercuriale.products
        """
        logger.info("🔹 Populating Mercuriale → Product associations...")
        folder = "db_files/mercuriales/"

        if not os.path.isdir(folder):
            logger.warning(f"⚠️ Mercuriale folder not found: {folder}")
            return

        try:
            for fname in sorted(os.listdir(folder)):
                if not fname.lower().endswith(".csv"):
                    continue
                mercuriale_name = fname.rsplit(".", 1)[0]
                mercuriale = self.session.query(Mercuriale).filter_by(name=mercuriale_name).first()
                if not mercuriale:
                    logger.info(f"⚪ Mercuriale file {fname} found but no DB entry for '{mercuriale_name}' — skipping")
                    continue

                fpath = os.path.join(folder, fname)
                df = None

                # try different encodings and ensure we read at least first column
                for enc in ("utf-8", "iso-8859-1", "latin-1"):
                    try:
                        df = pd.read_csv(fpath, sep=";", dtype=str, encoding=enc, on_bad_lines="skip")
                        if df is not None and not df.empty:
                            break
                    except (UnicodeDecodeError, pd.errors.ParserError, pd.errors.EmptyDataError):
                        # try next encoding
                        df = None
                if df is None or df.empty:
                    # try comma delimiter as final fallback
                    for enc in ("utf-8", "iso-8859-1", "latin-1"):
                        try:
                            df = pd.read_csv(fpath, sep=",", dtype=str, encoding=enc, on_bad_lines="skip")
                            if df is not None and not df.empty:
                                break
                        except Exception:
                            df = None

                if df is None or df.empty:
                    logger.warning(f"⚠️ Could not read or file empty: {fname}")
                    continue

                # unify column names & pick SKU column
                cols = [c.strip() for c in df.columns]
                cols_lower = [c.lower() for c in cols]
                df.columns = cols  # keep original case trimmed

                sku_col = None
                for cand in ("sku", "skus", "s n", "s/no", "n", "no", "nbr", "code"):
                    if cand in cols_lower:
                        idx = cols_lower.index(cand)
                        sku_col = df.columns[idx]
                        break

                if sku_col is None:
                    # fallback to first column if it looks like SKUs (mostly numeric)
                    sku_col = df.columns[0]
                    logger.debug(f"Using first column '{sku_col}' as SKU for {fname}")

                # extract, normalize and deduplicate SKUs
                raw_skus = df[sku_col].dropna().astype(str).str.strip().tolist()
                if not raw_skus:
                    logger.warning(f"⚠️ No SKUs found in {fname} (column {sku_col})")
                    continue

                sku_variants = self._normalize_sku_variants(raw_skus)

                # log a short sample for verification
                sample_before = raw_skus[:8]
                sample_after = list(sku_variants)[:8]
                logger.info(f"📦 {fname}: read {len(raw_skus)} SKUs (sample: {sample_before}) -> {len(sku_variants)} normalized variants (sample: {sample_after})")

                # Query DB for products matching any variant
                # To avoid huge IN clauses we convert to list and chunk if needed
                variants_list = list(sku_variants)
                found_products = []
                chunk_size = 1000
                from sqlalchemy import select

                for i in range(0, len(variants_list), chunk_size):
                    chunk = variants_list[i : i + chunk_size]
                    q = self.session.query(Product).filter(Product.sku.in_(chunk))
                    found_products.extend(q.all())

                # dedupe found_products by sku
                found_by_sku = {p.sku: p for p in found_products}
                assigned_count = len(found_by_sku)

                # attach products (only unique objects)
                mercuriale.products = list(found_by_sku.values())
                self.session.add(mercuriale)
                logger.info(f"✅ {assigned_count} products assigned to {mercuriale_name}")

            # commit all assignments once finished
            self.session.commit()
            logger.info("✅ Mercuriale product associations complete.")
        except Exception as exc:
            self.session.rollback()
            logger.error(f"❌ Failed to populate Mercuriale products: {exc}")
            raise

    # -------------------------
    # High-level runner
    # -------------------------
    def run_all(self):
        logger.info("🚀 Starting full import pipeline...")

        # 1. products & customers (from paths in .env)
        self.update_products(Config.PRODUCT_CSV_PATH)
        self.update_customers(Config.CUSTOMER_CSV_PATH)

        # 2. mercuriale creation & customer assignment
        self.populate_mercuriales_from_conditions()
        self.assign_customers_to_mercuriales()

        # 3. ensure CSVs are in consistent format, then attach products to mercuriales
        self.preprocess_csv_files()
        self.populate_mercuriale_products()

        logger.info("✅ Full import pipeline complete.")
