# src/importers/customer_importer.py

import logging
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from ftfy import fix_text
from unidecode import unidecode
from src.models import Customer
from src.importers.base import BaseCSVImporter

logger = logging.getLogger(__name__)

class CustomerImporter(BaseCSVImporter):
    """
    Imports and cleans customer CSV data from ERP exports.
    Fixes encoding, normalizes headers, ensures uniqueness, and populates the Customer model,
    including 'name2'.
    """

    # Mapping normalized headers -> model fields
    HEADER_MAP = {
        "n": "customer_number",
        "nom": "name",
        "nom 2": "name2",
        "zone de livraison": "delivery_zone",
        "code postal": "postal_code",
        "ville": "city",
        "1 gamme obligatoire": "required_range",
        "2 type client": "client_type",
        "3 sous type client": "sub_client_type",
    }

    def _normalize_header(self, h: str) -> str:
        """Clean a single CSV header to match mapping keys"""
        h = fix_text(str(h).strip())
        h = unidecode(h)
        h = h.replace("*", " ").replace("-", " ").replace("_", " ")
        # ⚠ DO NOT strip digits, because 'Nom 2' is meaningful
        h = ' '.join(h.split()).lower()
        return h

    def import_from_csv(self, csv_file_path: str):
        logger.info(f"📦 Importing customers from: {csv_file_path}")

        try:
            df = pd.read_csv(csv_file_path, delimiter=';', skipinitialspace=True, dtype=str, encoding="latin-1")
        except FileNotFoundError:
            logger.error(f"❌ File not found: {csv_file_path}")
            return
        except Exception as e:
            logger.error(f"❌ Failed to read CSV: {e}")
            return

        logger.info(f"Original columns: {df.columns.tolist()}")

        # Fix broken characters in headers
        df.columns = [fix_text(str(h)).strip() for h in df.columns]

        # Normalize headers
        normalized_headers = [self._normalize_header(h) for h in df.columns]

        # Map to model fields and ensure uniqueness
        new_columns = []
        seen = {}
        for h in normalized_headers:
            mapped = self.HEADER_MAP.get(h, h)
            if mapped in seen:
                count = seen[mapped] + 1
                mapped = f"{mapped}_{count}"
            seen[mapped] = seen.get(mapped, 0)
            new_columns.append(mapped)
        df.columns = new_columns

        logger.info(f"Final columns after mapping: {df.columns.tolist()}")
        logger.debug(f"First 5 rows:\n{df.head()}")

        df.dropna(how='all', inplace=True)

        added, updated = 0, 0
        for idx, row in df.iterrows():
            customer_number = str(row["customer_number"]).strip() if "customer_number" in row else ""
            if not customer_number:
                logger.warning(f"Skipping row {idx} with empty customer_number")
                continue

            customer = self.session.query(Customer).filter_by(customer_number=customer_number).first()
            if not customer:
                customer = Customer(customer_number=customer_number)
                self.session.add(customer)
                added += 1
                logger.debug(f"Adding new customer: {customer_number}")
            else:
                updated += 1
                logger.debug(f"Updating existing customer: {customer_number}")

            # Assign fields safely, use row[col] to avoid Series
            for field in ["name", "name2", "delivery_zone", "postal_code", "city",
                          "required_range", "client_type", "sub_client_type"]:
                if field in row:
                    value = row[field]
                    if pd.notna(value) and str(value).strip() != "":
                        if field == "required_range":
                            setattr(customer, field, str(value).upper() == "OUI")
                        else:
                            setattr(customer, field, str(value).strip())

        try:
            self.session.commit()
            logger.info(f"✅ Customers imported. Added: {added}, Updated: {updated}")
        except SQLAlchemyError as e:
            self.session.rollback()
            logger.error(f"❌ DB commit failed: {e}")
            raise
