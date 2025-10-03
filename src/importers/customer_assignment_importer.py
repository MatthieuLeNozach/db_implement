import pandas as pd
import logging
from sqlalchemy.exc import SQLAlchemyError
from models import Customer, Mercuriale, Product

logger = logging.getLogger(__name__)


class AssignmentImporter:
    """
    Handles assignments between customers, mercuriales, and products.
    """

    def __init__(self, session):
        self.session = session

    def assign_customers_to_mercuriale(self, csv_file_path: str):
        """
        Assign each customer to a mercuriale based on a CSV file.
        CSV format: customer_number, mercuriale_name
        """
        logger.info(f"📎 Assigning customers to mercuriales from: {csv_file_path}")

        df = pd.read_csv(csv_file_path, dtype=str).fillna("")
        assignments, skipped = 0, 0

        for _, row in df.iterrows():
            customer_number = row.get("customer_number", "").strip()
            mercuriale_name = row.get("mercuriale_name", "").strip()

            if not customer_number or not mercuriale_name:
                skipped += 1
                continue

            customer = self.session.query(Customer).filter_by(customer_number=customer_number).first()
            mercuriale = self.session.query(Mercuriale).filter_by(name=mercuriale_name).first()

            if customer and mercuriale:
                customer.mercuriale = mercuriale
                assignments += 1
            else:
                skipped += 1

        try:
            self.session.commit()
            logger.info(f"✅ Assigned {assignments} customers. ⏩ Skipped {skipped} (not found).")
        except SQLAlchemyError as e:
            logger.error(f"❌ Commit failed: {e}")
            self.session.rollback()
            raise

    def assign_products_to_mercuriale(self, csv_file_path: str):
        """
        Assign products to mercuriales based on a CSV file.
        CSV format: mercuriale_name, sku
        """
        logger.info(f"📦 Assigning products to mercuriales from: {csv_file_path}")

        df = pd.read_csv(csv_file_path, dtype=str).fillna("")
        assignments, skipped = 0, 0

        for _, row in df.iterrows():
            mercuriale_name = row.get("mercuriale_name", "").strip()
            sku = row.get("sku", "").strip()

            if not mercuriale_name or not sku:
                skipped += 1
                continue

            mercuriale = self.session.query(Mercuriale).filter_by(name=mercuriale_name).first()
            product = self.session.query(Product).filter_by(sku=sku).first()

            if mercuriale and product:
                if product not in mercuriale.products:
                    mercuriale.products.append(product)
                    assignments += 1
            else:
                skipped += 1

        try:
            self.session.commit()
            logger.info(f"✅ Assigned {assignments} products. ⏩ Skipped {skipped} (not found).")
        except SQLAlchemyError as e:
            logger.error(f"❌ Commit failed: {e}")
            self.session.rollback()
            raise
