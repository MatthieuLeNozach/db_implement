import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from models import Customer
from .base import BaseCSVImporter
import logging

logger = logging.getLogger(__name__)

class CustomerImporter(BaseCSVImporter):
    def clean_column_name(self, col_name: str) -> str:
        """Customer-specific column cleaning."""
        cleaned = col_name.strip('*').strip()
        mapping = {
            "N°": "CustomerNumber",
            "Nom": "Name",
            "Nom 2": "Name2",
            "Zone de livraison": "DeliveryZone",
            "Code postal": "PostalCode",
            "Ville": "City",
            "1-GAMME OBLIGATOIRE": "RequiredRange",
            "2-TYPE CLIENT": "ClientType",
            "3-Sous Type Client": "SubClientType"
        }
        return mapping.get(cleaned, cleaned.replace(" ", ""))

    def import_from_csv(self, csv_file_path: str):
        logger.info(f"Importing customers from: {csv_file_path}")
        df = pd.read_csv(csv_file_path, delimiter=';', skipinitialspace=True)
        df.columns = [self.clean_column_name(c) for c in df.columns]
        df.dropna(how='all', inplace=True)

        customers_added = 0
        for _, row in df.iterrows():
            customer_number = str(row.get('CustomerNumber', ''))
            if not customer_number:
                continue

            existing = self.session.query(Customer).filter_by(customer_number=customer_number).first()
            if existing:
                continue

            customer = Customer(
                customer_number=customer_number,
                name=row.get('Name', ''),
                name2=row.get('Name2', ''),
                delivery_zone=row.get('DeliveryZone', ''),
                postal_code=row.get('PostalCode', ''),
                city=row.get('City', ''),
                required_range=row.get('RequiredRange', '').upper() == "OUI",
                client_type=row.get('ClientType', ''),
                sub_client_type=row.get('SubClientType', '')
            )
            self.session.add(customer)
            customers_added += 1

        try:
            self.session.commit()
            logger.info(f"✅ Imported {customers_added} customers.")
        except SQLAlchemyError as e:
            logger.error(f"❌ Commit failed: {e}")
            self.session.rollback()
