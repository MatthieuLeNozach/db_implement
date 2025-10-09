# src/importers/customer_importer.py

import logging
import pandas as pd
from src.models.models import Customer
from .base_importer import BaseImporter

logger = logging.getLogger(__name__)


class CustomerImporter(BaseImporter):
    """Import and update customers from ERP CSV exports."""
    
    # Mapping normalized headers → model fields
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
    
    # Model fields to update
    UPDATE_FIELDS = [
        "name", "name2", "delivery_zone", "postal_code", "city",
        "required_range", "client_type", "sub_client_type"
    ]
    
    def import_from_csv(self, csv_file_path: str):
        """Import customers from CSV file."""
        logger.info(f"👥 Importing customers from: {csv_file_path}")
        
        # Read CSV
        df = self.csv_reader.read_csv(csv_file_path, delimiter=";")
        if df is None:
            logger.error(f"❌ Failed to read {csv_file_path}")
            return
        
        logger.info(f"Original columns: {df.columns.tolist()}")
        
        # Normalize and map headers (preserve digits for "nom 2")
        df = self.header_normalizer.apply_header_mapping(
            df, self.HEADER_MAP, strip_digits=False
        )
        logger.info(f"Final columns: {df.columns.tolist()}")
        
        # Drop empty rows
        df.dropna(how='all', inplace=True)
        
        # Verify customer_number column exists
        if "customer_number" not in df.columns:
            logger.error("❌ No customer_number column found. Cannot import.")
            return
        
        # Import customers
        added, updated = 0, 0
        for idx, row in df.iterrows():
            customer_number = str(row["customer_number"]).strip()
            if not customer_number:
                logger.warning(f"⚠️ Skipping row {idx} with empty customer_number")
                continue
            
            # Find or create customer
            customer = self.session.query(Customer).filter_by(
                customer_number=customer_number
            ).first()
            
            if not customer:
                customer = Customer(customer_number=customer_number)
                self.session.add(customer)
                added += 1
                logger.debug(f"➕ Adding customer: {customer_number}")
            else:
                updated += 1
                logger.debug(f"🔄 Updating customer: {customer_number}")
            
            # Update fields
            for field in self.UPDATE_FIELDS:
                if field not in row:
                    continue
                
                value = row[field]
                if pd.notna(value) and str(value).strip():
                    # Special handling for boolean field
                    if field == "required_range":
                        setattr(customer, field, str(value).upper() == "OUI")
                    else:
                        setattr(customer, field, str(value).strip())
        
        # Commit changes
        self.safe_commit(f"Customers import: {added} added, {updated} updated")
        logger.info(f"✅ Customers imported: Added={added}, Updated={updated}")
