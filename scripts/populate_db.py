#!/usr/bin/env python3
import sys
import os

# Add project root to Python path (so `import src` works)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd

from src.core.config import Config
from src.models import Base
from src.models.format_config import FormatConfig
from src.importers.import_manager import ImportManager
from src.importers.customer_assignment_importer import CustomerAssignmentCondition

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ------------------------
# CLI arguments
# ------------------------
import argparse
parser = argparse.ArgumentParser(description="Populate/update DB tables from CSV files.")
parser.add_argument(
    "--tables",
    nargs="+",
    default=["products", "customers", "mercuriales"],
    help="List of tables to update (options: products, customers, mercuriales, all).",
)
args = parser.parse_args()

# ------------------------
# Database setup
# ------------------------
engine = create_engine(Config.DATABASE_URL, echo=False, future=True)
Session = sessionmaker(bind=engine)
session = Session()

# Ensure tables exist
FormatConfig.__table__.drop(engine, checkfirst=True)
Base.metadata.create_all(engine)

# ------------------------
# Import assignment conditions (CSV -> DB table)
# ------------------------
conditions_csv = "db_files/rules/assignment_conditions.csv"
if os.path.exists(conditions_csv):
    logger.info(f"📥 Importing assignment conditions from {conditions_csv}")
    df = pd.read_csv(conditions_csv, dtype=str).fillna("")
    for _, row in df.iterrows():
        # Avoid duplicate entries
        exists = session.query(CustomerAssignmentCondition).filter_by(id=row["id"]).first()
        if exists:
            continue

        cond = CustomerAssignmentCondition(
            id=int(row["id"]),
            field=row["field"],
            operator=row["operator"],
            value=row["value"],
            mercuriale_name=row["mercuriale_name"],
            priority=int(row["priority"]),
            required=row["required"].strip().upper() == "TRUE"
        )
        session.add(cond)
    session.commit()
    logger.info("✅ Assignment conditions imported.")
else:
    logger.warning(f"⚠️ Assignment conditions CSV not found: {conditions_csv}")
    
    

# ------------------------
# Import format configurations (CSV -> DB table)
# ------------------------
if "formats" in [t.lower() for t in args.tables] or "all" in [t.lower() for t in args.tables]:
    formats_csv = "db_files/rules/format_config.csv"
    if os.path.exists(formats_csv):
        logger.info(f"📥 Importing format configurations from {formats_csv}")
        df_formats = pd.read_csv(formats_csv, dtype=str).fillna("")

        for idx, row in df_formats.iterrows():
            try:
                logger.debug(f"Processing row {idx}: {row.to_dict()}")

                # Convert list-like columns
                strategies = [s.strip() for s in row["customer_matching_strategies"].split(",") if s.strip()]
                company_patterns = [p.strip() for p in row["company_name_patterns"].split(",") if p.strip()]

                # Check if already exists
                exists = session.query(FormatConfig).filter_by(format_name=row["format_name"]).first()
                if exists:
                    logger.info(f"Skipping existing format: {row['format_name']}")
                    continue

                config = FormatConfig(
                    format_name=row["format_name"],
                    po_number_fuzzy=row["po_number_fuzzy"],
                    delivery_date_regex=row["delivery_date_regex"],
                    entity_code_regex=row["entity_code_regex"],
                    entity_name_regex=row["entity_name_regex"],
                    header_fuzzy=row["header_fuzzy"],
                    skip_footer_keywords=row["skip_footer_keywords"],
                    min_columns=int(row["min_columns"]),
                    fuzzy_threshold=float(row["fuzzy_threshold"]),
                    column_description=row["column_description"],
                    column_sku=row["column_sku"],
                    column_quantity=row["column_quantity"],
                    column_unit=row["column_unit"],
                    customer_matching_strategies=strategies,
                    company_name_patterns=company_patterns
                )

                session.add(config)
                session.commit()
                logger.info(f"✅ Imported format configuration: {row['format_name']}")

            except Exception as e:
                session.rollback()
                logger.error(f"❌ Failed to import row {idx} ({row['format_name']}): {e}")

    else:
        logger.warning(f"⚠️ Format configurations CSV not found: {formats_csv}")


# ------------------------
# Import manager
# ------------------------
manager = ImportManager(session)

# ------------------------
# Execute updates
# ------------------------
tables_to_update = [t.lower() for t in args.tables]

if "all" in tables_to_update:
    manager.run_all()
else:
    if "products" in tables_to_update:
        manager.update_products(Config.PRODUCT_CSV_PATH)
    if "customers" in tables_to_update:
        manager.update_customers(Config.CUSTOMER_CSV_PATH)
    if "mercuriales" in tables_to_update:
        manager.populate_mercuriales_from_conditions()
        manager.assign_customers_to_mercuriales()

logger.info("✅ DB population/update finished.")
