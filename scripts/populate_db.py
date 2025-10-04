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

from src.config import Config
from src.models import Base
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
