#!/usr/bin/env python3
"""
Database population script v2.0

Usage:
    python scripts/populate_db.py                                    # Default: all tables
    python scripts/populate_db.py --tables products customers        # Specific tables
    python scripts/populate_db.py --tables formats                   # Only formats
    python scripts/populate_db.py --skip-rules                       # Skip rules import
"""
import sys
import os

# Add project root to Python path (so `import src` works)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
import logging
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd

from src.core.config import Config
from src.models.models import Base, FormatConfig, CustomerAssignmentCondition
from src.importers import ImportManager

# ------------------------
# Logging setup
# ------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ------------------------
# CLI arguments
# ------------------------
def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Populate/update DB tables from CSV files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                    # Import everything
  %(prog)s --tables products customers        # Import specific tables
  %(prog)s --tables mercuriales               # Only Mercuriale operations
  %(prog)s --skip-rules                       # Skip assignment rules import
  %(prog)s --tables formats                   # Only format configurations
        """
    )
    
    parser.add_argument(
        "--tables",
        nargs="+",
        default=["all"],
        choices=["all", "products", "customers", "mercuriales", "formats", "rules"],
        help="Tables to update (default: all)"
    )
    
    parser.add_argument(
        "--skip-rules",
        action="store_true",
        help="Skip importing assignment rules"
    )
    
    parser.add_argument(
        "--mercuriale-folder",
        default="db_files/mercuriales/",
        help="Path to mercuriale CSV files folder"
    )
    
    parser.add_argument(
        "--rules-file",
        default="db_files/rules/assignment_conditions.csv",
        help="Path to assignment rules CSV"
    )
    
    parser.add_argument(
        "--formats-file",
        default="db_files/rules/format_config.csv",
        help="Path to format configuration CSV"
    )
    
    parser.add_argument(
        "--drop-formats",
        action="store_true",
        help="Drop and recreate FormatConfig table"
    )
    
    return parser.parse_args()


# ------------------------
# Assignment rules import
# ------------------------
def import_assignment_rules(session, csv_path: str):
    """Import assignment conditions from CSV to DB table."""
    if not os.path.exists(csv_path):
        logger.warning(f"⚠️ Assignment rules CSV not found: {csv_path}")
        return
    
    logger.info(f"📥 Importing assignment rules from {csv_path}")
    
    try:
        df = pd.read_csv(csv_path, dtype=str).fillna("")
        added = 0
        skipped = 0
        
        for _, row in df.iterrows():
            # Check if already exists
            rule_id = row.get("id")
            if rule_id:
                exists = session.query(CustomerAssignmentCondition).filter_by(
                    id=int(rule_id)
                ).first()
                if exists:
                    skipped += 1
                    continue
            
            # Create new condition
            cond = CustomerAssignmentCondition(
                id=int(rule_id) if rule_id else None,
                field=row["field"],
                operator=row["operator"],
                value=row["value"],
                mercuriale_name=row["mercuriale_name"],
                priority=int(row["priority"]),
                required=row["required"].strip().upper() in ["TRUE", "1", "YES", "OUI"]
            )
            session.add(cond)
            added += 1
        
        session.commit()
        logger.info(f"✅ Assignment rules imported: {added} added, {skipped} skipped")
    
    except Exception as e:
        session.rollback()
        logger.error(f"❌ Failed to import assignment rules: {e}")
        raise


# ------------------------
# Format configurations import
# ------------------------
def import_format_configs(session, csv_path: str, drop_table: bool = False):
    """Import format configurations from CSV to DB table."""
    if not os.path.exists(csv_path):
        logger.warning(f"⚠️ Format configurations CSV not found: {csv_path}")
        return
    
    logger.info(f"📥 Importing format configurations from {csv_path}")
    
    # Drop table if requested
    if drop_table:
        logger.info("🗑️ Dropping FormatConfig table...")
        FormatConfig.__table__.drop(session.bind, checkfirst=True)
        Base.metadata.create_all(session.bind)
    
    try:
        df = pd.read_csv(csv_path, dtype=str).fillna("")
        added = 0
        skipped = 0
        
        for idx, row in df.iterrows():
            try:
                logger.debug(f"Processing row {idx}: {row.get('format_name', 'unknown')}")
                
                # Check if already exists
                exists = session.query(FormatConfig).filter_by(
                    format_name=row["format_name"]
                ).first()
                
                if exists:
                    logger.debug(f"Skipping existing format: {row['format_name']}")
                    skipped += 1
                    continue
                
                # Parse list-like columns - keep as comma-separated strings
                # The FormatConfig model likely expects strings, not Python lists
                strategies_str = str(row.get("customer_matching_strategies", "")).strip()
                # Remove extra quotes and normalize
                strategies_str = strategies_str.replace('"', '').strip()
                
                company_patterns_str = str(row.get("company_name_patterns", "")).strip()
                # Remove extra quotes and normalize
                company_patterns_str = company_patterns_str.replace('"', '').strip()
                
                # Create config
                config = FormatConfig(
                    format_name=row["format_name"],
                    po_number_fuzzy=row.get("po_number_fuzzy", ""),
                    delivery_date_regex=row.get("delivery_date_regex", ""),
                    entity_code_regex=row.get("entity_code_regex", ""),
                    entity_name_regex=row.get("entity_name_regex", ""),
                    header_fuzzy=row.get("header_fuzzy", ""),
                    skip_footer_keywords=row.get("skip_footer_keywords", ""),
                    min_columns=int(row.get("min_columns", 0)),
                    fuzzy_threshold=float(row.get("fuzzy_threshold", 80.0)),
                    column_description=row.get("column_description", ""),
                    column_sku=row.get("column_sku", ""),
                    column_quantity=row.get("column_quantity", ""),
                    column_unit=row.get("column_unit", ""),
                    customer_matching_strategies=strategies_str,
                    company_name_patterns=company_patterns_str
                )
                
                session.add(config)
                session.flush()  # Flush immediately to catch errors per row
                added += 1
                logger.info(f"✅ Added format: {row['format_name']}")
            
            except Exception as e:
                session.rollback()  # Rollback this row
                logger.error(f"❌ Failed to import row {idx} ({row.get('format_name', 'unknown')}): {e}")
                continue
        
        session.commit()
        logger.info(f"✅ Format configurations imported: {added} added, {skipped} skipped")
    
    except Exception as e:
        session.rollback()
        logger.error(f"❌ Failed to import format configurations: {e}")
        raise


# ------------------------
# Main execution
# ------------------------
def main():
    args = parse_arguments()
    
    logger.info("🚀 Starting database population script v2.0")
    logger.info(f"Tables to update: {', '.join(args.tables)}")
    
    # ------------------------
    # Database setup
    # ------------------------
    engine = create_engine(Config.database.DATABASE_URL, echo=False, future=True)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Ensure tables exist
    Base.metadata.create_all(engine)
    logger.info("✅ Database tables verified/created")
    
    # Normalize table selection
    tables = set(t.lower() for t in args.tables)
    if "all" in tables:
        tables = {"products", "customers", "mercuriales", "formats", "rules"}
    
    # ------------------------
    # Import assignment rules (if not skipped)
    # ------------------------
    if "rules" in tables and not args.skip_rules:
        import_assignment_rules(session, args.rules_file)
    
    # ------------------------
    # Import format configurations
    # ------------------------
    if "formats" in tables:
        import_format_configs(session, args.formats_file, drop_table=args.drop_formats)
    
    # ------------------------
    # Initialize ImportManager v2.0
    # ------------------------
    manager = ImportManager(session, mercuriale_folder=args.mercuriale_folder)
    
    # ------------------------
    # Execute imports based on selection
    # ------------------------
    try:
        # Full pipeline
        if tables >= {"products", "customers", "mercuriales"}:
            logger.info("📦 Running FULL import pipeline...")
            manager.run_full_pipeline()
        
        # Individual imports
        else:
            if "products" in tables:
                manager.import_products()
            
            if "customers" in tables:
                manager.import_customers()
            
            if "mercuriales" in tables:
                manager.setup_mercuriales()
        
        logger.info("✅✅✅ Database population/update finished successfully! ✅✅✅")
    
    except Exception as e:
        logger.error(f"❌ Import failed: {e}")
        session.rollback()
        raise
    
    finally:
        session.close()


if __name__ == "__main__":
    main()