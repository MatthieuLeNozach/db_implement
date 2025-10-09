# src/importers/import_manager.py

import logging
from sqlalchemy.exc import SQLAlchemyError
from src.core.config import Config
from .product_importer import ProductImporter
from .customer_importer import CustomerImporter
from .assignment_importer import AssignmentImporter
from .mercuriale_importer import MercurialeImporter

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ImportManager:
    """
    High-level orchestrator for the complete import pipeline.
    
    Pipeline stages:
    1. Import products from ERP CSV
    2. Import customers from ERP CSV
    3. Import assignment rules from CSV (optional)
    4. Create Mercuriales from assignment conditions
    5. Assign customers to Mercuriales
    6. Preprocess mercuriale CSV files
    7. Assign products to Mercuriales
    """
    
    def __init__(self, session, mercuriale_folder: str = "db_files/mercuriales/"):
        self.session = session
        self.product_importer = ProductImporter(session)
        self.customer_importer = CustomerImporter(session)
        self.assignment_importer = AssignmentImporter(session)
        self.mercuriale_importer = MercurialeImporter(session, mercuriale_folder)
    
    def import_products(self, csv_path: str = None):
        """
        Import products from CSV.
        
        Args:
            csv_path: Path to product CSV (defaults to Config.PRODUCT_CSV_PATH)
        """
        path = csv_path or Config.paths.PRODUCT_CSV_PATH
        logger.info("📦 Starting product import...")
        
        try:
            self.product_importer.import_from_csv(path)
        except SQLAlchemyError as e:
            logger.error(f"❌ Product import failed: {e}")
            self.session.rollback()
            raise
    
    def import_customers(self, csv_path: str = None):
        """
        Import customers from CSV.
        
        Args:
            csv_path: Path to customer CSV (defaults to Config.paths.CUSTOMER_CSV_PATH)
        """
        path = csv_path or Config.paths.CUSTOMER_CSV_PATH
        logger.info("👥 Starting customer import...")
        
        try:
            self.customer_importer.import_from_csv(path)
        except SQLAlchemyError as e:
            logger.error(f"❌ Customer import failed: {e}")
            self.session.rollback()
            raise
    
    def import_assignment_rules(self, csv_path: str):
        """
        Import customer assignment rules from CSV.
        
        Args:
            csv_path: Path to assignment rules CSV
        """
        logger.info("⚙️ Starting assignment rules import...")
        
        try:
            self.assignment_importer.import_rules_from_csv(csv_path)
        except SQLAlchemyError as e:
            logger.error(f"❌ Assignment rules import failed: {e}")
            self.session.rollback()
            raise
    
    def setup_mercuriales(self):
        """
        Complete Mercuriale setup:
        1. Create Mercuriale records from assignment conditions
        2. Assign customers to Mercuriales
        3. Preprocess mercuriale CSV files
        4. Assign products to Mercuriales
        """
        logger.info("🔹 Starting Mercuriale setup...")
        
        try:
            # Create Mercuriales from conditions
            self.mercuriale_importer.populate_from_conditions()
            
            # Assign customers
            self.assignment_importer.assign_customers_to_mercuriales()
            
            # Prepare CSV files
            self.mercuriale_importer.preprocess_csv_files()
            
            # Assign products
            self.mercuriale_importer.populate_products()
            
            logger.info("✅ Mercuriale setup complete")
        
        except Exception as e:
            logger.error(f"❌ Mercuriale setup failed: {e}")
            self.session.rollback()
            raise
    
    def run_full_pipeline(self):
        """
        Execute the complete import pipeline.
        
        This is the main entry point for a full data refresh.
        """
        logger.info("🚀 Starting FULL import pipeline...")
        
        try:
            # Stage 1: Import base data
            self.import_products()
            self.import_customers()
            
            # Stage 2: Setup Mercuriales and assignments
            self.setup_mercuriales()
            
            logger.info("✅✅✅ FULL import pipeline completed successfully! ✅✅✅")
        
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}")
            self.session.rollback()
            raise
    
    def run_mercuriale_only(self):
        """
        Run only Mercuriale-related operations.
        
        Useful when products/customers are already imported and you only
        need to update Mercuriale assignments.
        """
        logger.info("🔹 Running Mercuriale-only pipeline...")
        
        try:
            self.setup_mercuriales()
            logger.info("✅ Mercuriale-only pipeline complete")
        
        except Exception as e:
            logger.error(f"❌ Mercuriale-only pipeline failed: {e}")
            self.session.rollback()
            raise
    
    def run_customer_reassignment(self, default_mercuriale: str = "mercuriale_medelys"):
        """
        Re-run customer to Mercuriale assignment only.
        
        Useful when assignment rules have changed.
        
        Args:
            default_mercuriale: Fallback Mercuriale for unmatched customers
        """
        logger.info("🔄 Re-assigning customers to Mercuriales...")
        
        try:
            self.assignment_importer.assign_customers_to_mercuriales(default_mercuriale)
            logger.info("✅ Customer reassignment complete")
        
        except Exception as e:
            logger.error(f"❌ Customer reassignment failed: {e}")
            self.session.rollback()
            raise