import logging
from sqlalchemy.exc import SQLAlchemyError
from src.config import Config
from src.models import Base, Mercuriale, CustomerAssignmentCondition
from src.importers.product_importer import ProductImporter
from src.importers.customer_importer import CustomerImporter
from src.importers.customer_assignment_importer import CustomerAssignmentImporter

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ImportManager:
    """
    Manages the full import pipeline:
    - Products
    - Customers
    - Assignment conditions
    - Mercuriale population & customer assignment
    """

    def __init__(self, session):
        self.session = session
        self.product_importer = ProductImporter(session)
        self.customer_importer = CustomerImporter(session)
        self.assignment_importer = CustomerAssignmentImporter(session)

    # -------------------------
    # Per-table import methods
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
    # Mercuriale / assignment logic
    # -------------------------

    def populate_mercuriales_from_conditions(self):
        """
        Ensure all Mercuriale entries exist based on CustomerAssignmentCondition table.
        """
        logger.info("🔹 Populating Mercuriale table from assignment conditions...")
        try:
            conditions = self.session.query(CustomerAssignmentCondition).all()
            if not conditions:
                logger.warning("⚠️ No assignment conditions found in the database.")
                return

            mercuriale_names = {c.mercuriale_name.strip() for c in conditions if c.mercuriale_name}
            for name in mercuriale_names:
                if not self.session.query(Mercuriale).filter_by(name=name).first():
                    self.session.add(Mercuriale(name=name))
                    logger.info(f"✅ Added Mercuriale: {name}")

            self.session.commit()
        except SQLAlchemyError as e:
            self.session.rollback()
            logger.error(f"❌ Failed to populate Mercuriale table: {e}")
            raise

    def assign_customers_to_mercuriales(self):
        """
        Assign customers based on the imported assignment conditions.
        """
        logger.info("🔹 Assigning customers to Mercuriales based on conditions...")
        try:
            self.assignment_importer.assign_mercuriale_from_conditions()
        except Exception as e:
            self.session.rollback()
            logger.error(f"❌ Failed to assign customers: {e}")
            raise

    # -------------------------
    # High-level method
    # -------------------------

    def run_all(self):
        """
        Execute the full import pipeline:
        1. Update products
        2. Update customers
        3. Populate Mercuriales from conditions
        4. Assign customers
        """
        logger.info("🚀 Starting full import pipeline...")

        self.update_products(Config.PRODUCT_CSV_PATH)
        self.update_customers(Config.CUSTOMER_CSV_PATH)

        # Populate mercuriales based on assignment conditions
        self.populate_mercuriales_from_conditions()

        # Assign customers based on assignment conditions
        self.assign_customers_to_mercuriales()

        logger.info("✅ Full import pipeline complete.")
