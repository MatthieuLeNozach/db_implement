import logging
from sqlalchemy.exc import SQLAlchemyError
from models import Base, Mercuriale
from importers.product_importer import ProductImporter
from importers.customer_importer import CustomerImporter
from importers.customer_assignment_importer import CustomerAssignmentImporter

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ImportManager:
    """
    Manages the full import pipeline:
    - Products
    - Customers
    - Mercuriale population & customer assignment
    """

    def __init__(self, session):
        self.session = session
        self.product_importer = ProductImporter(session)
        self.customer_importer = CustomerImporter(session)
        self.assignment_importer = CustomerAssignmentImporter(session)

    def populate_mercuriales_from_rules(self):
        """
        Scan ASSIGNMENT_RULES and ensure Mercuriale entries exist in DB.
        """
        logger.info("🔹 Populating Mercuriale table from assignment rules...")

        for rule in self.assignment_importer.ASSIGNMENT_RULES:
            name = rule["mercuriale_name"]
            mercuriale = self.session.query(Mercuriale).filter_by(name=name).first()
            if not mercuriale:
                mercuriale = Mercuriale(name=name)
                self.session.add(mercuriale)
                logger.info(f"Added Mercuriale: {name}")
        try:
            self.session.commit()
        except SQLAlchemyError as e:
            self.session.rollback()
            logger.error(f"Failed to populate Mercuriale table: {e}")
            raise

    def run(self, product_csv_path: str, customer_csv_path: str):
        """
        Execute the full import pipeline.
        """
        # 1️⃣ Import products
        logger.info("📦 Importing products...")
        self.product_importer.import_from_csv(product_csv_path)

        # 2️⃣ Import customers
        logger.info("👥 Importing customers...")
        self.customer_importer.import_from_csv(customer_csv_path)

        # 3️⃣ Populate Mercuriales from rules
        self.populate_mercuriales_from_rules()

        # 4️⃣ Assign customers to Mercuriales based on rules
        logger.info("🔹 Assigning customers to Mercuriales...")
        self.assignment_importer.assign_mercuriale()
