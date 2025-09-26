import logging
from models import Customer, Mercuriale
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


class CustomerAssignmentImporter:
    """
    Dynamically assigns customers to mercuriales based on conditional rules
    applied to customer fields (e.g., Nom2, RequiredRange).
    """

    ASSIGNMENT_RULES = [
        {
            "condition": lambda c: c.name2 and "COMPASS" in c.name2.upper(),
            "mercuriale_name": "compass",
        },
        {
            "condition": lambda c: c.name2 and any(x in c.name2.upper() for x in ["SODEXO", "SOGERES"]) and getattr(c, "required_range", False),
            "mercuriale_name": "sogeres",
        },
        {
            "condition": lambda c: c.name2 and any(x in c.name2.upper() for x in ["SODEXO", "SOGERES"]) and not getattr(c, "required_range", False),
            "mercuriale_name": "sodexo_open",
        },
        {
            "condition": lambda c: c.name2 and "ELIOR" in c.name2.upper(),
            "mercuriale_name": "elior_open",
        },

    ]

    def __init__(self, session):
        self.session = session

    def assign_mercuriale(self):
        customers = self.session.query(Customer).all()
        assigned_count = 0
        skipped_count = 0

        for customer in customers:
            assigned = False
            for rule in self.ASSIGNMENT_RULES:
                try:
                    if rule["condition"](customer):
                        mercuriale = self.session.query(Mercuriale).filter_by(
                            name=rule["mercuriale_name"]
                        ).first()
                        if mercuriale:
                            customer.mercuriale = mercuriale
                            assigned = True
                            assigned_count += 1
                            break
                        else:
                            logger.warning(f"Mercuriale {rule['mercuriale_name']} not found")
                except Exception as e:
                    logger.error(f"Error evaluating rule for customer {customer.customer_number}: {e}")

            if not assigned:
                skipped_count += 1
                logger.info(f"No rule matched for customer {customer.customer_number}")

        try:
            self.session.commit()
            logger.info(
                f"✅ Customer assignment complete: {assigned_count} assigned, {skipped_count} skipped"
            )
        except SQLAlchemyError as e:
            self.session.rollback()
            logger.error(f"❌ Commit failed: {e}")
            raise
