# src/importers/customer_assignment_importer.py

import logging
from sqlalchemy.exc import SQLAlchemyError
from ..models.models import Customer, Mercuriale, CustomerAssignmentCondition


logger = logging.getLogger(__name__)

class CustomerAssignmentImporter:
    """
    Assigns customers to Mercuriales based on conditions stored in the database.
    Rules are read from CustomerAssignmentCondition and applied in ascending priority order.
    First matching rule wins unless overridden by a higher-priority rule with required=True.
    """

    def __init__(self, session):
        self.session = session

    def assign_mercuriale_from_conditions(self):
        """
        Assign customers to Mercuriales based on DB-stored assignment conditions.
        """
        conditions = (
            self.session.query(CustomerAssignmentCondition)
            .order_by(CustomerAssignmentCondition.priority.asc())
            .all()
        )
        customers = self.session.query(Customer).all()

        for customer in customers:
            assigned = False

            for cond in conditions:
                field_val = getattr(customer, cond.field, None)
                if field_val is None:
                    continue

                field_str = str(field_val).upper()
                cond_val_str = str(cond.value).upper()

                match = False
                if cond.operator == "equals" and field_str == cond_val_str:
                    match = True
                elif cond.operator == "contains" and cond_val_str in field_str:
                    match = True
                elif cond.operator == "not_equals" and field_str != cond_val_str:
                    match = True

                if match:
                    mercuriale = self.session.query(Mercuriale).filter_by(
                        name=cond.mercuriale_name
                    ).first()
                    if mercuriale:
                        customer.mercuriale = mercuriale
                        assigned = True
                        logger.info(
                            f"Customer {customer.customer_number} assigned to {mercuriale.name} "
                            f"via condition {cond.field} {cond.operator} {cond.value}"
                        )
                    else:
                        logger.warning(
                            f"Condition matched but Mercuriale {cond.mercuriale_name} not found"
                        )

                    # If the condition is required, stop checking further rules
                    if cond.required:
                        break

            # If no condition matched, assign full-access Mercuriale (optional)
            if not assigned:
                full_access = self.session.query(Mercuriale).filter_by(
                    name="mercuriale_medelys"
                ).first()
                if full_access:
                    customer.mercuriale = full_access
                    logger.info(
                        f"Customer {customer.customer_number} assigned to full-access Mercuriale {full_access.name}"
                    )
                else:
                    logger.warning(
                        f"Customer {customer.customer_number} not assigned and full-access Mercuriale missing"
                    )

        try:
            self.session.commit()
        except SQLAlchemyError as e:
            self.session.rollback()
            logger.error(f"❌ Commit failed while assigning customers: {e}")
            raise
