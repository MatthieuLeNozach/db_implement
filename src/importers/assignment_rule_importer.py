import pandas as pd
import logging
from sqlalchemy.exc import SQLAlchemyError
from src.models.assignment_rules import CustomerAssignmentRule

logger = logging.getLogger(__name__)

class AssignmentRuleImporter:
    """
    Imports assignment rules/conditions from a CSV into the DB.
    CSV columns: field, operator, value, mercuriale_name, priority, required
    """

    def __init__(self, session):
        self.session = session

    def import_from_csv(self, csv_file_path: str):
        logger.info(f"⚙️ Importing customer assignment rules from: {csv_file_path}")
        df = pd.read_csv(csv_file_path, dtype=str).fillna("")

        added, updated = 0, 0

        for _, row in df.iterrows():
            field = row.get("field", "").strip()
            operator = row.get("operator", "").strip()
            value = row.get("value", "").strip()
            mercuriale_name = row.get("mercuriale_name", "").strip()
            priority = row.get("priority", "").strip()
            required = row.get("required", "").strip().upper() in ["TRUE", "1", "YES"]

            if not field or not operator or not value or not mercuriale_name:
                logger.warning(f"Skipping incomplete row: {row.to_dict()}")
                continue

            try:
                priority = int(priority)
            except ValueError:
                priority = 99  # default fallback

            # Check for existing identical rule
            existing = (
                self.session.query(CustomerAssignmentRule)
                .filter_by(
                    field=field,
                    operator=operator,
                    value=value,
                    mercuriale_name=mercuriale_name,
                    priority=priority,
                    required=required
                )
                .first()
            )

            if existing:
                updated += 1
            else:
                rule = CustomerAssignmentRule(
                    field=field,
                    operator=operator,
                    value=value,
                    mercuriale_name=mercuriale_name,
                    priority=priority,
                    required=required
                )
                self.session.add(rule)
                added += 1

        try:
            self.session.commit()
            logger.info(f"✅ Assignment rules import complete: {added} added, {updated} already existed.")
        except SQLAlchemyError as e:
            logger.error(f"❌ Commit failed: {e}")
            self.session.rollback()
            raise
