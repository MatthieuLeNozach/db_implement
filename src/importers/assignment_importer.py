# src/importers/assignment_importer.py

import logging
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from src.models.models import (
    Customer, Mercuriale, CustomerAssignmentCondition, 
    CustomerAssignmentRule  # if still using this model
)
from .base_importer import BaseImporter

logger = logging.getLogger(__name__)


class AssignmentImporter(BaseImporter):
    """
    Unified importer for:
    1. Assignment rules/conditions from CSV
    2. Applying conditions to assign customers to Mercuriales
    """
    
    def import_rules_from_csv(self, csv_file_path: str):
        """
        Import assignment rules from CSV.
        
        Expected columns: field, operator, value, mercuriale_name, priority, required
        """
        logger.info(f"⚙️ Importing assignment rules from: {csv_file_path}")
        
        df = self.csv_reader.read_csv(csv_file_path)
        if df is None:
            logger.error(f"❌ Failed to read {csv_file_path}")
            return
        
        df = df.fillna("")
        
        added, updated = 0, 0
        for _, row in df.iterrows():
            field = row.get("field", "").strip()
            operator = row.get("operator", "").strip()
            value = row.get("value", "").strip()
            mercuriale_name = row.get("mercuriale_name", "").strip()
            priority = row.get("priority", "").strip()
            required = row.get("required", "").strip().upper() in ["TRUE", "1", "YES", "OUI"]
            
            # Validate required fields
            if not all([field, operator, value, mercuriale_name]):
                logger.warning(f"⚠️ Skipping incomplete row: {row.to_dict()}")
                continue
            
            # Parse priority with fallback
            try:
                priority = int(priority)
            except ValueError:
                priority = 99
                logger.debug(f"Using default priority 99 for rule: {field} {operator} {value}")
            
            # Check for existing rule
            existing = (
                self.session.query(CustomerAssignmentCondition)
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
                rule = CustomerAssignmentCondition(
                    field=field,
                    operator=operator,
                    value=value,
                    mercuriale_name=mercuriale_name,
                    priority=priority,
                    required=required
                )
                self.session.add(rule)
                added += 1
        
        self.safe_commit(f"Assignment rules: {added} added, {updated} already existed")
        logger.info(f"✅ Rules imported: Added={added}, Updated={updated}")
    
    def assign_customers_to_mercuriales(self, default_mercuriale: str = "mercuriale_medelys"):
        """
        Assign customers to Mercuriales based on stored conditions.
        
        Rules are applied in ascending priority order.
        First match wins unless overridden by higher-priority required rule.
        
        Args:
            default_mercuriale: Fallback Mercuriale name for unmatched customers
        """
        logger.info("🔹 Assigning customers to Mercuriales based on conditions...")
        
        # Load conditions sorted by priority
        conditions = (
            self.session.query(CustomerAssignmentCondition)
            .order_by(CustomerAssignmentCondition.priority.asc())
            .all()
        )
        
        if not conditions:
            logger.warning("⚠️ No assignment conditions found")
            return
        
        customers = self.session.query(Customer).all()
        assigned_count = 0
        unassigned_count = 0
        
        for customer in customers:
            assigned = False
            
            for cond in conditions:
                # Get customer field value
                field_value = getattr(customer, cond.field, None)
                if field_value is None:
                    continue
                
                # Normalize for comparison
                field_str = str(field_value).upper()
                cond_value_str = str(cond.value).upper()
                
                # Apply operator
                match = self._apply_operator(field_str, cond_value_str, cond.operator)
                
                if match:
                    # Find Mercuriale
                    mercuriale = self.session.query(Mercuriale).filter_by(
                        name=cond.mercuriale_name
                    ).first()
                    
                    if mercuriale:
                        customer.mercuriale = mercuriale
                        assigned = True
                        assigned_count += 1
                        logger.debug(
                            f"✅ Customer {customer.customer_number} → {mercuriale.name} "
                            f"(rule: {cond.field} {cond.operator} {cond.value})"
                        )
                    else:
                        logger.warning(
                            f"⚠️ Condition matched but Mercuriale '{cond.mercuriale_name}' not found"
                        )
                    
                    # Stop if required condition
                    if cond.required:
                        break
            
            # Assign default Mercuriale if no match
            if not assigned:
                default = self.session.query(Mercuriale).filter_by(
                    name=default_mercuriale
                ).first()
                
                if default:
                    customer.mercuriale = default
                    unassigned_count += 1
                    logger.debug(
                        f"📋 Customer {customer.customer_number} → {default.name} (default)"
                    )
                else:
                    logger.warning(
                        f"⚠️ Customer {customer.customer_number} not assigned "
                        f"(default Mercuriale '{default_mercuriale}' not found)"
                    )
        
        self.safe_commit("Customer-Mercuriale assignments")
        logger.info(
            f"✅ Assignment complete: {assigned_count} matched, "
            f"{unassigned_count} defaulted"
        )
    
    @staticmethod
    def _apply_operator(field_value: str, condition_value: str, operator: str) -> bool:
        """Apply comparison operator."""
        if operator == "equals":
            return field_value == condition_value
        elif operator == "contains":
            return condition_value in field_value
        elif operator == "not_equals":
            return field_value != condition_value
        elif operator == "startswith":
            return field_value.startswith(condition_value)
        elif operator == "endswith":
            return field_value.endswith(condition_value)
        else:
            logger.warning(f"⚠️ Unknown operator: {operator}")
            return False
