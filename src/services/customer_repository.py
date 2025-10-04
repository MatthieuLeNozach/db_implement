# src/repositories/customer_repository.py
from typing import Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import or_
from src.models.customer import Customer
import logging
import re

logger = logging.getLogger(__name__)

class CustomerRepository:
    """Repository for customer database operations."""
    
    def __init__(self, session: Session):
        self.session = session
    
    def find_by_customer_number(self, customer_number: str) -> Optional[Customer]:
        """Find customer by exact customer number."""
        return self.session.query(Customer).filter(
            Customer.customer_number == customer_number
        ).first()
    
    def find_by_name_pattern(self, pattern: str) -> List[Customer]:
        """Find customers by name pattern (case-insensitive)."""
        escaped_pattern = f"%{pattern}%"
        return self.session.query(Customer).filter(
            or_(
                Customer.name.ilike(escaped_pattern),
                Customer.name2.ilike(escaped_pattern)
            )
        ).all()
    
    def find_by_entity_code(self, entity_code: str) -> Optional[Customer]:
        """Find customer by entity code using fuzzy matching."""
        # Clean entity code
        clean_code = self._clean_entity_code(entity_code)
        
        # Try exact match first
        customers = self.find_by_name_pattern(clean_code)
        
        if customers:
            # Return first match if found
            logger.info(f"Found customer for entity code '{entity_code}': {customers[0].customer_number}")
            return customers[0]
        
        return None
    
    def _clean_entity_code(self, entity_code: str) -> str:
        """Clean entity code by removing common prefixes."""
        return entity_code.replace("UR ", "").replace("SO", "").replace("FR", "").strip()
    
    def search_by_company_patterns(self, patterns: List[str]) -> List[Customer]:
        """Search customers by company name patterns."""
        customers = []
        for pattern in patterns:
            found = self.find_by_name_pattern(pattern)
            customers.extend(found)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_customers = []
        for customer in customers:
            if customer.id not in seen:
                seen.add(customer.id)
                unique_customers.append(customer)
        
        return unique_customers