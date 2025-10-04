# src/services/customer_matching_service.py
from typing import Optional, Dict, Any, List
from src.repositories.customer_repository import CustomerRepository
from src.models.customer import Customer
import logging

logger = logging.getLogger(__name__)

class CustomerMatchingService:
    """Service for matching customers based on various strategies."""
    
    def __init__(self, customer_repo: CustomerRepository, matching_rules: Dict[str, Any]):
        self.customer_repo = customer_repo
        self.matching_rules = matching_rules
    
    def find_customer_by_entity_code(self, entity_code: str) -> Optional[str]:
        """Find customer number using configured matching strategies."""
        if not entity_code:
            logger.debug("No entity code provided")
            return None
        
        strategies = self.matching_rules.get("customer_matching_strategies", ["exact_match"])
        logger.debug(f"Trying matching strategies: {strategies} for entity_code: {entity_code}")
        
        for strategy in strategies:
            customer = self._try_matching_strategy(entity_code, strategy)
            if customer:
                logger.info(f"Found customer {customer.customer_number} using strategy: {strategy}")
                return customer.customer_number
        
        logger.warning(f"No customer found for entity_code: {entity_code}")
        return None
    
    def _try_matching_strategy(self, entity_code: str, strategy: str) -> Optional[Customer]:
        """Try a specific matching strategy."""
        logger.debug(f"Trying strategy: {strategy} for entity_code: {entity_code}")
        
        if strategy == "exact_match":
            return self.customer_repo.find_by_entity_code(entity_code)
        
        elif strategy == "company_name_fallback":
            company_patterns = self.matching_rules.get("company_name_patterns", [])
            customers = self.customer_repo.search_by_company_patterns(company_patterns)
            
            if len(customers) == 1:
                return customers[0]
            elif len(customers) > 1:
                logger.warning(f"Multiple customers found for company patterns: {company_patterns}")
                return customers[0]  # Return first match
        
        elif strategy == "manual_mapping":
            manual_map = self.matching_rules.get("entity_code_mapping", {})
            if entity_code in manual_map:
                mapped_code = manual_map[entity_code]
                return self.customer_repo.find_by_entity_code(mapped_code)
        
        return None