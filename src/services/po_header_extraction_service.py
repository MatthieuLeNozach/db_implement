# src/services/po_header_extraction_service.py
from typing import Dict, Any, Optional
from pathlib import Path
from collections import OrderedDict
from src.services.database_service import DatabaseService
from src.services.customer_matching_service import CustomerMatchingService
from src.services.pdf_extraction_service import PDFExtractionService
from src.repositories.customer_repository import CustomerRepository
import logging

logger = logging.getLogger(__name__)

class POHeaderExtractionService:
    """Main service for extracting PO header information from PDFs."""
    
    def __init__(self, format_rules: Dict[str, Any]):
        self.format_rules = format_rules
        self.db_service = DatabaseService()
        self.pdf_service = PDFExtractionService(format_rules.get("extraction_rules", {}))
    
    def extract_info(self, pdf_path: str) -> OrderedDict:
        """Extract complete PO header information from PDF."""
        # Validate extraction rules
        if "extraction_rules" not in self.format_rules:
            raise KeyError(f"Extraction rules not found for format: {self.format_rules}")
        
        # Extract text from PDF
        text = self.pdf_service.extract_text_from_pdf(pdf_path)
        if not text:
            logger.warning(f"No text extracted from PDF: {pdf_path}")
            return OrderedDict()
        
        # Extract structured data
        extracted_data = self.pdf_service.extract_structured_data(text)
        
        # Enhance with customer information
        enhanced_data = self._enhance_with_customer_data(extracted_data)
        
        logger.debug(f"Final extracted info: {enhanced_data}")
        return enhanced_data
    
    def _enhance_with_customer_data(self, data: Dict[str, Any]) -> OrderedDict:
        """Enhance extracted data with customer information from database."""
        entity_code = data.get("entity_code")
        
        if entity_code:
            with self.db_service.get_session() as session:
                customer_repo = CustomerRepository(session)
                matching_service = CustomerMatchingService(customer_repo, self.format_rules)
                
                customer_number = matching_service.find_customer_by_entity_code(entity_code)
                data["customer_number"] = customer_number
        else:
            data["customer_number"] = None
        
        return self.pdf_service._order_extracted_data(data)