# src/services/pdf_extraction_service.py
from typing import Dict, Any, Optional
import pdfplumber
import re
from collections import OrderedDict
import logging

logger = logging.getLogger(__name__)

class PDFExtractionService:
    """Service for extracting text and information from PDF files."""
    
    def __init__(self, extraction_rules: Dict[str, Any]):
        self.extraction_rules = extraction_rules
    
    def extract_text_from_pdf(self, pdf_path: str) -> Optional[str]:
        """Extract text content from PDF file."""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                text_parts = []
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
                return "\n".join(text_parts) if text_parts else None
        except Exception as e:
            logger.error(f"Error extracting text from PDF {pdf_path}: {e}")
            return None
    
    def extract_structured_data(self, text: str) -> Dict[str, Any]:
        """Extract structured data from text using configured rules."""
        if not self.extraction_rules:
            raise ValueError("No extraction rules configured")
        
        extracted_data = {}
        
        # Extract PO number
        extracted_data["po_number"] = self._extract_po_number(text)
        
        # Extract delivery date
        extracted_data["delivery_date"] = self._extract_delivery_date(text)
        
        # Extract entity code
        extracted_data["entity_code"] = self._extract_entity_code(text)
        
        # Extract entity name
        extracted_data["entity_name"] = self._extract_entity_name(text)
        
        return self._order_extracted_data(extracted_data)
    
    def _extract_po_number(self, text: str) -> Optional[str]:
        """Extract PO number using processor or pattern."""
        processor = self.extraction_rules.get("po_number_processor")
        if processor:
            try:
                return processor(text)
            except Exception as e:
                logger.error(f"Error in PO number processor: {e}")
                return None
        
        pattern = self.extraction_rules.get("po_number_fuzzy")
        if pattern:
            match = re.search(f"{pattern}\\s*(\\S+)", text)
            return match.group(1) if match else None
        
        return None
    
    def _extract_delivery_date(self, text: str) -> Optional[str]:
        """Extract delivery date using patterns."""
        patterns = self.extraction_rules.get("delivery_date_fuzzy", [])
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1)
        return None
    
    def _extract_entity_code(self, text: str) -> Optional[str]:
        """Extract entity code using processor."""
        processor = self.extraction_rules.get("entity_code_processor")
        if processor:
            try:
                return processor(text)
            except Exception as e:
                logger.error(f"Error in entity code processor: {e}")
        return None
    
    def _extract_entity_name(self, text: str) -> Optional[str]:
        """Extract entity name using processor."""
        processor = self.extraction_rules.get("entity_name_processor")
        if processor:
            try:
                return processor(text)
            except Exception as e:
                logger.error(f"Error in entity name processor: {e}")
        return None
    
    def _order_extracted_data(self, data: Dict[str, Any]) -> OrderedDict:
        """Order extracted data in desired sequence."""
        desired_order = [
            "customer_number",
            "entity_name", 
            "entity_code",
            "delivery_date",
            "po_number"
        ]
        
        ordered_data = OrderedDict()
        for key in desired_order:
            if key in data:
                ordered_data[key] = data[key]
        
        # Add any remaining keys
        for key, value in data.items():
            if key not in ordered_data:
                ordered_data[key] = value
        
        return ordered_data