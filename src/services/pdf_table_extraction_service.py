# src/services/pdf_extraction_service.py (updated)
from typing import Dict, Any, Optional
from pathlib import Path
import pandas as pd
import pdfplumber
import re
from collections import OrderedDict
import logging

from .pdf_table_extraction_service import PDFTableExtractionService

logger = logging.getLogger(__name__)

class PDFTablextractionService:
    """Service for extracting text and table data from PDF files."""
    
    def __init__(self, extraction_rules: Dict[str, Any]):
        self.extraction_rules = extraction_rules
        self.table_service = PDFTableExtractionService(extraction_rules)
    
    def extract_table_data(self, pdf_path: Path) -> pd.DataFrame:
        """Extract table data from PDF."""
        return self.table_service.extract_table_data(pdf_path)
    
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
        
        # Extract various fields
        extracted_data["po_number"] = self._extract_po_number(text)
        extracted_data["delivery_date"] = self._extract_delivery_date(text)
        extracted_data["entity_code"] = self._extract_entity_code(text)
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
        
        # Fallback to pattern-based extraction
        pattern = self.extraction_rules.get("po_number_fuzzy", "")
        if pattern:
            try:
                match = re.search(rf"{re.escape(pattern)}\s*([^\n]+)", text, re.IGNORECASE)
                return match.group(1).strip() if match else None
            except Exception as e:
                logger.error(f"Error extracting PO number with pattern '{pattern}': {e}")
                return None
        
        return None
    
    def _extract_delivery_date(self, text: str) -> Optional[str]:
        """Extract delivery date using patterns."""
        patterns = self.extraction_rules.get("delivery_date_fuzzy", [])
        
        for pattern in patterns:
            try:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    return match.group(1)
            except Exception as e:
                logger.error(f"Error with delivery date pattern '{pattern}': {e}")
                continue
        
        return None
    
    def _extract_entity_code(self, text: str) -> Optional[str]:
        """Extract entity code using processor or patterns."""
        processor = self.extraction_rules.get("entity_code_processor")
        if processor:
            try:
                return processor(text)
            except Exception as e:
                logger.error(f"Error in entity code processor: {e}")
                return None
        
        # Fallback to pattern-based extraction
        patterns = self.extraction_rules.get("entity_code_fuzzy", [])
        for pattern in patterns:
            try:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    return match.group(0)
            except Exception as e:
                logger.error(f"Error with entity code pattern '{pattern}': {e}")
                continue
        
        return None
    
    def _extract_entity_name(self, text: str) -> Optional[str]:
        """Extract entity name using fuzzy pattern."""
        pattern = self.extraction_rules.get("entity_name_fuzzy", "")
        if not pattern:
            return None
        
        try:
            # Look for the pattern and extract following lines
            lines = text.split('\n')
            for i, line in enumerate(lines):
                if pattern.lower() in line.lower():
                    # Extract the next few lines as entity name
                    name_lines = []
                    for j in range(i + 1, min(i + 4, len(lines))):
                        if lines[j].strip() and not self._is_likely_header_or_footer(lines[j]):
                            name_lines.append(lines[j].strip())
                        else:
                            break
                    return ' '.join(name_lines) if name_lines else None
            return None
        except Exception as e:
            logger.error(f"Error extracting entity name with pattern '{pattern}': {e}")
            return None
    
    def _is_likely_header_or_footer(self, line: str) -> bool:
        """Check if line is likely a header or footer to exclude from entity name."""
        line_lower = line.lower()
        exclude_patterns = [
            'page', 'commande', 'livraison', 'total', 'montant', 
            'tva', 'ht', 'ttc', 'code', 'article', 'quantité'
        ]
        return any(pattern in line_lower for pattern in exclude_patterns)
    
    def _order_extracted_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Order extracted data in a consistent format."""
        ordered_fields = ["po_number", "delivery_date", "entity_code", "entity_name"]
        ordered_data = OrderedDict()
        
        for field in ordered_fields:
            if field in data and data[field] is not None:
                ordered_data[field] = data[field]
        
        return dict(ordered_data)
    
    def validate_extraction_rules(self) -> bool:
        """Validate that extraction rules are properly configured."""
        required_rules = ["header_fuzzy"]
        missing_rules = [rule for rule in required_rules if rule not in self.extraction_rules]
        
        if missing_rules:
            logger.error(f"Missing required extraction rules: {missing_rules}")
            return False
        
        return True
    
    def get_extraction_summary(self, pdf_path: Path) -> Dict[str, Any]:
        """Get a summary of what can be extracted from the PDF."""
        text = self.extract_text_from_pdf(str(pdf_path))
        if not text:
            return {"error": "Could not extract text from PDF"}
        
        structured_data = self.extract_structured_data(text)
        table_data = self.extract_table_data(pdf_path)
        
        return {
            "file_name": pdf_path.name,
            "text_extracted": bool(text),
            "text_length": len(text) if text else 0,
            "structured_data": structured_data,
            "table_rows": len(table_data) if not table_data.empty else 0,
            "table_columns": table_data.columns.tolist() if not table_data.empty else []
        }