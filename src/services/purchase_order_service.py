# src/services/purchase_order_service.py
"""
Unified Purchase Order Processing Service
Handles end-to-end processing of PO PDFs with clear logging and structured output
"""

import time
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import logging
import pandas as pd
import pdfplumber
import re
from datetime import datetime

logger = logging.getLogger(__name__)


# ============================================================================
# DATA STRUCTURES
# ============================================================================

@dataclass
class POHeader:
    """Purchase Order Header Information"""
    po_number: Optional[str] = None
    delivery_date: Optional[str] = None
    entity_code: Optional[str] = None
    entity_name: Optional[str] = None
    customer_number: Optional[str] = None
    customer_name: Optional[str] = None
    mercuriale_name: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class POLine:
    """Purchase Order Line Item"""
    sku: str
    description: str
    quantity: int
    unit: Optional[str] = None
    comment: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class POProcessingResult:
    """Complete PO Processing Result"""
    success: bool
    file_name: str
    customer_format: str
    header: Optional[POHeader] = None
    lines: List[POLine] = None
    validation_stats: Dict[str, Any] = None
    error_message: Optional[str] = None
    processing_timestamp: str = None
    
    def __post_init__(self):
        if self.lines is None:
            self.lines = []
        if self.validation_stats is None:
            self.validation_stats = {}
        if self.processing_timestamp is None:
            self.processing_timestamp = datetime.now().isoformat()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "file_name": self.file_name,
            "customer_format": self.customer_format,
            "header": self.header.to_dict() if self.header else None,
            "lines": [line.to_dict() for line in self.lines],
            "validation_stats": self.validation_stats,
            "error_message": self.error_message,
            "processing_timestamp": self.processing_timestamp
        }


# ============================================================================
# EXTRACTION RULES LOADER
# ============================================================================

class ExtractionRulesLoader:
    """Loads and parses extraction rules from CSV configuration"""
    
    @staticmethod
    def load_from_csv(csv_path: Path) -> Dict[str, Dict[str, Any]]:
        """Load extraction rules from CSV file"""
        import csv
        
        rules = {}
        logger.info(f"📋 Loading extraction rules from: {csv_path}")
        
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                format_name = row['format_name']
                
                rules[format_name] = {
                    # Header extraction patterns
                    'po_number_fuzzy': row.get('po_number_fuzzy', ''),
                    'delivery_date_regex': row.get('delivery_date_regex', ''),
                    'entity_code_regex': row.get('entity_code_regex', ''),
                    'entity_name_regex': row.get('entity_name_regex', ''),
                    
                    # Table extraction rules
                    'header_fuzzy': row.get('header_fuzzy', ''),
                    'skip_footer_keywords': row.get('skip_footer_keywords', '').split(';'),
                    'min_columns': int(row.get('min_columns', 3)),
                    'fuzzy_threshold': float(row.get('fuzzy_threshold', 0.8)),
                    
                    # Column mapping
                    'column_description': row.get('column_description', '').split(';'),
                    'column_sku': row.get('column_sku', '').split(';'),
                    'column_quantity': row.get('column_quantity', '').split(';'),
                    'column_unit': row.get('column_unit', '').split(';'),
                    
                    # Customer matching
                    'customer_matching_strategies': row.get('customer_matching_strategies', '').split(';'),
                    'company_name_patterns': row.get('company_name_patterns', '').split(';')
                }
                
                logger.info(f"✅ Loaded rules for format: {format_name}")
        
        return rules


# ============================================================================
# PDF EXTRACTION ENGINE
# ============================================================================

class PDFExtractor:
    """Handles PDF text and table extraction with detailed logging"""
    
    def __init__(self, rules: Dict[str, Any]):
        self.rules = rules
        logger.info(f"🔧 PDFExtractor initialized with rules: {list(rules.keys())[:5]}...")
    
    def extract_full_text(self, pdf_path: Path) -> str:
        """Extract all text from PDF"""
        logger.info(f"📄 Extracting text from: {pdf_path.name}")
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                text_parts = []
                for i, page in enumerate(pdf.pages, 1):
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
                        logger.debug(f"  Page {i}: {len(page_text)} chars")
                
                full_text = "\n".join(text_parts)
                logger.info(f"✅ Extracted {len(full_text)} total characters from {len(text_parts)} pages")
                return full_text
        except Exception as e:
            logger.error(f"❌ Text extraction failed: {e}")
            return ""
    
    def extract_header_info(self, text: str) -> POHeader:
        """Extract header information using regex patterns"""
        logger.info("🔍 Extracting header information...")
        
        header = POHeader()
        
        # PO Number
        po_pattern = self.rules.get('po_number_fuzzy', '')
        if po_pattern:
            match = re.search(rf"{re.escape(po_pattern)}\s*[:\s]*([^\n]+)", text, re.IGNORECASE)
            if match:
                header.po_number = match.group(1).strip()
                logger.info(f"  ✓ PO Number: {header.po_number}")
            else:
                logger.warning(f"  ✗ PO Number not found (pattern: {po_pattern})")
        
        # Delivery Date
        date_pattern = self.rules.get('delivery_date_regex', '')
        if date_pattern:
            match = re.search(date_pattern, text, re.IGNORECASE)
            if match:
                header.delivery_date = match.group(1) if match.lastindex else match.group(0)
                logger.info(f"  ✓ Delivery Date: {header.delivery_date}")
            else:
                logger.warning(f"  ✗ Delivery Date not found (pattern: {date_pattern})")
        
        # Entity Code
        entity_code_pattern = self.rules.get('entity_code_regex', '')
        if entity_code_pattern:
            match = re.search(entity_code_pattern, text, re.IGNORECASE)
            if match:
                header.entity_code = match.group(1) if match.lastindex else match.group(0)
                logger.info(f"  ✓ Entity Code: {header.entity_code}")
            else:
                logger.warning(f"  ✗ Entity Code not found (pattern: {entity_code_pattern})")
        
        # Entity Name
        entity_name_pattern = self.rules.get('entity_name_regex', '')
        if entity_name_pattern:
            match = re.search(entity_name_pattern, text, re.IGNORECASE)
            if match:
                header.entity_name = match.group(1) if match.lastindex else match.group(0)
                logger.info(f"  ✓ Entity Name: {header.entity_name}")
            else:
                logger.warning(f"  ✗ Entity Name not found (pattern: {entity_name_pattern})")
        
        return header
    
    def extract_table_data(self, pdf_path: Path) -> pd.DataFrame:
        """Extract table data from PDF with fuzzy header matching"""
        logger.info(f"📊 Extracting table data from: {pdf_path.name}")
        
        header_fuzzy = self.rules.get('header_fuzzy', '')
        min_columns = self.rules.get('min_columns', 3)
        skip_keywords = self.rules.get('skip_footer_keywords', [])
        fuzzy_threshold = self.rules.get('fuzzy_threshold', 0.8)
        
        logger.info(f"  Rules: header_fuzzy='{header_fuzzy}', min_columns={min_columns}, threshold={fuzzy_threshold}")
        
        try:
            all_rows = []
            header_row = None
            
            with pdfplumber.open(pdf_path) as pdf:
                logger.info(f"  📖 PDF has {len(pdf.pages)} pages")
                
                for page_num, page in enumerate(pdf.pages, 1):
                    logger.info(f"  📄 Processing page {page_num}...")
                    tables = page.extract_tables()
                    
                    if not tables:
                        logger.warning(f"    ⚠️  No tables found on page {page_num}")
                        continue
                    
                    logger.info(f"    Found {len(tables)} table(s)")
                    
                    for table_idx, table in enumerate(tables, 1):
                        logger.info(f"    📋 Table {table_idx}: {len(table)} rows")
                        
                        for row_idx, row in enumerate(table):
                            if not row or len([c for c in row if c]) < min_columns:
                                continue
                            
                            # Check for footer keywords
                            first_cell = str(row[0] or '').lower()
                            if any(kw.lower() in first_cell for kw in skip_keywords):
                                logger.info(f"      🛑 Footer detected at row {row_idx}: '{first_cell[:30]}'")
                                break
                            
                            # Try to detect header row
                            if header_row is None and header_fuzzy:
                                row_text = ' '.join([str(c) for c in row if c])
                                if self._fuzzy_match(row_text, header_fuzzy, fuzzy_threshold):
                                    header_row = [str(c).strip() if c else '' for c in row]
                                    logger.info(f"      ✅ Header row detected: {header_row}")
                                    continue
                            
                            # Add data row
                            all_rows.append([str(c).strip() if c else '' for c in row])
            
            if not all_rows:
                logger.warning("⚠️  No data rows extracted")
                return pd.DataFrame()
            
            # Create DataFrame
            if header_row:
                df = pd.DataFrame(all_rows, columns=header_row)
                logger.info(f"✅ Created DataFrame with detected header: {header_row}")
            else:
                logger.warning("⚠️  No header row detected, using first row as header")
                df = pd.DataFrame(all_rows[1:], columns=all_rows[0])
            
            logger.info(f"✅ Extracted table: {df.shape[0]} rows × {df.shape[1]} columns")
            logger.info(f"   Columns: {df.columns.tolist()}")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Table extraction failed: {e}", exc_info=True)
            return pd.DataFrame()
    
    @staticmethod
    def _fuzzy_match(text1: str, text2: str, threshold: float = 0.8) -> bool:
        """Fuzzy string matching"""
        from difflib import SequenceMatcher
        ratio = SequenceMatcher(None, text1.lower(), text2.lower()).ratio()
        return ratio >= threshold


# ============================================================================
# DATA MAPPER
# ============================================================================

class DataMapper:
    """Maps extracted data to standardized format"""
    
    def __init__(self, rules: Dict[str, Any]):
        self.rules = rules
    
    def map_table_to_lines(self, df: pd.DataFrame) -> List[POLine]:
        """Map DataFrame to list of POLine objects"""
        if df.empty:
            logger.warning("⚠️  Empty DataFrame, no lines to map")
            return []
        
        logger.info(f"🗺️  Mapping {len(df)} rows to POLine objects...")
        
        # Find matching columns
        sku_col = self._find_column(df.columns, self.rules.get('column_sku', []))
        desc_col = self._find_column(df.columns, self.rules.get('column_description', []))
        qty_col = self._find_column(df.columns, self.rules.get('column_quantity', []))
        unit_col = self._find_column(df.columns, self.rules.get('column_unit', []))
        
        logger.info(f"  Column mapping:")
        logger.info(f"    SKU: {sku_col}")
        logger.info(f"    Description: {desc_col}")
        logger.info(f"    Quantity: {qty_col}")
        logger.info(f"    Unit: {unit_col}")
        
        if not all([sku_col, desc_col, qty_col]):
            logger.error("❌ Missing required columns for mapping")
            return []
        
        lines = []
        for idx, row in df.iterrows():
            try:
                sku = str(row[sku_col]).strip()
                description = str(row[desc_col]).strip()
                quantity = self._parse_quantity(row[qty_col])
                unit = str(row[unit_col]).strip() if unit_col else None
                
                if description and quantity > 0:
                    lines.append(POLine(
                        sku=sku if sku not in ['', 'nan', 'None'] else '000000',
                        description=description,
                        quantity=quantity,
                        unit=unit
                    ))
            except Exception as e:
                logger.warning(f"  ⚠️  Row {idx} mapping failed: {e}")
                continue
        
        logger.info(f"✅ Mapped {len(lines)} valid lines")
        return lines
    
    @staticmethod
    def _find_column(columns: List[str], possible_names: List[str]) -> Optional[str]:
        """Find matching column using fuzzy matching"""
        for col in columns:
            for name in possible_names:
                if DataMapper._fuzzy_match(col, name, 0.8):
                    return col
        return None
    
    @staticmethod
    def _fuzzy_match(text1: str, text2: str, threshold: float) -> bool:
        from difflib import SequenceMatcher
        return SequenceMatcher(None, text1.lower(), text2.lower()).ratio() >= threshold
    
    @staticmethod
    def _parse_quantity(value: Any) -> int:
        """Parse quantity value to integer"""
        try:
            # Handle string with commas and decimals
            if isinstance(value, str):
                value = value.replace(',', '.').strip()
                value = re.sub(r'[^\d.]', '', value)
            return int(float(value)) if value else 0
        except:
            return 0


# ============================================================================
# MAIN SERVICE
# ============================================================================


class PurchaseOrderService:
    """Main service for processing purchase orders"""
    
    def process_file(self, file_path: Path, customer_format: str) -> POProcessingResult:
        start_time = time.time()  # 👈 START TIMER
        
        logger.info(f"\n{'='*80}")
        logger.info(f"🔄 PROCESSING: {file_path.name}")
        logger.info(f"   Format: {customer_format}")
        logger.info(f"{'='*80}\n")
        
        if customer_format not in self.rules_config:
            error = f"Unknown customer format: {customer_format}"
            logger.error(f"❌ {error}")
            return POProcessingResult(
                success=False,
                file_name=file_path.name,
                customer_format=customer_format,
                error_message=error
            )

        rules = self.rules_config[customer_format]

        try:
            extractor = PDFExtractor(rules)
            mapper = DataMapper(rules)

            full_text = extractor.extract_full_text(file_path)
            if not full_text:
                raise ValueError("No text could be extracted from PDF")

            header = extractor.extract_header_info(full_text)
            df = extractor.extract_table_data(file_path)
            lines = mapper.map_table_to_lines(df)

            processing_duration = round(time.time() - start_time, 3)  # 👈 END TIMER

            result = POProcessingResult(
                success=True,
                file_name=file_path.name,
                customer_format=customer_format,
                header=header,
                lines=lines,
                validation_stats={
                    'total_lines': len(lines),
                    'table_rows_extracted': len(df),
                    'columns_found': df.columns.tolist() if not df.empty else []
                }
            )

            # Add processing duration to result for database save
            result.validation_stats["processing_duration"] = processing_duration

            logger.info(f"\n{'='*80}")
            logger.info(f"✅ SUCCESS: {file_path.name}")
            logger.info(f"   Lines: {len(lines)}")
            logger.info(f"   Duration: {processing_duration:.3f} sec")  # 👈 LOG IT
            logger.info(f"{'='*80}\n")

            return result

        except Exception as e:
            logger.error(f"❌ FAILED: {file_path.name} — {e}", exc_info=True)
            return POProcessingResult(
                success=False,
                file_name=file_path.name,
                customer_format=customer_format,
                error_message=str(e)
            )


# ============================================================================
# DATABASE INTEGRATION (Optional)
# ============================================================================

class DatabaseIntegration:
    """Helper for saving PO results to database"""
    
    def __init__(self, db_service):
        self.db_service = db_service
    
    def save_result(self, result: POProcessingResult) -> Dict[str, Any]:
        """Save processing result to database"""
        if not result.success:
            return {"saved": False, "error": result.error_message}
        
        try:
            with self.db_service.get_session() as session:
                from models.models import PurchaseOrder, PurchaseOrderLine

                po = PurchaseOrder(
                    po_number=result.header.po_number,
                    delivery_date=result.header.delivery_date,
                    entity_code=result.header.entity_code,
                    entity_name=result.header.entity_name,
                    customer_number=result.header.customer_number,
                    file_name=result.file_name,
                    processing_date=datetime.utcnow(),
                    processing_duration=result.validation_stats.get("processing_duration")  # 👈 NEW
                )
                session.add(po)
                session.flush()

                for line in result.lines:
                    po_line = PurchaseOrderLine(
                        order_id=po.id,
                        sku=line.sku,
                        description=line.description,
                        quantity=line.quantity,
                        unit=line.unit,
                        comment=line.comment
                    )
                    session.add(po_line)
                
                session.commit()
                
                logger.info(f"💾 Saved PO {po.po_number} in {po.processing_duration:.3f}s with {len(result.lines)} lines")

                return {
                    "saved": True,
                    "po_id": po.id,
                    "lines_saved": len(result.lines),
                    "processing_duration": po.processing_duration
                }

        except Exception as e:
            logger.error(f"❌ Database save failed: {e}", exc_info=True)
            return {"saved": False, "error": str(e)}
