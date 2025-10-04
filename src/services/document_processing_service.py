# src/services/document_processing_service.py
from typing import Dict, Any, Optional, List
from pathlib import Path
import pandas as pd
from difflib import SequenceMatcher
import logging

from src.core.constants import ProcessingResult, BCColumns
from src.core.formats import FORMATS
from src.services.database_service import DatabaseService
from src.services.pdf_extraction_service import PDFExtractionService
from src.services.po_header_extraction_service import POHeaderExtractionService
from src.services.customer_matching_service import CustomerMatchingService
from src.repositories.customer_repository import CustomerRepository

logger = logging.getLogger(__name__)

class DocumentProcessingService:
    """Main service for processing purchase order documents (PDF/CSV) into BC format."""
    
    def __init__(self):
        """Initialize the document processing service."""
        logger.info("Initializing DocumentProcessingService.")
        self.db_service = DatabaseService()
        
    def process_document(self, file_path: Path, customer_format: str) -> ProcessingResult:
        """
        Process a document file and return structured results.
        
        Args:
            file_path: Path to the document file
            customer_format: Customer format key from FORMATS
            
        Returns:
            ProcessingResult with processed data and metadata
        """
        logger.info(f"Processing document: {file_path.name} for format: {customer_format}")
        
        try:
            # Validate format
            if customer_format not in FORMATS:
                raise ValueError(f"Unknown customer format: {customer_format}")
            
            format_spec = FORMATS[customer_format]
            
            # Extract table data
            df_raw = self._extract_table_data(file_path, format_spec)
            
            if df_raw.empty:
                logger.warning(f"No table data extracted from {file_path.name}")
                return ProcessingResult(
                    customer=customer_format,
                    file_path=file_path,
                    success=False,
                    error_message="No table data could be extracted from the document"
                )
            
            # Map to BC format
            df_bc = self._map_to_bc_format(df_raw, format_spec)
            
            # Extract header information for PDFs
            header_info = None
            if file_path.suffix.lower() == '.pdf':
                header_info = self._extract_header_info(file_path, format_spec)
            
            # Create processing result
            return ProcessingResult(
                customer=customer_format,
                file_path=file_path,
                success=True,
                processed_df=df_bc,
                stats={
                    'total_rows': len(df_bc),
                    'columns': df_bc.columns.tolist()
                },
                header_info=header_info
            )
            
        except Exception as e:
            logger.error(f"Error processing document {file_path.name}: {e}")
            return ProcessingResult(
                customer=customer_format,
                file_path=file_path,
                success=False,
                error_message=str(e)
            )
    
    def _extract_table_data(self, file_path: Path, format_spec: Dict[str, Any]) -> pd.DataFrame:
        """Extract table data from document based on file type."""
        file_extension = file_path.suffix.lower()
        
        if file_extension == ".csv":
            return self._process_csv(file_path, format_spec)
        elif file_extension == ".pdf":
            return self._process_pdf(file_path, format_spec)
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")
    
    def _process_csv(self, csv_path: Path, format_spec: Dict[str, Any]) -> pd.DataFrame:
        """Process CSV file and extract table data."""
        logger.info(f"Processing CSV file: {csv_path.name}")
        
        try:
            # Try different separators
            df = None
            for separator in [",", ";", "\t"]:
                try:
                    temp_df = pd.read_csv(csv_path, sep=separator, encoding="utf-8")
                    if len(temp_df.columns) > 1:
                        df = temp_df
                        logger.info(f"Successfully read CSV with separator '{separator}'")
                        break
                except Exception:
                    continue
            
            if df is None:
                df = pd.read_csv(csv_path, encoding="utf-8")
                logger.info("Successfully read CSV with inferred separator.")
            
            logger.info(f"CSV loaded with {len(df)} rows and columns: {df.columns.tolist()}")
            
            # Apply extraction rules if defined
            if "extraction_rules" in format_spec:
                df = self._apply_csv_extraction_rules(df, format_spec["extraction_rules"])
            
            return df.dropna(how="all").reset_index(drop=True)
            
        except Exception as e:
            logger.error(f"Error reading CSV file {csv_path.name}: {e}")
            return pd.DataFrame()
    
    def _process_pdf(self, pdf_path: Path, format_spec: Dict[str, Any]) -> pd.DataFrame:
        """Process PDF file and extract table data."""
        if "extraction_rules" not in format_spec:
            raise ValueError(f"No extraction rules defined for PDF processing")
        
        pdf_service = PDFExtractionService(format_spec["extraction_rules"])
        return pdf_service.extract_table_data(pdf_path)
    
    def _apply_csv_extraction_rules(self, df: pd.DataFrame, rules: Dict[str, Any]) -> pd.DataFrame:
        """Apply extraction rules to CSV data."""
        header_keyword = rules.get("header_fuzzy", "").lower()
        
        if header_keyword:
            header_row_idx = None
            for idx, row in df.iterrows():
                if any(
                    self._fuzzy_match(str(cell), header_keyword)
                    for cell in row
                    if pd.notna(cell)
                ):
                    header_row_idx = idx
                    logger.info(f"Found header row at index {idx}")
                    break
            
            if header_row_idx is not None:
                new_headers = [str(h).strip() for h in df.iloc[header_row_idx].tolist()]
                df = df.iloc[header_row_idx + 1:].copy()
                df.columns = new_headers
                df = df.reset_index(drop=True)
                logger.info(f"Updated headers to: {df.columns.tolist()}")
        
        return df
    
    def _extract_header_info(self, pdf_path: Path, format_spec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract header information from PDF."""
        try:
            header_service = POHeaderExtractionService(format_spec)
            return dict(header_service.extract_info(str(pdf_path)))
        except Exception as e:
            logger.error(f"Error extracting header info from {pdf_path.name}: {e}")
            return None
    
    def _map_to_bc_format(self, df: pd.DataFrame, format_spec: Dict[str, Any]) -> pd.DataFrame:
        """Map extracted data to BC format."""
        logger.info("Mapping to BC format.")
        
        if df.empty:
            logger.info("Empty raw dataframe — skipping BC mapping.")
            return pd.DataFrame(columns=BCColumns.get_columns())
        
        column_mapping = format_spec.get("column_mapping", {})
        actual_columns = df.columns.tolist()
        
        # Find matching columns
        mapped_columns = {}
        for bc_col_name, possible_names in column_mapping.items():
            actual_col = self._find_matching_column(actual_columns, possible_names)
            if actual_col:
                mapped_columns[bc_col_name] = actual_col
                logger.debug(f"Mapped '{actual_col}' -> {bc_col_name}")
            else:
                logger.warning(f"Could not find column for {bc_col_name}. Tried: {possible_names}")
        
        # Validate required columns
        required_cols = [BCColumns.SKU, BCColumns.DESCRIPTION, BCColumns.QUANTITY]
        missing_cols = [col for col in required_cols if col not in mapped_columns]
        
        if missing_cols:
            logger.error(f"Missing required columns for mapping: {missing_cols}")
            logger.error(f"Available columns: {actual_columns}")
            raise ValueError(f"Could not map required columns: {missing_cols}")
        
        # Process data
        try:
            # Process quantity
            quantity_series = df[mapped_columns[BCColumns.QUANTITY]]
            quantity_processor = format_spec.get("quantity_processor")
            
            if quantity_processor:
                logger.debug("Applying customer-specific quantity processor.")
                quantity_series = quantity_processor(quantity_series)
            else:
                logger.debug("Applying default quantity processing.")
                quantity_series = (
                    quantity_series.astype(str)
                    .replace("", "0")
                    .str.replace(",", ".", regex=False)
                    .str.extract(r"(\d+(?:\.\d+)?)", expand=False)
                    .fillna("0")
                )
            
            quantity_numeric = pd.to_numeric(quantity_series, errors="coerce").fillna(0)
            quantity_int = quantity_numeric.round().astype(int)
            
            # Process SKU
            sku_series = df[mapped_columns[BCColumns.SKU]].astype(str).str.strip()
            sku_series = sku_series.replace(["", "nan", "None", "NaN"], "000000")
            
            # Create result DataFrame
            result_df = pd.DataFrame({
                BCColumns.TYPE: "Article",
                BCColumns.SKU: sku_series,
                BCColumns.DESCRIPTION: df[mapped_columns[BCColumns.DESCRIPTION]].astype(str),
                BCColumns.COMMENT: "",
                BCColumns.QUANTITY: quantity_int,
            })
            
            # Add unit information if available
            unit_column = mapped_columns.get("UNIT")
            if unit_column:
                result_df["_unit"] = df[unit_column].astype(str).str.strip()
                logger.debug(f"Added unit information from column {unit_column}")
            
            # Filter valid rows
            result_df = result_df[
                (result_df[BCColumns.DESCRIPTION].str.strip() != "") &
                (result_df[BCColumns.QUANTITY] > 0)
            ]
            
            logger.info(f"Successfully mapped {len(result_df)} rows to BC format.")
            return result_df
            
        except Exception as e:
            logger.error(f"Error mapping to BC format: {e}")
            logger.debug(f"Mapped columns: {mapped_columns}")
            raise
    
    def _fuzzy_match(self, cell: str, keyword: str, threshold: float = 0.8) -> bool:
        """Check if cell matches keyword using fuzzy matching."""
        if not cell:
            return False
        ratio = SequenceMatcher(None, cell.lower(), keyword.lower()).ratio()
        return ratio >= threshold
    
    def _find_matching_column(self, actual_columns: List[str], possible_names: List[str], 
                             threshold: float = 0.8) -> Optional[str]:
        """Find matching column using fuzzy matching."""
        for actual_col in actual_columns:
            for possible_name in possible_names:
                if self._fuzzy_match(actual_col, possible_name, threshold):
                    return actual_col
        return None
    
    def get_supported_formats(self) -> List[str]:
        """Get list of supported customer formats."""
        return list(FORMATS.keys())
    
    def get_format_info(self, customer_format: str) -> Dict[str, Any]:
        """Get format information for a specific customer format."""
        if customer_format not in FORMATS:
            raise ValueError(f"Unknown customer format: {customer_format}")
        return FORMATS[customer_format]