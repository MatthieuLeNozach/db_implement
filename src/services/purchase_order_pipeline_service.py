# src/services/purchase_order_pipeline_service.py

from pathlib import Path
from typing import Dict, List, Optional
import logging
import pandas as pd

from core.constants import ProcessingResult, BCColumns
from core.formats import FORMATS
from .document_processing_service import DocumentProcessingService
from .po_header_extraction_service import POHeaderExtractionService
from .product_validation_service import ProductValidationService
from .database_service import DatabaseService

logger = logging.getLogger(__name__)


class PurchaseOrderPipeline:
    """
    Main orchestration service for processing purchase order documents.
    Responsibilities:
    - Discover files
    - Run processing pipeline per file
    - Aggregate and summarize results
    """

    def __init__(
        self,
        base_po_directory: Path,
        formats_config: Optional[Dict] = None,
        base_db_directory: Optional[Path] = None
    ):
        self.base_po_directory = Path(base_po_directory)
        self.base_db_directory = Path(base_db_directory) if base_db_directory else None
        self.formats_config = formats_config or FORMATS

        # Core services
        self.document_service = DocumentProcessingService()
        self.db_service = DatabaseService()
        self.results: List[ProcessingResult] = []

        logger.info(f"📁 Pipeline initialized → PO dir: {self.base_po_directory}")
        if self.base_db_directory:
            logger.info(f"📁 Database dir: {self.base_db_directory}")

    # ------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------

    def discover_files(self, customer: str) -> List[Path]:
        """Return all PO files (PDF/CSV) for a given customer."""
        config = self.formats_config.get(customer)
        if not config:
            raise ValueError(f"Customer '{customer}' not found in formats config")

        po_folder = config.get("po_folder_name")
        if not po_folder:
            raise ValueError(f"Customer '{customer}' missing 'po_folder_name'")

        customer_dir = self.base_po_directory / po_folder
        if not customer_dir.exists():
            logger.warning(f"⚠️  Directory not found: {customer_dir}")
            return []

        files = list(customer_dir.glob("*.pdf")) + list(customer_dir.glob("*.csv"))
        logger.info(f"📂 Found {len(files)} PO files for '{customer}'")
        return files

    # ------------------------------------------------------------
    # Single-file processing
    # ------------------------------------------------------------

    def process_single_file(self, file_path: Path, customer: str) -> ProcessingResult:
        """Process a single PO file end-to-end."""
        logger.info(f"🔄 Processing: {file_path.name} (customer={customer})")
        result = ProcessingResult(customer=customer, file_path=file_path, success=False)

        if not file_path.is_file():
            result.error_message = f"File not found: {file_path}"
            logger.error(result.error_message)
            return result

        try:
            # 1️⃣ Header extraction (PDF only)
            if file_path.suffix.lower() == ".pdf":
                result.header_info = self._extract_header_info(file_path, customer)

            # 2️⃣ Document parsing + BC mapping
            doc_result = self.document_service.process_document(file_path, customer)
            if not doc_result.success:
                result.error_message = doc_result.error_message
                return result

            result.processed_df = doc_result.processed_df
            result.stats = dict(doc_result.stats or {})
            logger.info(f"📊 Parsed {len(result.processed_df)} rows")

            # 3️⃣ Product validation
            clean_df, faulty_df, validation_stats = self._validate_products(doc_result.processed_df, customer)
            result.clean_df, result.faulty_df = clean_df, faulty_df
            result.stats.update(validation_stats)

            result.success = True
            logger.info(
                f"✅ Completed: {file_path.name} → Clean: {validation_stats['clean_rows']} / Faulty: {validation_stats['faulty_rows']}"
            )

        except Exception as e:
            result.error_message = f"❌ Pipeline error: {e}"
            logger.exception(result.error_message)

        return result

    # ------------------------------------------------------------
    # Batch processing
    # ------------------------------------------------------------

    def process_customer_files(self, customer: str) -> List[ProcessingResult]:
        """Process all files for a specific customer."""
        files = self.discover_files(customer)
        if not files:
            return []

        results = [self.process_single_file(file, customer) for file in files]
        self.results.extend(results)
        success_count = sum(r.success for r in results)

        logger.info(f"🏁 {customer}: {success_count}/{len(results)} files processed successfully")
        return results

    def process_all_customers(self) -> Dict[str, List[ProcessingResult]]:
        """Run pipeline for all configured customers."""
        all_results = {
            customer: self.process_customer_files(customer)
            for customer in self.formats_config.keys()
        }

        logger.info(f"🎉 All customers processed. Total files: {len(self.results)}")
        return all_results

    # ------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------

    def get_summary_report(self) -> Dict:
        """Return a high-level summary of pipeline execution."""
        total_files = len(self.results)
        success_count = sum(r.success for r in self.results)
        failed_count = total_files - success_count

        total_clean = sum(r.stats.get("clean_rows", 0) for r in self.results if r.stats)
        total_faulty = sum(r.stats.get("faulty_rows", 0) for r in self.results if r.stats)

        return {
            "total_files": total_files,
            "successful_files": success_count,
            "failed_files": failed_count,
            "total_clean_rows": total_clean,
            "total_faulty_rows": total_faulty,
            "success_rate": round(success_count / total_files * 100, 2) if total_files else 0.0,
            "customers_processed": sorted({r.customer for r in self.results})
        }
