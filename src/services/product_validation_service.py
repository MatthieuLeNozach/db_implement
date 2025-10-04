# src/services/product_validation_service.py
from typing import Dict, Tuple, List, Optional
from pathlib import Path
import pandas as pd
import logging
import re

from src.core.constants import BCColumns, ValidationIssue
from src.services.database_service import DatabaseService
from src.models.product import Product
from src.models.mercuriale import MercurialeProductAssociation

logger = logging.getLogger(__name__)


class ProductValidationService:
    """Service for validating and transforming product data against mercuriale."""
    
    OUTPUT_COLUMNS = BCColumns.get_columns()

    def __init__(self, mercuriale_name: str):
        """Initialize with specific mercuriale."""
        logger.info(f"Initializing ProductValidationService for mercuriale: {mercuriale_name}")
        self.db_service = DatabaseService()
        self.mercuriale_name = mercuriale_name
        self._load_mercuriale_data()
        self._build_description_search_index()

    def _load_mercuriale_data(self) -> None:
        """Load mercuriale data from database."""
        logger.info(f"Loading mercuriale data for: {self.mercuriale_name}")
        
        with self.db_service.get_session() as session:
            # Query products with their redux coefficients for this mercuriale
            query = (
                session.query(
                    Product.sku,
                    Product.description,
                    MercurialeProductAssociation.reduxcoef
                )
                .join(MercurialeProductAssociation, Product.id == MercurialeProductAssociation.product_id)
                .join(
                    self.db_service.get_model('Mercuriale'),
                    MercurialeProductAssociation.mercuriale_id == self.db_service.get_model('Mercuriale').id
                )
                .filter(self.db_service.get_model('Mercuriale').name == self.mercuriale_name)
            )
            
            results = query.all()
            
            if not results:
                logger.warning(f"No products found for mercuriale: {self.mercuriale_name}")
                self.mercuriale_df = pd.DataFrame(columns=[BCColumns.SKU, BCColumns.DESCRIPTION, 'redux_coefficient'])
                return
            
            # Convert to DataFrame
            self.mercuriale_df = pd.DataFrame([
                {
                    BCColumns.SKU: str(result.sku).zfill(6),
                    BCColumns.DESCRIPTION: result.description or "",
                    'redux_coefficient': result.reduxcoef or 1.0
                }
                for result in results
            ])
            
            logger.info(f"Loaded {len(self.mercuriale_df)} products from mercuriale: {self.mercuriale_name}")

    def _build_description_search_index(self) -> None:
        """Build TF-IDF index for description-based product matching."""
        if self.mercuriale_df.empty:
            logger.warning("Cannot build search index: mercuriale data is empty")
            self.tfidf_vectorizer = None
            self.tfidf_matrix = None
            return
            
        logger.info("Building description search index using TF-IDF.")
        
        try:
            from sklearn.feature_extraction.text import TfidfVectorizer
            from sklearn.metrics.pairwise import cosine_similarity
            
            # Preprocess descriptions
            descriptions = self.mercuriale_df[BCColumns.DESCRIPTION].astype(str)
            cleaned_descriptions = []
            
            for desc in descriptions:
                # Remove size patterns and normalize
                cleaned = re.sub(r"\b\d+\s*(KG|G|ML|L|CM|MM)\b", "", desc, flags=re.IGNORECASE)
                cleaned = " ".join(cleaned.split()).upper()
                cleaned_descriptions.append(cleaned)
            
            # Build TF-IDF vectorizer
            custom_stop_words = [
                "KG", "G", "ML", "L", "CM", "MM", "1", "2", "3", "4", "5", 
                "10", "100", "500", "1000", "POUDRE", "LIQUIDE"
            ]
            
            self.tfidf_vectorizer = TfidfVectorizer(
                stop_words=custom_stop_words,
                ngram_range=(1, 2),
                min_df=1,
                max_features=1000,
                lowercase=True,
            )
            
            self.tfidf_matrix = self.tfidf_vectorizer.fit_transform(cleaned_descriptions)
            self.cleaned_descriptions = cleaned_descriptions
            
            logger.info(f"TF-IDF index built with {self.tfidf_matrix.shape[1]} features")
            
        except ImportError:
            logger.warning("sklearn not available. Description matching will be disabled.")
            self.tfidf_vectorizer = None
            self.tfidf_matrix = None
        except Exception as e:
            logger.error(f"Failed to build TF-IDF index: {e}")
            self.tfidf_vectorizer = None
            self.tfidf_matrix = None

    def find_product_by_description(self, description: str, threshold: float = 0.3, top_n: int = 3) -> List[Tuple[str, str, float]]:
        """Find products by description using TF-IDF similarity."""
        if not self.tfidf_vectorizer or self.tfidf_matrix is None:
            logger.debug("TF-IDF index not available for description matching")
            return []
        
        try:
            from sklearn.metrics.pairwise import cosine_similarity
            import numpy as np
            
            # Clean input description
            cleaned_desc = re.sub(r"\b\d+\s*(KG|G|ML|L|CM|MM)\b", "", description, flags=re.IGNORECASE)
            cleaned_desc = " ".join(cleaned_desc.split()).upper()
            
            # Transform and calculate similarity
            desc_vector = self.tfidf_vectorizer.transform([cleaned_desc])
            similarities = cosine_similarity(desc_vector, self.tfidf_matrix).flatten()
            
            # Get top matches
            top_indices = np.argsort(similarities)[::-1][:top_n]
            matches = []
            
            for idx in top_indices:
                similarity = similarities[idx]
                if similarity >= threshold:
                    sku = self.mercuriale_df.iloc[idx][BCColumns.SKU]
                    original_desc = self.mercuriale_df.iloc[idx][BCColumns.DESCRIPTION]
                    matches.append((sku, original_desc, similarity))
                    
            return matches
            
        except Exception as e:
            logger.error(f"Error in description matching: {e}")
            return []

    def validate_and_transform(self, df_to_validate: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
        """
        Validate and transform DataFrame against mercuriale.
        
        Returns:
            Tuple of (clean_df, faulty_df, stats)
        """
        logger.info(f"Starting validation of {len(df_to_validate)} rows against mercuriale: {self.mercuriale_name}")
        
        df_processed = df_to_validate.copy()
        
        # Ensure all required columns exist
        for col in self.OUTPUT_COLUMNS:
            if col not in df_processed.columns:
                default_value = self._get_default_value(col)
                df_processed[col] = default_value
                logger.warning(f"Missing column '{col}' added with default values")
        
        # Normalize SKUs
        df_processed = self._normalize_skus(df_processed)
        
        # Initialize tracking variables
        clean_rows_list = []
        faulty_rows_list = []
        error_counter = 0
        
        stats = {
            "total_rows": len(df_processed),
            "clean_rows": 0,
            "faulty_rows": 0,
            "issues_found": {
                "unauthorized_product": 0,
                "description_corrected": 0,
                "quantity_corrected": 0,
                "sku_inferred": 0,
                "carton_unit": 0,
            },
        }
        
        # Process each row
        for idx, row in df_processed.iterrows():
            result = self._process_row(row, idx, error_counter)
            
            if result["type"] == "clean":
                clean_rows_list.append(result["row"])
                stats["clean_rows"] += 1
                stats["issues_found"].update(result["issues"])
            else:
                # Faulty processing can return multiple rows (error + original)
                faulty_rows_list.extend(result["rows"])
                stats["faulty_rows"] += len(result["rows"])
                stats["issues_found"].update(result["issues"])
                error_counter = result["error_counter"]
        
        # Create result DataFrames
        clean_df = pd.DataFrame(clean_rows_list, columns=self.OUTPUT_COLUMNS) if clean_rows_list else pd.DataFrame(columns=self.OUTPUT_COLUMNS)
        faulty_df = pd.DataFrame(faulty_rows_list, columns=self.OUTPUT_COLUMNS) if faulty_rows_list else pd.DataFrame(columns=self.OUTPUT_COLUMNS)
        
        logger.info(f"Validation complete: {stats['clean_rows']} clean, {stats['faulty_rows']} faulty rows")
        
        return clean_df, faulty_df, stats

    def _get_default_value(self, column: str):
        """Get default value for a column."""
        defaults = {
            BCColumns.TYPE: "Article",
            BCColumns.SKU: "000000",
            BCColumns.DESCRIPTION: "",
            BCColumns.COMMENT: "",
            BCColumns.QUANTITY: 0
        }
        return defaults.get(column, "")

    def _normalize_skus(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize SKU values."""
        if BCColumns.SKU not in df.columns:
            df[BCColumns.SKU] = "000000"
            return df
        
        def normalize_sku(sku_val):
            if pd.isna(sku_val) or str(sku_val).strip() in ["", "nan", "None", "NaN"]:
                return "000000"
            try:
                numeric_val = pd.to_numeric(sku_val, errors="coerce")
                if pd.isna(numeric_val):
                    return "000000"
                return str(int(numeric_val)).zfill(6)
            except:
                return "000000"
        
        df[BCColumns.SKU] = df[BCColumns.SKU].astype(str).str.strip()
        df[BCColumns.SKU] = df[BCColumns.SKU].apply(normalize_sku)
        
        return df

    def _process_row(self, row: pd.Series, idx: int, error_counter: int) -> Dict:
        """Process a single row for validation."""
        clean_row = row.copy()
        clean_row[BCColumns.COMMENT] = ""
        
        sku_val = clean_row[BCColumns.SKU]
        description_val = clean_row[BCColumns.DESCRIPTION]
        issues_found = {
            "unauthorized_product": 0,
            "description_corrected": 0,
            "quantity_corrected": 0,
            "sku_inferred": 0,
            "carton_unit": 0,
        }
        
        # Check for carton units
        if "_unit" in row and self._is_carton_unit(row["_unit"]):
            return self._handle_carton_unit(row, idx, error_counter + 1, issues_found)
        
        # Find product in mercuriale
        mercuriale_match = self.mercuriale_df[self.mercuriale_df[BCColumns.SKU] == sku_val]
        
        # Handle missing SKU with description matching
        if mercuriale_match.empty and sku_val == "000000":
            return self._handle_missing_sku(row, idx, description_val, error_counter + 1, issues_found)
        
        # Handle unauthorized product
        if mercuriale_match.empty:
            return self._handle_unauthorized_product(row, idx, sku_val, error_counter + 1, issues_found)
        
        # Process valid product
        return self._handle_valid_product(clean_row, mercuriale_match, idx, issues_found)

    def _is_carton_unit(self, unit_val: str) -> bool:
        """Check if unit indicates carton packaging."""
        unit_str = str(unit_val).lower()
        carton_keywords = ["carton", "ctn", "ctn."]
        return any(keyword in unit_str for keyword in carton_keywords)

    def _handle_carton_unit(self, row: pd.Series, idx: int, error_counter: int, issues: Dict) -> Dict:
        """Handle rows with carton units."""
        logger.info(f"Row {idx}: Found carton unit: {row.get('_unit', 'unknown')}")
        
        # Create error row
        error_row = pd.Series(index=self.OUTPUT_COLUMNS, dtype=object)
        error_row[BCColumns.TYPE] = "Error"
        error_row[BCColumns.SKU] = str(error_counter).zfill(6)
        error_row[BCColumns.DESCRIPTION] = "Unité en carton - vérification manuelle requise"
        error_row[BCColumns.COMMENT] = ""
        error_row[BCColumns.QUANTITY] = 0
        
        # Create original row copy
        original_row = row[self.OUTPUT_COLUMNS].copy()
        original_row[BCColumns.COMMENT] = ""
        
        issues["carton_unit"] = 1
        
        return {
            "type": "faulty",
            "rows": [error_row, original_row],
            "error_counter": error_counter,
            "issues": issues
        }

    def _handle_missing_sku(self, row: pd.Series, idx: int, description_val: str, error_counter: int, issues: Dict) -> Dict:
        """Handle rows with missing SKUs using description matching."""
        logger.info(f"Row {idx}: Attempting description matching for '{description_val}'")
        
        description_matches = self.find_product_by_description(description_val, threshold=0.3, top_n=1)
        
        if not description_matches:
            return self._handle_unauthorized_product(row, idx, "000000", error_counter, issues)
        
        # Use best match
        inferred_sku, official_description, similarity = description_matches[0]
        logger.info(f"Row {idx}: Inferred SKU '{inferred_sku}' (similarity: {similarity:.3f})")
        
        # Create error row
        error_row = pd.Series(index=self.OUTPUT_COLUMNS, dtype=object)
        error_row[BCColumns.TYPE] = "Error"
        error_row[BCColumns.SKU] = str(error_counter).zfill(6)
        error_row[BCColumns.DESCRIPTION] = f"SKU inféré par description (similarité: {similarity:.2f})"
        error_row[BCColumns.COMMENT] = ""
        error_row[BCColumns.QUANTITY] = 0
        
        # Create corrected row
        corrected_row = row.copy()
        corrected_row[BCColumns.SKU] = inferred_sku
        corrected_row[BCColumns.DESCRIPTION] = official_description
        corrected_row[BCColumns.COMMENT] = ""
        
        # Apply redux coefficient
        mercuriale_match = self.mercuriale_df[self.mercuriale_df[BCColumns.SKU] == inferred_sku]
        if not mercuriale_match.empty:
            corrected_row = self._apply_redux_coefficient(corrected_row, mercuriale_match, idx)
        
        issues["sku_inferred"] = 1
        
        return {
            "type": "faulty",
            "rows": [error_row, corrected_row[self.OUTPUT_COLUMNS]],
            "error_counter": error_counter,
            "issues": issues
        }

    def _handle_unauthorized_product(self, row: pd.Series, idx: int, sku_val: str, error_counter: int, issues: Dict) -> Dict:
        """Handle unauthorized products."""
        logger.info(f"Row {idx}: SKU '{sku_val}' not authorized")
        
        # Create error row
        error_row = pd.Series(index=self.OUTPUT_COLUMNS, dtype=object)
        error_row[BCColumns.TYPE] = "Error"
        error_row[BCColumns.SKU] = str(error_counter).zfill(6)
        error_row[BCColumns.DESCRIPTION] = "Produit non autorisé"
        error_row[BCColumns.COMMENT] = ""
        error_row[BCColumns.QUANTITY] = 0
        
        # Create original row copy
        original_row = row[self.OUTPUT_COLUMNS].copy()
        original_row[BCColumns.COMMENT] = ""
        
        issues["unauthorized_product"] = 1
        
        return {
            "type": "faulty",
            "rows": [error_row, original_row],
            "error_counter": error_counter,
            "issues": issues
        }

    def _handle_valid_product(self, clean_row: pd.Series, mercuriale_match: pd.DataFrame, idx: int, issues: Dict) -> Dict:
        """Handle valid products found in mercuriale."""
        if len(mercuriale_match) > 1:
            logger.warning(f"Multiple entries for SKU '{clean_row[BCColumns.SKU]}' in mercuriale")
        
        official_description = mercuriale_match.iloc[0][BCColumns.DESCRIPTION]
        
        # Check and correct description
        if clean_row[BCColumns.DESCRIPTION].strip() != official_description.strip():
            clean_row[BCColumns.DESCRIPTION] = official_description
            issues["description_corrected"] = 1
            logger.info(f"Row {idx}: Description corrected to '{official_description}'")
        
        # Apply redux coefficient
        clean_row = self._apply_redux_coefficient(clean_row, mercuriale_match, idx, issues)
        
        return {
            "type": "clean",
            "row": clean_row[self.OUTPUT_COLUMNS],
            "issues": issues
        }

    def _apply_redux_coefficient(self, row: pd.Series, mercuriale_match: pd.DataFrame, idx: int, issues: Dict = None) -> pd.Series:
        """Apply redux coefficient to quantity."""
        if issues is None:
            issues = {}
            
        redux_coefficient = mercuriale_match.iloc[0]["redux_coefficient"]
        
        if pd.notna(redux_coefficient) and redux_coefficient != 0 and redux_coefficient != 1:
            try:
                current_quantity = pd.to_numeric(row[BCColumns.QUANTITY], errors="coerce")
                if pd.notna(current_quantity):
                    adjusted_quantity = round(current_quantity / redux_coefficient)
                    row[BCColumns.QUANTITY] = max(1, adjusted_quantity)
                    issues["quantity_corrected"] = issues.get("quantity_corrected", 0) + 1
                    logger.info(f"Row {idx}: Applied redux coefficient {redux_coefficient}")
            except Exception as e:
                logger.error(f"Row {idx}: Error applying redux coefficient: {e}")
        
        return row

    @classmethod
    def from_customer_config(cls, customer: str, formats_config: Dict, base_directory: Path = None):
        """Create validator from customer configuration."""
        logger.info(f"Creating ProductValidationService for customer: {customer}")
        
        if customer not in formats_config:
            raise ValueError(f"Customer '{customer}' not found in formats configuration")
        
        customer_config = formats_config[customer]
        mercuriale_filename = customer_config.get("mercuriale_file_name")
        
        if not mercuriale_filename:
            raise ValueError(f"No mercuriale_file_name defined for customer '{customer}'")
        
        # Extract mercuriale name from filename (remove extension)
        mercuriale_name = Path(mercuriale_filename).stem
        
        return cls(mercuriale_name)