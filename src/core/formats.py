import re
from typing import Dict, Any, Optional
from .constants import BCColumns  # Assuming BCColumns is an Enum or similar

FORMATS = {
    "Compass": {
        "restauration_group_db": "grpe_rest.csv",  # Or None if not a restauration group
        "extraction_rules": {
            "po_number_fuzzy": "Bon de commande n°",
            "delivery_date_fuzzy": [r"Date de livraison (\d{2}/\d{2}/\d{4})"],
            "entity_code_fuzzy": ["Code UR"],
            "entity_name_fuzzy": "Nom UR",
            "header_fuzzy": "Désignation produit",
            "skip_footer_keywords": ["Total", "Montant TTC"],
            "min_columns": 5,
            "fuzzy_threshold": 0.8,
        },
        "column_mapping": {
            BCColumns.DESCRIPTION: ["Désignation produit", "Libellé", "Description"],
            BCColumns.SKU: ["Réf. Fournisseur"],
            BCColumns.QUANTITY: ["Qté cde", "Qté", "Quantité", "Quantity"],
            "UNIT": ["Unité", "Unit", "U."],
        },
        "quantity_processor": lambda x: x.astype(str)
        .str.replace(",", ".", regex=False)
        .str.extract(r"(\d+(?:\.\d+)?)", expand=False)
        .fillna("0"),
        "entity_code_processor": lambda text: (
            match.group(1) if (match := re.search(r"Code UR\s*(\S+)", text)) else None
        ),
        # Customer matching strategies - try exact match first, then fallback to company name
        "customer_matching_strategies": ["exact_match", "company_name_fallback"],
        "company_name_patterns": [r"COMPASS GROUP FRANCE"],
        # Optional: Manual mapping for specific entity codes if needed
        "entity_code_mapping": {
            # "G24703": "D62001"  # Example: map extracted code to database code
        },
        "mercuriale_file_name": "mercuriale_medelys.csv",
        "po_folder_name": "Compass",
    },
    "SodexoClassique": {
        "restauration_group_db": "grpe_rest.csv",  # Or None if not a restauration group
        "extraction_rules": {
            "po_number_fuzzy": "COMMANDE N°",
            "delivery_date_fuzzy": [r"Livraison le (\d{2}/\d{2}/\d{4})"],
            "entity_code_fuzzy": [r"SO\d{4,8}", r"FR\d{4,8}"],
            "entity_name_fuzzy": "Adresse de livraison",
            "header_fuzzy": "Code Article",
            "skip_footer_keywords": ["Total commande"],
            "min_columns": 5,
            "fuzzy_threshold": 0.8,
        },
        "column_mapping": {
            BCColumns.DESCRIPTION: ["Libellé article", "Libellé", "Description"],
            BCColumns.SKU: ["Code Frn", "Code", "SKU"],
            BCColumns.QUANTITY: ["Quantité", "Qté", "Quantity"],
            "UNIT": ["Unité Cde", "Unité", "Unit", "U."],
        },
        "quantity_processor": lambda x: x.astype(str)
        .str.replace(",", ".", regex=False)
        .str.extract(r"(\d+(?:\.\d+)?)", expand=False)
        .fillna("0"),
        "entity_code_processor": lambda text: next(
            (
                match.group(0)
                for pattern in [r"SO\d{4,8}", r"FR\d{4,8}"]
                for match in [re.search(pattern, text)]
                if match
            ),
            None,
        ),
        # Sodexo uses standard exact matching
        "customer_matching_strategies": ["exact_match"],
        "mercuriale_file_name": "mercuriale_sodexo_classique.csv",
        "po_folder_name": "SodexoClassique",
    },
    "Elior": {
        "restauration_group_db": "grpe_rest.csv",  # Or None if not a restauration group
        "extraction_rules": {
            "po_number_fuzzy": "Commande n° :",
            "delivery_date_fuzzy": [r"Pour livraison le : (\d{2}/\d{2}/\d{4})"],
            "entity_code_fuzzy": [],
            "entity_name_fuzzy": [],
            "header_fuzzy": "Code Elior",
            "skip_footer_keywords": ["Total HT de la commande"],
            "min_columns": 5,
            "fuzzy_threshold": 0.8,
        },
        "column_mapping": {
            BCColumns.DESCRIPTION: ["Produit", "Libellé", "Description"],
            BCColumns.SKU: ["Code article fournisseur", "Code", "SKU"],
            BCColumns.QUANTITY: ["Quantité commandée", "Qté", "Quantity"],
            "UNIT": ["Unité", "Unit", "U."],
        },
        "quantity_processor": lambda x: x.astype(str)
        .str.extract(r"(\d+(?:,\d+)?)", expand=False)
        .fillna("0")
        .str.replace(",", ".", regex=False),
        "po_number_processor": lambda text: (
            (
                lambda match: (
                    (
                        match.group(1).strip()
                        + " "
                        + " ".join(
                            line.strip()
                            for line in text[match.end() :].splitlines()
                            if line.strip()
                            and not re.match(
                                r"^(Code|Produit|Qté|Conditionnement|Marque|Origine|Total)",
                                line,
                            )
                        )
                    )[:150]
                )
            )(match)
            if (match := re.search(r"Commande n° : ([^\n]+)", text))
            else None
        ),
        "entity_code_processor": lambda text: (
            match.group(1)
            if (match := re.search(r"(\d{6})\s+([A-Z\s]+)", text))
            else None
        ),
        "entity_name_processor": lambda text: (
            match.group(2).strip()
            if (match := re.search(r"(\d{6})\s+([A-Z\s]+)", text))
            else None
        ),
        # Elior uses standard exact matching
        "customer_matching_strategies": ["exact_match"],
        "mercuriale_file_name": "mercuriale_elior.csv",
        "po_folder_name": "Elior",
    },
    "FleurDeMets": {
        "restauration_group_db": "grpe_rest.csv",
        "extraction_rules": {
            "po_number_fuzzy": "N° Commande",
            "delivery_date_fuzzy": [r"Livraison le : (\d{2}/\d{2}/\d{4})"],
            "entity_code_fuzzy": ["Réf. client"],
            "entity_name_fuzzy": "FLEUR DE METS",
            "header_fuzzy": "Code",
            "skip_footer_keywords": ["Total", "Montant TTC", "Note"],
            "min_columns": 4,
            "fuzzy_threshold": 0.7,
        },
        "column_mapping": {
            BCColumns.DESCRIPTION: [
                "Produit / RØfØrence",
                "Produit / Référence",
                "Produit",
                "Référence",
            ],  # Added the Ø version
            BCColumns.SKU: ["Code"],
            BCColumns.QUANTITY: ["QtØ", "Qté", "Quantité"],  # Added the Ø version
            "UNIT": ["Cond.", "Conditionnement", "Unité", "Unit"],
        },
        "quantity_processor": lambda x: x.astype(str)
        .str.replace(",", ".", regex=False)
        .str.extract(r"(\d+(?:\.\d+)?)", expand=False)
        .fillna("0"),
        "entity_code_processor": lambda text: (
            match.group(1)
            if (match := re.search(r"Réf\. client\s*:\s*(\S+)", text))
            else None
        ),
        "entity_name_processor": lambda text: "FLEUR DE METS",
        "customer_matching_strategies": ["exact_match"],
        "mercuriale_file_name": "mercuriale_medelys.csv",
        "po_folder_name": "FleurDeMets",
    },
    "ButardEnescot": {
        "restauration_group_db": "grpe_rest.csv",
        "extraction_rules": {
            "po_number_fuzzy": "N° Commande",
            "delivery_date_fuzzy": [r"Livraison le :(\d{2}/\d{2}/\d{4})"],
            "entity_code_fuzzy": [r"Réf. client :(\d+)"],
            "entity_name_fuzzy": "SAS BUTARD ENESCOT",  # Updated to match PDF
            "header_fuzzy": "Code",
            "skip_footer_keywords": ["Total HT", "Total TTC", "%Rem"],
            "min_columns": 4,  # Reduced to match core columns
            "fuzzy_threshold": 0.6,  # Lowered threshold for better matching
            "junk_header_terms": [
                "entre :",
                "et :",
                "Lieu livr. :",
                "00:00",
                "Date :",
                "à partir du",
            ],
        },
        "column_mapping": {
            BCColumns.DESCRIPTION: [
                "Produit / Référence",
                "Produit/Référence",
                "Produit",
            ],
            BCColumns.SKU: ["Code"],
            BCColumns.QUANTITY: [
                "Qté",
                "QtØ",
                "Qte",
            ],  # ← Add fuzzy/garbled variants here
            "UNIT": ["Cond."],
        },
        "quantity_processor": lambda x: x.astype(str)
        .str.replace(",", ".", regex=False)
        .str.extract(r"(\d+(?:\.\d+)?)", expand=False)
        .fillna("0"),
        "entity_code_processor": lambda text: (
            match.group(1)
            if (match := re.search(r"Réf\. client :(\d+)", text))
            else None
        ),
        "entity_name_processor": lambda text: "BUTARD ENESCOT",
        "customer_matching_strategies": ["exact_match"],
        "mercuriale_file_name": "mercuriale_medelys.csv",
        "po_folder_name": "ButardEnescot",
    },
    "Cuisine De Qualité": {
        "restauration_group_db": None,  # Not a restauration group based on the document
        "extraction_rules": {
            "po_number_fuzzy": "N° Commande",
            "delivery_date_fuzzy": [r"Livraison le : (\d{2}/\d{2}/\d{4})"],
            "entity_code_fuzzy": None,  # No clear entity code pattern in this format
            "entity_name_fuzzy": "CUISINE DE QUALITE",
            "header_fuzzy": "Code",  # Full header as it appears in PDF
            "skip_footer_keywords": [
                "Note",
                "Tel :",
                "Nous vous demandons",
                "VICTOR PIRES",
            ],
            "min_columns": 3,  # Reduced from 4 since we have Code/Ref, Qty, Cond
            "fuzzy_threshold": 0.7,  # Reduced threshold for better matching
            "junk_header_terms": [
                "entre :",
                "et :",
                "Lieu livr. :",
                "00:00",
                "Date :",
                "à partir du",
            ],
        },
        "column_mapping": {
            BCColumns.DESCRIPTION: ["Produit / Référence"],  # Product codes/references
            BCColumns.SKU: ["Code"],
            BCColumns.QUANTITY: ["Qté", "Quantité"],
            "UNIT": ["Cond.", "Conditionnement", "unite", "kg", "litre", "gr", "btl"],
        },
        "quantity_processor": lambda x: x.astype(str)
        .str.replace(",", ".", regex=False)
        .str.extract(r"(\d+(?:\.\d+)?)", expand=False)
        .fillna("0"),
        "entity_code_processor": None,  # No entity code extraction needed
        # Customer matching strategies
        "customer_matching_strategies": ["exact_match", "company_name_fallback"],
        "company_name_patterns": [r"CUISINE DE QUALITE"],
        # No entity code mapping needed
        "entity_code_mapping": {},
        "mercuriale_file_name": "mercuriale_medelys.csv",
        "po_folder_name": "CuisineDeQualite",
    },
}
