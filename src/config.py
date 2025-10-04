import os
from dotenv import load_dotenv

# Load .env from project root
load_dotenv()

class Config:
    # --- File Paths ---
    PRODUCT_CSV_PATH = os.getenv("PRODUCT_CSV_PATH")
    CUSTOMER_CSV_PATH = os.getenv("CUSTOMER_CSV_PATH")
    ASSIGNMENT_RULES_CSV_PATH = os.getenv("ASSIGNMENT_RULES_CSV_PATH")

    # --- Database ---
    DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///contract_data.db")

    # --- Application Settings ---
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
