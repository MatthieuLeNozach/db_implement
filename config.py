import os
from dotenv import load_dotenv

# Load .env from project root
load_dotenv()

# CSV paths
PRODUCT_CSV_PATH = os.getenv("PRODUCT_CSV_PATH")
CUSTOMER_CSV_PATH = os.getenv("CUSTOMER_CSV_PATH")

# DB URL
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///mydb.db")

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
