# src/importers/base.py

import pandas as pd
import logging

logger = logging.getLogger(__name__)

class BaseCSVImporter:
    def __init__(self, session):
        self.session = session

    def preview_csv(self, csv_file_path, delimiter=',', n_rows=5):
        """Preview CSV structure for debugging."""
        try:
            df = pd.read_csv(csv_file_path, delimiter=delimiter, nrows=n_rows)
            print(f"\nPreview of {csv_file_path}:")
            print(f"Columns: {list(df.columns)}")
            print(f"Shape: {df.shape}")
            print("\nFirst few rows:")
            print(df.head())
            return df
        except Exception as e:
            print(f"Error previewing {csv_file_path}: {e}")
            if delimiter == ',':
                print("Trying with semicolon delimiter...")
                return self.preview_csv(csv_file_path, delimiter=';', n_rows=n_rows)
