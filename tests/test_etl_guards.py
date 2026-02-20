import sys
import os

# Add project root to sys.path to allow importing 'src'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
import pandas as pd
import numpy as np
from src.etl_guards import clean_erp_extract

def test_clean_erp_extract_logic():
    # Erstelle Test-Daten
    df_test = pd.DataFrame({
        'order_id': [1, 1, 2],
        'product_id': [10, 10, 20],
        'order_date': ['2023-01-01', '2023-01-01', '01/01/2023'],
        'lead_time': ['3 days', '3 days', '5']
    })
    
    test_file = 'test_temp.csv'
    df_test.to_csv(test_file, index=False)
    
    # Ausführen der Reinigung
    cleaned_df = clean_erp_extract(test_file)
    
    # Assertions (Erwartungen prüfen)
    assert len(cleaned_df) == 2  # Duplikat sollte weg sein
    assert cleaned_df['lead_time'].iloc[0] == 3.0  # String-Artefakt weg
    
    # Aufräumen
    os.remove(test_file)

if __name__ == "__main__":
    test_clean_erp_extract_logic()
    print("Test executed successfully!")