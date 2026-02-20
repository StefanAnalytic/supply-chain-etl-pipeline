import pandas as pd
import numpy as np
import logging
from pathlib import Path
from typing import Union

# Logger konfigurieren
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_erp_extract(file_path: Union[str, Path]) -> pd.DataFrame:
    """
    Lädt und bereinigt ERP-Extrakte mit defensiven ETL-Guards.
    
    Args:
        file_path: Pfad zur rohen CSV-Datei.
        
    Returns:
        pd.DataFrame: Bereinigter DataFrame bereit für die Heuristik.
    """
    logger.info(f"Starte ETL-Prozess für: {file_path}")
    
    try:
        df = pd.read_csv(file_path, low_memory=False)
    except FileNotFoundError:
        logger.error(f"Datei nicht gefunden: {file_path}")
        raise
        
    initial_rows = len(df)
    
    # Guard 1: Date Format Anomaly Detection & Coercion
    if 'order_date' in df.columns:
        # 'mixed' ist der moderne Standard in Pandas 2.0+
        df['order_date'] = pd.to_datetime(df['order_date'], format='mixed', errors='coerce')
        logger.info("Guard 1 (Date Coercion) angewandt.")
        
    # Guard 2: String stripping from numeric fields
    if 'lead_time' in df.columns:
        df['lead_time'] = df['lead_time'].astype(str).str.extract(r'(\d+)')[0].astype(float)
        # Besser: Median nur berechnen, wenn valide Werte vorhanden sind
        median_lead_time = df['lead_time'].median()
        df['lead_time'] = df['lead_time'].fillna(median_lead_time)
        logger.info("Guard 2 (String Stripping & Imputation) angewandt.")
        
    # Defensive Deduplication
    if 'order_id' in df.columns and 'product_id' in df.columns:
        df = df.drop_duplicates(subset=['order_id', 'product_id'], keep='first')
        dropped_rows = initial_rows - len(df)
        logger.info(f"Defensive Deduplication: {dropped_rows} Duplikate entfernt.")
    
    logger.info(f"ETL erfolgreich. Finale Zeilenanzahl: {len(df)}")
    return df