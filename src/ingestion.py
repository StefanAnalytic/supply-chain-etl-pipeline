import pandas as pd
import os
import sys

def load_and_clean_data(filepath):
    # KILL-SWITCH
    if os.environ.get("DISABLE_ETL") == "1":
        print("WARN: Kill-Switch aktiv. Pipeline gestoppt.")
        sys.exit(0)
        
    # Anpassen auf unsere Supply Chain Daten (quantity gibt es nicht immer, wir nehmen lead_time)
    expected_cols = {'order_id', 'product_id', 'order_date'} 
    
    # CHUNKING & DEFENSIVE SCHEMA CHECK
    chunks = []
    print("Starte defensiven Daten-Import in Chunks...")
    for chunk in pd.read_csv(filepath, chunksize=10000):
        # Normalize ERP weirdness (macht alles klein und entfernt Leerzeichen)
        chunk.columns = chunk.columns.str.lower().str.strip() 
        
        if not expected_cols.issubset(chunk.columns):
            raise ValueError(f"Schema Drift erkannt! Erwartet: {expected_cols}, gefunden: {chunk.columns}")
        
        # Leere Zeilen bei kritischen IDs droppen
        chunk_clean = chunk.dropna(subset=['order_id', 'product_id'])
        chunks.append(chunk_clean)
        
    print("Import erfolgreich. Chunks zusammengeführt.")
    return pd.concat(chunks, ignore_index=True)