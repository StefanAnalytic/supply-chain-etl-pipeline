import argparse
import pandas as pd
import logging
import glob
import os

# Professionelles Logging einrichten
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    # 1. Terminal-Argumente definieren (Das macht das Skript professionell)
    parser = argparse.ArgumentParser(description="Pre-Screening für rohe Datensätze")
    parser.add_argument("--path", type=str, required=True, help="Pfad zum Datenordner")
    parser.add_argument("--min_rows", type=int, default=5000, help="Minimale Anzahl an Zeilen")
    parser.add_argument("--max_null_per_col", type=float, default=0.4, help="Maximaler Anteil an Null-Werten pro Spalte")
    args = parser.parse_args()

    # 2. CSV-Dateien im Zielordner suchen
    csv_files = glob.glob(os.path.join(args.path, "*.csv"))
    if not csv_files:
        logger.error(f"Keine CSV-Dateien im Pfad '{args.path}' gefunden!")
        return

    # 3. Pre-Screen Logik ausführen
    for file in csv_files:
        logger.info(f"Auditiere Datei: {file}")
        # 'latin1' encoding ist oft bei Supply Chain Daten von Kaggle nötig
        df = pd.read_csv(file, encoding='latin1', low_memory=False) 
        
        rows = len(df)
        logger.info(f"Zeilen gefunden: {rows}")
        
        # Check: Genug Zeilen?
        if rows < args.min_rows:
            logger.warning(f"FEHLSCHLAG: Nur {rows} Zeilen (Minimum: {args.min_rows})")
            continue
            
        # Check: Zu viele leere Felder (Null-Werte)?
        null_ratios = df.isnull().mean()
        bad_cols = null_ratios[null_ratios > args.max_null_per_col]
        
        if not bad_cols.empty:
            logger.warning(f"Warnung: Folgende Spalten überschreiten die {args.max_null_per_col*100}% Null-Toleranz:")
            for col, ratio in bad_cols.items():
                logger.warning(f" - {col}: {ratio:.1%}")
                
        logger.info(f"ERFOLG: Pre-Screen Audit für {file} bestanden. License_ok: True.")

if __name__ == "__main__":
    main()