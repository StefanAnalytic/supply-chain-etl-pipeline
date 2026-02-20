# 📦 Supply Chain ETL Pipeline

Ein Python-Projekt, das fehlerhafte Supply-Chain-Daten aus ERP-Systemen automatisch bereinigt, filtert und für SQL-Datenbanken vorbereitet.

**Tech Stack:** Python (Pandas), Pytest, SQL

## 🚀 Was der Code macht
* `src/ingestion.py`: Lädt riesige Datenmengen stückweise (Chunking), fängt geänderte Spaltennamen ab (Schema Drift) und hat einen Not-Aus-Schalter.
* `src/pre_screen.py`: Checkt, ob die Rohdaten brauchbar sind (wirft Dateien mit zu vielen leeren Feldern raus).
* `src/etl_guards.py`: Repariert kaputte Datumsformate und Text-Fehler in Zahlen-Spalten.
* `tests/`: Pytest-Skript, das automatisch checkt, ob die Reparatur-Logik funktioniert.

## ⚙️ Wie man es ausführt
```bash
# 1. Pakete installieren
pip install -r requirements.txt

# 2. Daten-Check starten
python src/pre_screen.py --path data/raw --min_rows 5000 --max_null_per_col 0.4

# 3. Code testen
pytest tests/test_etl_guards.py