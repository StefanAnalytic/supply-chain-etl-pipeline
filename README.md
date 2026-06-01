# 📦 Supply Chain ETL Pipeline

> Automatisierte Bereinigung, Filterung und SQL-Vorbereitung für fehlerhafte ERP-Daten.

**Tech Stack:** `Python` | `Pandas` | `SQL` | `Pytest`

## ✨ Features

* **📥 Robust Ingestion** (`src/ingestion.py`): Chunking, Schema-Drift-Handling und Not-Aus-Schalter für riesige Datenmengen.
* **🛡️ Pre-Screening** (`src/pre_screen.py`): Automatische Qualitätskontrolle – wirft Dateien mit zu vielen Nullwerten direkt raus.
* **🔧 ETL Guards** (`src/etl_guards.py`): Repariert defekte Datumsformate und fehlerhafte Texteingaben in Zahlen-Spalten.
* **✅ Automated Tests** (`tests/`): Vollständige Pytest-Abdeckung zur Absicherung der Reparatur-Logik.

## 🚀 Quick Start

```bash
# 1. Abhängigkeiten installieren
pip install -r requirements.txt

# 2. Daten-Qualitätscheck starten
python src/pre_screen.py --path data/raw --min_rows 5000 --max_null_per_col 0.4

# 3. Tests ausführen
pytest tests/test_etl_guards.py
