<div align="center">

# 📦 Supply Chain ETL Pipeline

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue?logo=python&logoColor=white&style=for-the-badge)](https://www.python.org/)
[![Pandas](https://img.shields.io/badge/Data-Pandas-150458?logo=pandas&logoColor=white&style=for-the-badge)](https://pandas.pydata.org/)
[![SQL](https://img.shields.io/badge/Database-SQL-003B57?logo=sqlite&logoColor=white&style=for-the-badge)](https://en.wikipedia.org/wiki/SQL)
[![Pytest](https://img.shields.io/badge/Testing-Pytest-0A9EDC?logo=pytest&logoColor=white&style=for-the-badge)](https://docs.pytest.org/en/latest/)

**Automatisierte Bereinigung, Filterung und SQL-Vorbereitung für fehlerhafte ERP-Daten.**

*Ein defensives Daten-Framework, das fehlerhafte Supply-Chain-Daten robust für weiterführende Analysen und Datenbanken aufbereitet.*

---
</div>

## ✨ Features & Architektur

Die Pipeline schützt nachfolgende Systeme zuverlässig vor schlechter Datenqualität ("Garbage In, Garbage Out"):

| Komponente | Dateipfad | Beschreibung & Core Logic |
| :--- | :--- | :--- |
| **📥 Robust Ingestion** | `src/ingestion.py` | Sicheres Laden riesiger Datenmengen durch **Chunking**, integriertes **Schema-Drift-Handling** (fängt geänderte Spaltennamen ab) und einen dedizierten Not-Aus-Schalter. |
| **🛡️ Pre-Screening** | `src/pre_screen.py` | Die automatische Qualitätskontrolle. Prüft die Nutzbarkeit der Rohdaten und blockiert Dateien mit einer zu hohen Dichte an Nullwerten direkt am Pipeline-Eingang. |
| **🔧 ETL Guards** | `src/etl_guards.py` | Die Reparatur-Engine. Behebt defekte Datumsformate und bereinigt fehlerhafte Texteingaben (z. B. versehentliche Buchstaben) in numerischen Spalten. |
| **✅ Automated Tests** | `tests/` | Vollständige Testabdeckung mit Pytest zur kontinuierlichen Absicherung der Reparatur-Logik bei Code-Änderungen. |

---

## 🚀 Quick Start & Ausführung

<details>
<summary><b>🛠️ Installation & Ausführung (Hier klicken zum Aufklappen)</b></summary>

### 1. Abhängigkeiten installieren
```bash
pip install -r requirements.txt
