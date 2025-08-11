# IPL Data Engineering Pipeline

**Author:** Souvind N K  
**Project:** IPL Data Engineering Pipeline (Azure + Databricks friendly)

## Project Overview
This project demonstrates an end-to-end data engineering pipeline for IPL (Indian Premier League) data.
It includes data extraction, PySpark transformation, storage in Parquet (Delta-ready), and sample SQL analytics queries.

The pipeline is designed to run locally for evaluation and can be adapted to Azure Databricks + ADLS + Synapse.

## Structure
- `extract.py` : Download or copy raw IPL CSV datasets (matches.csv, deliveries.csv). For demo, sample data is provided.
- `transform.py` : PySpark script to transform raw CSV into cleaned Parquet datasets and generate aggregates.
- `databricks_notebook_ipl_transform.py` : Databricks-friendly notebook (source format).
- `load.py` : Example upload to Azure Blob / ADLS Gen2 (uses azure-storage-blob).
- `sql_scripts/` : SQL queries for common IPL analytics reporting.
- `requirements.txt` : Python dependencies.
- `sample_data/` : Small sample matches and deliveries CSV files for local runs.
- `.gitignore` : Patterns for Git.
- `pipeline_diagram.png` : Placeholder diagram (replace with your architecture image).

## Quick Start (local)
1. Create virtualenv and install dependencies:
```bash
python -m venv venv
source venv/bin/activate   # or venv\Scripts\activate on Windows
pip install -r requirements.txt
```

2. Extract (prepare sample data):
```bash
python extract.py
# This will copy sample CSVs to data/raw/
```

3. Transform (run PySpark locally):
```bash
python transform.py --matches data/raw/matches.csv --deliveries data/raw/deliveries.csv --output data/processed
```

4. Load (upload to ADLS / Blob):
```bash
python load.py --local-path data/processed --container-name my-container --connection-string "$AZURE_STORAGE_CONNECTION_STRING"
```

## Notes for Databricks
- Import `databricks_notebook_ipl_transform.py` as a notebook or paste cells into a new notebook.
- Use cluster with Spark 3.x and Python 3.8+

## Contact
Souvind N K — souvind.souvi@gmail.com  
LinkedIn: https://www.linkedin.com/in/souvind-sajeev/
