# Sustainable Energy Data Ingestion & Analytics Platform

## Overview
This repository demonstrates a production-style data engineering platform designed to ingest, transform, and validate sustainability and energy-related datasets from diverse external sources. The project focuses on building reliable, SQL-centric data pipelines that support analytics and product-driven decision-making in the context of the Green Economy and energy sustainability.

The system is intentionally designed to mirror real-world SaaS data platforms, handling messy external data while enforcing data quality, schema consistency, and reproducible environments.

---

## Key Capabilities
- Acquisition and ingestion of data from:
  - Government open data APIs
  - Third-party data providers
  - CSV database exports
  - Public PDF reports
- SQL-focused data modeling and transformation
- Automated data quality validation and testing
- AWS-style data lake architecture (simulated locally)
- Fully reproducible development environment using GitHub Codespaces

---

## Architecture (High-Level)

External Data Sources
├── APIs
├── CSV Exports
└── PDF Reports
↓
Ingestion Layer (Python)
↓
Raw Storage (data/raw/)
↓
Transformation Layer (Python / SQL logic)
↓
Processed Data (data/processed/)
↓
Automated Tests (Data Quality & Schema Validation)

yaml
Copy code

---

## Repository Structure

.
├── .devcontainer/ # GitHub Codespaces configuration (reproducible environment)
├── ingestion/ # Data acquisition and ingestion scripts
├── transformation/python/ # Data transformation and normalization logic
├── tests/ # Automated data quality and schema tests
├── data/ # Local data lake (raw / processed)
├── requirements.txt # Python dependencies
└── README.md

yaml
Copy code

---

## Technology Stack
- **Languages:** Python, SQL
- **Data Processing:** Pandas
- **Storage Format:** Parquet
- **Testing:** Pytest (data quality & schema validation)
- **Cloud Concepts:** AWS S3-style partitioned storage
- **Environment:** GitHub Codespaces, Dev Containers
- **Version Control:** Git, GitHub

---

## Running the Project (GitHub Codespaces)

This project is configured to run out-of-the-box in GitHub Codespaces.

1. Open the repository on GitHub  
2. Click **Code → Codespaces → Create codespace on main**
3. Wait for the environment to build automatically

Dependencies are installed automatically via `.devcontainer`.

---

## Running Locally (Optional)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install fastparquet
Run Automated Tests
bash
Copy code
pytest -q
These tests validate:

Required schema fields

Detection of invalid or unexpected values

Basic data quality guarantees

Run Data Transformation Pipeline
bash
Copy code
python transformation/python/transform_data.py \
  --dataset energy_metrics \
  --data-dir data
This will:

Load the latest raw dataset

Normalize schema and data types

Deduplicate records

Write analytics-ready output to data/processed/

Why This Project Matters
This project demonstrates hands-on experience with:

Designing and implementing data pipelines

Leading data ingestion from unreliable external sources

Applying data quality validation and automated testing

Working independently with minimal supervision

Building data systems that support sustainability-focused products

It reflects real-world database and data engineering challenges rather than toy examples.

Author
Bita Ashoori
Database / Data Engineer
Vancouver, BC

