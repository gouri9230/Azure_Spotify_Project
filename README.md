# Spotify Data Lakehouse Pipeline
An end-to-end production-grade data engineering pipeline built on **Azure** and **Databricks**, implementing the **Medallion Architecture** (Bronze → Silver → Gold) with real-time incremental loading, data governance, CI/CD, and automated alerting.

## Project Overview
This project simulates a real-world data engineering workflow using a **Spotify dataset** as the data source. The pipeline ingests raw data from **Azure SQL Database**, processes it through three medallion layers using **Azure Data Factory**, **Databricks**, and **PySpark**, and produces analytics-ready Gold tables using **Delta Live Tables**.

Key features:
- Incremental and idempotent data loading using **Spark Structured Streaming** and **Autoloader**
- Automatic **schema evolution** handling with zero manual intervention
- **SCD Type 2** history tracking via Delta Live Tables auto CDC flow
- Centralised data governance using **Databricks Unity Catalog**
- **Metadata-driven**, Jinja-templated transformation framework — no repetitive per-table code
- **CI/CD** via Databricks Asset Bundles and Git
- Automated **email alerting** on pipeline failure via Azure Logic Apps

## Architecture

```
Azure SQL DB (Source)
        │
        ▼
Azure Data Factory (ADF)
  └── Parameterized Pipeline (8 activities)
  └── JSON-driven incremental config
        │
        ▼
ADLS Gen2 ──────────────────────────────────────────────────────────
        │                                                           │
  [BRONZE LAYER]                                              [Unity Catalog]
  Raw Parquet files                                     Metastore + External Locations
        │                                                  Security Groups + Schemas
        ▼
  [SILVER LAYER] — Databricks
  Spark Structured Streaming + Autoloader
  Schema Evolution + Idempotent loading
  Jinja-templated metadata-driven transforms
  Custom utility functions (utils/)
  Delta format output
        │
        ▼
  [GOLD LAYER] — Delta Live Tables
  Declarative DLT pipelines
  Auto CDC → SCD Type 2
  Data Quality Expectations
  Star Schema (4 dims + 1 fact)
        │
        ▼
  Analytics / Reporting
```
## Tech Stack

| Category | Tools |
|---|---|
| Cloud | Microsoft Azure |
| Ingestion | Azure Data Factory (ADF), Autoloader |
| Storage | Azure Data Lake Storage Gen2 (ADLS Gen2), Azure SQL Database |
| Processing | Apache Spark, PySpark, Spark Structured Streaming |
| Lakehouse | Databricks, Delta Lake, Delta Live Tables (DLT) |
| Data Modeling | Star Schema, SCD Type 2 |
| Governance | Databricks Unity Catalog, Unity Metastore |
| Transformation | Jinja Templates, Python, SQL |
| CI/CD | Databricks Asset Bundles, Git |
| Monitoring | Azure Logic Apps |
| Format | Parquet (Bronze), Delta (Silver & Gold) |

---
## Project Structure

```
spotify-data-lakehouse/
│
├── adf/
│   ├── pl_incremental_ingestion.json    # ADF pipeline definitions
|       └── logic_app activity           # Azure Logic Apps email alert definition
│   └── parameter_tableList.json         # Parameter value definition for dynamic & automatic processing of tables
│
├── databricks/
│   ├── silver/
│   │   ├── autoloader/         # Autoloader + Structured Streaming notebooks
│   │   ├── transformations/    # Table-level transformation logic
│   │   └── utils/              # Custom utility functions (e.g. drop_columns)
│   ├── gold/
│   │   └── dlt_pipelines/      # Delta Live Tables Python definitions
│   └── databricks.yaml         # Databricks Asset Bundle config
│
├── unity_catalog/
│   └── setup/                  # Metastore, schemas, external locations setup
│
├── media/
│   └── screenshots/            # pipeline screenshots & video
│
└── README.md
```
## Pipeline Details

### Bronze Layer

- **Source:** Azure SQL Database (Spotify dataset)
- **Tool:** Azure Data Factory with 8 parameterized activities
- **Loading strategy:** JSON config file drives incremental load logic - no watermark tables or stored procedures needed
- **Output:** Raw data stored in **Parquet format** in ADLS Gen2 Bronze container

### Silver Layer

- **Tool:** Databricks (PySpark + Spark Structured Streaming)
- **Ingestion:** Autoloader reads new files from Bronze incrementally
- **Schema evolution:** Automatically handled - no pipeline changes needed when source schema changes
- **Idempotency:** Spark Structured Streaming ensures no duplicate processing
- **Transformations applied:**
  - Capitalize `user_name` column in dimension user table
  - Remove special characters from `track_name` column
  - Create new `durationFlag` column: `Low` (<100s), `Medium` (<300s), `High` (>300s)
  - Drop auto-generated `_rescued_data` column from all tables
- **Metadata-driven framework:** Jinja templates dynamically generate SQL and PySpark transformation logic - eliminating repetitive per-table code
- **Custom utilities:** `utils/` folder contains reusable functions (e.g. `dropColumns()`)
- **Output:** Delta format tables registered in Unity Catalog Silver schema

### Gold Layer

- **Tool:** Delta Live Tables (DLT) - declarative pipeline
- **Pattern:** Staging area from Silver → Gold tables
- **SCD Type 2:** Auto CDC flow automatically generates history columns and tracks changes over time
- **Output:** Star schema with 4 dimension tables and 1 fact table registered in Unity Catalog Gold schema
  
## Data Model

**Star Schema** built on Spotify data:

```
          ┌─────────────────┐
          │   DimUser       │
          └────────┬────────┘
                   │
┌──────────┐  ┌────▼──────────┐  ┌──────────────┐
│DimTrack  │──│   FactStream  │──│ DimArtist    │
└──────────┘  └───────────────┘  └──────────────┘
                   │
          ┌────────▼────────┐
          │  DimDate        │
          └─────────────────┘
```

| Table | Type | Description |
|---|---|---|
| `DimUser` | Dimension | User profile data |
| `DimTrack` | Dimension | Track metadata with duration category |
| `DimArtist` | Dimension | Artist information |
| `DimDate` | Dimension | Date details |
| `FactStream` | Fact | Streaming events linking all dimensions |

All dimension tables include **SCD Type 2** history columns generated automatically by DLT auto CDC.

## Data Quality

Data quality validation is enforced in the **Gold layer** using **Delta Live Tables Expectations**
- Rules are defined declaratively in DLT pipeline code
- Invalid records are **dropped** from the pipeline (`expect_or_drop`)
- Keeps Gold layer clean and analytics-ready

## CI/CD & Deployment

- Pipeline code is packaged using **Databricks Asset Bundles** (`databricks.yaml`)
- All code is version-controlled and pushed to **GitHub**
- Asset Bundle enables consistent deployment across environments

## Monitoring & Alerting

- **Azure Logic Apps** monitors ADF pipeline runs
- On pipeline **failure**, Logic Apps triggers an HTTP API call
- An **automated email** is sent to the pipeline owner with failure status and details

## Setup & Installation

### Prerequisites
- Azure subscription
- Databricks workspace
- Azure Data Factory instance
- Azure Data Lake Storage Gen2

### Steps

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/Azure_Spotify_Project.git
cd Azure_Spotify_Project
```
2. **Set up Azure resources**
   - Create ADLS Gen2 storage account with Bronze, Silver, Gold containers
   - Create Azure SQL Database and load Spotify dataset
   - Configure ADF linked services and datasets
3. **Configure Unity Catalog**
   - Create access connector in Azure
   - Set up Databricks metastore linked to ADLS Gen2
   - Create external locations for Bronze, Silver, Gold
   - Create catalog schemas: `silver`, `gold`
4. **Deploy ADF Pipeline**
   - Import pipeline JSON from `adf/pipeline/`
   - Update linked service connection strings
   - Configure incremental JSON config file
5. **Run Silver layer notebooks**
   - Upload notebooks from `databricks/spotify_bundle/src/silver/` to Databricks workspace
   - Run Autoloader notebook to begin incremental ingestion
6. **Deploy Gold DLT Pipeline**
   - Create a Delta Live Tables pipeline in Databricks
   - Point to `databricks/spotify_bundle/src/gold/dlt/` notebooks
   - Run pipeline — SCD Type 2 and expectations applied automatically
7. **Deploy Asset Bundle**
```bash
databricks bundle deploy
databricks bundle run
```
## What did I learn from this project

- Designing and implementing **Medallion Architecture** on Azure from scratch
- Using **Spark Structured Streaming** and **Autoloader** for idempotent, schema-evolution-aware incremental ingestion
- Building **metadata-driven pipelines** with Jinja templates to eliminate repetitive transformation code
- Managing **Unity Catalog** — metastore setup, external locations, security groups, and centralized governance
- Implementing **SCD Type 2** automatically using Delta Live Tables auto CDC flow
- Enforcing **data quality** declaratively using DLT Expectations
- Packaging and deploying pipelines using **Databricks Asset Bundles** for CI/CD
- Setting up **automated alerting** with Azure Logic Apps on pipeline failure
