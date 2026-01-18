# OTA Data Pipeline

## Overview
This pipeline processes OTA event data from daily Parquet files.
Data is loaded into BigQuery, deduplicated, and then incrementally written to MySQL.
The pipeline is built with Apache Airflow and is safe for backfills and re-runs.

## Flow
1. Daily Parquet files land in Google Cloud Storage (GCS)
2. An external BigQuery table is created using only existing files
3. Data is inserted into a raw BigQuery table
4. Records are deduplicated in a second BigQuery table
5. Deduplicated data is loaded incrementally into MySQL

## Date Handling
- Optional parameters: `start_date`, `end_date`
- If not provided, the pipeline processes the most recent 3 days
- If no data exists for the selected range, the pipeline exits cleanly

## Deduplication
Duplicates are identified using:
- request_id
- funnel_id
- session_id
- page_name
- timestamp

The most recent record is kept based on ingestion time and source file.

## MySQL Loading
- Data is loaded in batches
- A timestamp offset prevents duplicate inserts
- Backfills reset the offset safely

## Key Properties
- Idempotent
- Backfill-safe
- No duplicate data


## Requirements

### Google Cloud Platform
- A GCP project must already exist  
  - Example project used in this case: `cs-ota`
- Two separate **service accounts** are required:
  - One for **BigQuery** operations
  - One for **GCS** operations
- A **JSON key file** must be generated for each service account
- The generated service account keys must be configured locally to run the project
- A dedicated **GCS bucket** must be created for storing daily Parquet files
- **Docker** must be installed to run the project locally


> **Note:**  
> The JSON files under the `service_accounts/` directory are **dummy and empty placeholders**.  
> To run the project locally, real service accounts must be created in GCP and the generated JSON keys must be placed into this directory with the required permissions.


> **Data Note:**  
> The compressed data file must be placed under the `data` directory: `data/case_data.parquet.gzip`
---




- No changes are required in:
- `.env` file
- `docker-compose.yml`

---

### Running the Project

To start all required services:

```bash
docker compose up -d