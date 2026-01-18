import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Param

sys.path.append(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "plugins",
        "operators",
    )
)
from parquet_daily_split_operator import ParquetDailySplitOperator
from gcs_parquet_upload_operator import GCSParquetUploadOperator
from bigquery_sql_operator import BigQuerySQLRunOperator
from bigquery_to_mysql_incremental import BigQueryDedupToMySQLIncrementalOperator

docs = """
Historic OTA data pipeline DAG.

Steps:
- Split a large parquet file into daily parquet files
- Upload daily files to Google Cloud Storage
- Load data into BigQuery external and raw tables
- Deduplicate events in BigQuery
- Incrementally load deduplicated events into MySQL
"""

INPUT_PARQUET = "/opt/airflow/data/case_data.parquet.gzip"
DAILY_OUTPUT_DIR = "/opt/airflow/data/daily_output"
GCP_PROJECT = "cs-ota"
BIGQUERY_EXTERNAL_DS = "externals"
BIGQUERY_DS = "ota_raw"
BIGQUERY_TABLE = "ota_events"
GCS_BUCKET_NAME = "cs-ota-bucket"
SUFFIX = "OTA"


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with (DAG(
    dag_id="historic_updater",
    start_date=datetime(2026, 1, 1),
    description='A simple tutorial DAG',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    doc_md=docs,
    is_paused_upon_creation=True,
    params={
        "start_ts": Param(default=None, type=["null", "string"]),
    },
    tags=["test", "parquet"],
) as dag):

    split_daily = ParquetDailySplitOperator(
        task_id="split_daily",
        input_path=INPUT_PARQUET,
        output_dir=DAILY_OUTPUT_DIR,
        timestamp_col="timestamp",
        suffix=SUFFIX,
    )

    upload_all = GCSParquetUploadOperator(
        task_id="upload_all",
        local_dir="/opt/airflow/data/daily_output",
        bucket=F"{GCS_BUCKET_NAME}",
        project=f"{GCP_PROJECT}",
        gcs_prefix="case_data/daily",
        service_account_name=f"{GCP_PROJECT}-gcs.json",
    )

    bigquery_events_data_insert = BigQuerySQLRunOperator(
        task_id="bigquery_events_data_insert",
        sql="sql/bigquery_events_data_insert.sql",
        project=GCP_PROJECT,
        service_account_name=f"{GCP_PROJECT}-bigquery.json",
        params={
            "GCP_PROJECT": GCP_PROJECT,
            "BIGQUERY_DS": BIGQUERY_DS,
            "BIGQUERY_EXTERNAL_DS": BIGQUERY_EXTERNAL_DS,
            "BIGQUERY_TABLE": BIGQUERY_TABLE,
            "GCS_BUCKET_NAME": GCS_BUCKET_NAME,
            "SUFFIX": SUFFIX,
        },
    )

    bigquery_events_dedupe = BigQuerySQLRunOperator(
        task_id="bigquery_events_dedupe",
        sql="sql/bigquery_dedupe_events.sql",
        project=GCP_PROJECT,
        service_account_name=f"{GCP_PROJECT}-bigquery.json",
        params={
            "GCP_PROJECT": GCP_PROJECT,
            "BIGQUERY_DS": BIGQUERY_DS,
            "BIGQUERY_TABLE": BIGQUERY_TABLE,
        },
    )

    load_mysql = BigQueryDedupToMySQLIncrementalOperator(
        task_id="load_mysql",
        bq_project=GCP_PROJECT,
        bq_dataset=BIGQUERY_DS,
        bq_table=f"{BIGQUERY_TABLE}_deduped",
        mysql_table="ota_events",
        service_account_name=f"{GCP_PROJECT}-bigquery.json",
        chunk_size=10_000,
        state_variable="ota_mysql_last_ts",
        create_table=True,
    )


    split_daily >> upload_all >> bigquery_events_data_insert >> bigquery_events_dedupe >> load_mysql





