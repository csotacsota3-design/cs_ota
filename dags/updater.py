import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
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
from build_ota_external_table import BuildOTAExternalTableOperator

docs = """
Recent OTA data pipeline DAG.

Builds a BigQuery external table from GCS parquet files in a requested date range.
If no files exist, the DAG stops early and routes to the no_data task.
If files exist, it loads the range into BigQuery, deduplicates events, and incrementally syncs to MySQL.

Params:
- start_date (YYYY-MM-DD): optional UTC start date
- end_date (YYYY-MM-DD): optional UTC end date
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
    dag_id="recent_updater",
    start_date=datetime(2026, 1, 1),
    description='A simple tutorial DAG',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    doc_md=docs,
    is_paused_upon_creation=True,
        params={
            "start_date": Param(
                default=None,
                type=["null", "string"],
                description="UTC start date (YYYY-MM-DD). Optional."
            ),
            "end_date": Param(
                default=None,
                type=["null", "string"],
                description="UTC end date (YYYY-MM-DD). Optional."
            ),
        },
    tags=["test", "parquet"],
) as dag):
    no_data = EmptyOperator(task_id="no_data")

    should_continue = ShortCircuitOperator(
        task_id="should_continue",
        python_callable=lambda ti: bool(ti.xcom_pull(task_ids="build_external_recent")),
    )

    build_external_recent = BuildOTAExternalTableOperator(
        task_id="build_external_recent",
        project=GCP_PROJECT,
        dataset=BIGQUERY_EXTERNAL_DS,
        table="external_ota_events_recent",
        bucket=GCS_BUCKET_NAME,
        gcs_prefix="case_data/daily",
        suffix=SUFFIX,
        range_start_date="{{ params.start_date }}",
        range_end_date="{{ params.end_date }}",
        service_account_name=f"{GCP_PROJECT}-gcs.json",
    )

    bigquery_events_data_insert = BigQuerySQLRunOperator(
        task_id="bigquery_events_data_insert",
        sql="sql/bq_insert_ota_recent_range.sql",
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
        sql="sql/bq_dedupe_ota_recent_range.sql",
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
        start_timestamp_override="{{ (params.start_date ~ 'T00:00:00+00:00') if params.start_date else '' }}"
    )

    build_external_recent >> should_continue

    should_continue >> bigquery_events_data_insert >> bigquery_events_dedupe >> load_mysql
    should_continue >> no_data



