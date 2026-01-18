from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context

from _base import ServiceAccountMixin


class BuildOTAExternalTableOperator(ServiceAccountMixin, BaseOperator):
    """
    Creates or replaces a BigQuery external table from OTA parquet files in GCS.
    Selects files based on a date range and builds the table using matching URIs.
    """
    template_fields = ("range_start_date", "range_end_date")

    def __init__(
        self,
        *,
        project: str,
        dataset: str,
        table: str,
        bucket: str,
        gcs_prefix: str,
        suffix: str,
        range_start_date: str | None = None,
        range_end_date: str | None = None,
        service_account_name: str = None,
        service_account_path: str = None,
        service_accounts_dir: str = "/opt/airflow/service_accounts",
        **kwargs,
    ):
        """
        Initializes the OTA external table builder operator.
        Arguments:
            project (str): BigQuery project id
            dataset (str): BigQuery dataset name
            table (str): external table name
            bucket (str): GCS bucket name
            gcs_prefix (str): path prefix for OTA parquet files
            suffix (str): file suffix without date and extension
            range_start_date (str | None): start date (YYYY-MM-DD)
            range_end_date (str | None): end date (YYYY-MM-DD)
            service_account_name (str | None): service account file name
            service_account_path (str | None): full path to service account json
            service_accounts_dir (str): directory containing service accounts
        """
        super().__init__(**kwargs)
        self.project = project
        self.dataset = dataset
        self.table = table
        self.bucket = bucket
        self.gcs_prefix = gcs_prefix.rstrip("/")
        self.suffix = suffix
        self.range_start_date = range_start_date
        self.range_end_date = range_end_date
        self.service_account_name = service_account_name
        self.service_account_path = service_account_path
        self.service_accounts_dir = service_accounts_dir

    def _norm_date_str(self, s: str | None):
        if s is None:
            return None
        s2 = str(s).strip()
        if s2 == "" or s2.lower() in {"none", "null"}:
            return None
        return s2

    def _date_range(self):
        end_s = self._norm_date_str(self.range_end_date)
        start_s = self._norm_date_str(self.range_start_date)

        if end_s:
            end = datetime.fromisoformat(end_s).date()
        else:
            end = datetime.now(timezone.utc).date()

        if start_s:
            start = datetime.fromisoformat(start_s).date()
        else:
            start = end - timedelta(days=2)

        return [
            (start + timedelta(days=i)).strftime("%Y%m%d")
            for i in range((end - start).days + 1)
        ]

    def execute(self, context: Context):
        from google.cloud import bigquery
        from google.cloud import storage

        gcs = self._gcs_client(project=self.project)
        bq = self._bq_client(project=self.project)

        bucket = gcs.bucket(self.bucket)

        uris = []
        for day in self._date_range():
            blob_path = f"{self.gcs_prefix}/{day}_{self.suffix}.parquet"
            if bucket.blob(blob_path).exists():
                uris.append(f"gs://{self.bucket}/{blob_path}")

        if not uris:
            self.log.warning("No OTA parquet files found for requested date range.")
            return False

        self.log.info("Using %d parquet files", len(uris))
        for u in uris:
            self.log.info("  %s", u)

        uri_list = ", ".join(f"'{u}'" for u in uris)

        sql = f"""
        CREATE OR REPLACE EXTERNAL TABLE `{self.project}.{self.dataset}.{self.table}`
        OPTIONS (
          format = 'PARQUET',
          uris = [{uri_list}]
        )
        """

        job = bq.query(sql, location="US")
        job.result()

        self.log.info("External table %s.%s created", self.dataset, self.table)
        return True
