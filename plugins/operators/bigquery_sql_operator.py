from __future__ import annotations

from _base import ServiceAccountMixin
from typing import Any, Dict, Optional
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context


class BigQuerySQLRunOperator(ServiceAccountMixin, BaseOperator):
    """
    Runs a BigQuery SQL query (inline or from a .sql file) using an optional service account.
    Supports templated SQL and optional destination table settings.
    """
    template_fields = (
        "sql",
        "project",
        "location",
        "service_account_name",
        "service_account_path",
        "service_accounts_dir",
        "destination_table",
        "write_disposition",
        "create_disposition",
    )

    def __init__(
        self,
        *,
        sql: str,
        project: Optional[str] = None,
        location: Optional[str] = None,
        service_account_name: Optional[str] = None,
        service_account_path: Optional[str] = None,
        service_accounts_dir: str = "/opt/airflow/service_accounts",
        destination_table: Optional[str] = None,
        write_disposition: str = "WRITE_APPEND",
        create_disposition: str = "CREATE_IF_NEEDED",
        labels: Optional[Dict[str, str]] = None,
        job_config_overrides: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        """
        Initializes the BigQuery SQL run operator.
        Arguments:
            sql (str): SQL text or path to a .sql file
            project (str | None): BigQuery project id
            location (str | None): BigQuery job location (e.g., US or EU)
            service_account_name (str | None): service account file name
            service_account_path (str | None): full path to service account json
            service_accounts_dir (str): directory containing service accounts
            destination_table (str | None): optional destination table for results
            write_disposition (str): BigQuery write disposition
            create_disposition (str): BigQuery create disposition
            labels (dict | None): BigQuery job labels
            job_config_overrides (dict | None): extra QueryJobConfig fields to set
        """
        super().__init__(**kwargs)
        self.sql = sql
        self.project = project
        self.location = location

        self.service_account_name = service_account_name
        self.service_account_path = service_account_path
        self.service_accounts_dir = service_accounts_dir

        self.destination_table = destination_table
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition

        self.labels = labels or {}
        self.job_config_overrides = job_config_overrides or {}

    def _load_sql_text(self):
        sql = (self.sql or "").strip()

        if sql.lower().endswith(".sql"):
            p = Path(sql)
            if not p.is_absolute():
                p = (Path("/opt/airflow/dags") / p).resolve()
            if not p.exists():
                raise AirflowException(f"SQL file not found: {sql} (resolved: {p})")
            return p.read_text(encoding="utf-8").strip()

        return sql

    def execute(self, context: Context) :
        from google.cloud import bigquery

        client = self._bq_client(project=self.project)

        raw_sql = self._load_sql_text()
        query_text = self.render_template(raw_sql, context)
        self.log.info("Running BigQuery query (project=%s location=%s)", client.project, self.location)
        self.log.info("Query starts with: %r", query_text[:60])

        job_config = bigquery.QueryJobConfig()
        job_config.use_legacy_sql = False
        if self.labels:
            job_config.labels = self.labels

        if self.destination_table:
            job_config.destination = self.destination_table
            job_config.write_disposition = self.write_disposition
            job_config.create_disposition = self.create_disposition

        for k, v in self.job_config_overrides.items():
            setattr(job_config, k, v)

        self.log.info("Running BigQuery query (project=%s location=%s sa=%s)",
                      client.project, self.location, "DEFAULT")

        job = client.query(query_text, job_config=job_config, location=self.location)
        result = job.result()
        num_rows = None
        try:
            num_rows = result.total_rows
        except Exception:
            pass

        self.log.info("BigQuery job finished: job_id=%s state=%s rows=%s",
                      job.job_id, job.state, num_rows)

        return {
            "job_id": job.job_id,
            "state": job.state,
            "project": client.project,
            "location": self.location,
            "destination_table": self.destination_table,
            "rows": num_rows,
        }
