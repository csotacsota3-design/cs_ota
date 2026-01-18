from __future__ import annotations

import os
from _base import ServiceAccountMixin
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path

import mysql.connector
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, Variable
from airflow.utils.context import Context
from google.cloud import bigquery

def _parse_iso_ts(ts: str):
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception as e:
        raise AirflowException(f"Invalid timestamp override {ts!r}. Use ISO8601. Error: {e}")

class BigQueryDedupToMySQLIncrementalOperator(ServiceAccountMixin, BaseOperator):
    """
    Incrementally loads deduplicated data from BigQuery into MySQL.
    Uses (request_id, timestamp) as the primary key and tracks progress using an Airflow Variable.
    """
    template_fields = ("start_timestamp_override",)

    def __init__(
        self,
        *,
        bq_project: str,
        bq_dataset: str,
        bq_table: str,
        mysql_table: str,
        chunk_size: int = 10_000,
        service_account_name: str | None = None,
        service_account_path: Optional[str] = None,
        service_accounts_dir: str = "/opt/airflow/service_accounts",
        state_variable: str = "ota_mysql_last_ts",
        start_timestamp_override: Optional[str] = None,
        create_table: bool = True,
        **kwargs: Any,
    ) -> None:
        """
        Initializes the BigQuery to MySQL incremental load operator.
        Arguments:
            bq_project (str): BigQuery project id
            bq_dataset (str): BigQuery dataset name
            bq_table (str): BigQuery table name
            mysql_table (str): target MySQL table name
            chunk_size (int): number of rows per batch
            state_variable (str): Airflow Variable used as cursor
            start_timestamp_override (str | None): optional start timestamp
            create_table (bool): create MySQL table if not exists
        """
        super().__init__(**kwargs)
        self.bq_project = bq_project
        self.bq_dataset = bq_dataset
        self.bq_table = bq_table
        self.mysql_table = mysql_table
        self.chunk_size = chunk_size
        self.service_account_name = service_account_name
        self.service_account_path = service_account_path
        self.service_accounts_dir = service_accounts_dir
        self.state_variable = state_variable
        self.start_timestamp_override = start_timestamp_override
        self.create_table = create_table

        self.columns: List[str] = [
            "request_id",
            "funnel_id",
            "session_id",
            "user_id",
            "user_agent",
            "device_type",
            "ip_address",
            "timestamp",
            "event_date",
            "page_name",
            "subscriber_id",
            "has_email_contact_permission",
            "has_phone_contact_permission",
            "hotel_price_raw",
            "hotel_price",
            "hotel_id",
            "currency",
            "country",
            "utm_source",
            "search_query",
            "num_guests",
            "destination_id",
            "hotel_name",
            "hotel_rating",
            "selected_room_id",
            "nights",
            "price_per_night",
            "payment_status",
            "confirmation_number",
            "source_day",
            "source_file",
            "ingested_at",
        ]
    def _mysql_env(self) :
        host = os.getenv("ETL_MYSQL_HOST", "mysql")
        port = int(os.getenv("ETL_MYSQL_PORT", "3306"))
        db = os.getenv("ETL_MYSQL_DB", "etl_db")
        user = os.getenv("ETL_MYSQL_USER", "etl_user")
        password = os.getenv("ETL_MYSQL_PASSWORD", "etl_user")

        if not host or not db or not user:
            raise AirflowException("Missing MySQL env vars (ETL_MYSQL_HOST/DB/USER).")

        return {"host": host, "port": port, "database": db, "user": user, "password": password}

    def _mysql_connect(self):
        cfg = self._mysql_env()
        return mysql.connector.connect(
            host=cfg["host"],
            port=cfg["port"],
            user=cfg["user"],
            password=cfg["password"],
            database=cfg["database"],
            autocommit=False,
        )

    def _mysql_create_table_sql(self):
        return f"""
CREATE TABLE IF NOT EXISTS `{self.mysql_table}` (
  request_id VARCHAR(128) NOT NULL,
  funnel_id VARCHAR(128) NULL,
  session_id VARCHAR(128) NULL,
  user_id BIGINT NULL,
  user_agent TEXT NULL,
  device_type VARCHAR(64) NULL,
  ip_address VARCHAR(64) NULL,
  `timestamp` DATETIME(6) NOT NULL,
  event_date DATE NULL,

  page_name VARCHAR(255) NULL,
  subscriber_id BIGINT NULL,

  has_email_contact_permission BOOLEAN NULL,
  has_phone_contact_permission BOOLEAN NULL,

  hotel_price_raw VARCHAR(255) NULL,
  hotel_price DECIMAL(18,6) NULL,

  hotel_id BIGINT NULL,
  currency VARCHAR(16) NULL,
  country VARCHAR(64) NULL,
  utm_source VARCHAR(255) NULL,
  search_query TEXT NULL,
  num_guests BIGINT NULL,
  destination_id BIGINT NULL,
  hotel_name VARCHAR(512) NULL,
  hotel_rating DOUBLE NULL,
  selected_room_id BIGINT NULL,
  nights BIGINT NULL,
  price_per_night DOUBLE NULL,
  payment_status VARCHAR(64) NULL,
  confirmation_number VARCHAR(128) NULL,

  source_day DATE NULL,
  source_file TEXT NULL,
  ingested_at DATETIME(6) NULL,

  PRIMARY KEY (request_id, `timestamp`),
  KEY idx_ts (`timestamp`),
  KEY idx_source_day (source_day)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""".strip()

    def _bq_fetch_chunk(self, client: bigquery.Client, cursor_ts: datetime):
        table_fq = f"`{self.bq_project}.{self.bq_dataset}.{self.bq_table}`"
        sql = f"""
SELECT {", ".join(self.columns)}
FROM {table_fq}
WHERE `timestamp` > @cursor_ts
ORDER BY `timestamp` ASC
LIMIT @limit
"""
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("cursor_ts", "TIMESTAMP", cursor_ts),
                bigquery.ScalarQueryParameter("limit", "INT64", self.chunk_size),
            ]
        )
        return [dict(r) for r in client.query(sql, job_config=job_config).result()]

    def _normalize_dt_for_mysql(self, dtv: Any):
        if dtv is None:
            return None
        if isinstance(dtv, datetime):
            if dtv.tzinfo is not None:
                return dtv.astimezone(timezone.utc).replace(tzinfo=None)
            return dtv
        try:
            return _parse_iso_ts(str(dtv)).astimezone(timezone.utc).replace(tzinfo=None)
        except Exception:
            return None

    def _rows_to_values(self, rows: List[Dict[str, Any]]):
        values: List[Tuple[Any, ...]] = []
        max_ts: Optional[datetime] = None

        for r in rows:
            ts = self._normalize_dt_for_mysql(r.get("timestamp"))
            if ts is None:
                continue
            if max_ts is None or ts > max_ts:
                max_ts = ts

            ing = self._normalize_dt_for_mysql(r.get("ingested_at"))

            row_tuple = []
            for c in self.columns:
                if c == "timestamp":
                    row_tuple.append(ts)
                elif c == "ingested_at":
                    row_tuple.append(ing)
                else:
                    row_tuple.append(r.get(c))
            values.append(tuple(row_tuple))

        if not values or max_ts is None:
            raise AirflowException("No valid rows to load in this chunk.")
        return values, max_ts.replace(tzinfo=timezone.utc)

    def _mysql_upsert(self, cur, values: List[Tuple[Any, ...]]):
        cols = ", ".join(f"`{c}`" for c in self.columns)
        placeholders = ", ".join(["%s"] * len(self.columns))
        pk_cols = {"request_id", "timestamp"}
        updates = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in self.columns if c not in pk_cols)

        sql = f"""
INSERT INTO `{self.mysql_table}` ({cols})
VALUES ({placeholders})
ON DUPLICATE KEY UPDATE {updates};
""".strip()

        cur.executemany(sql, values)

    def execute(self, context: Context):
        override = None
        params = context.get("params") or {}
        if params.get("start_ts"):
            override = str(params["start_ts"])
        elif self.start_timestamp_override:
            override = str(self.start_timestamp_override)

        if override:
            cursor_ts = _parse_iso_ts(override)
            if cursor_ts.tzinfo is None:
                cursor_ts = cursor_ts.replace(tzinfo=timezone.utc)
            else:
                cursor_ts = cursor_ts.astimezone(timezone.utc)
            self.log.info("Using start_ts override: %s", cursor_ts.isoformat())
        else:
            last = Variable.get(self.state_variable, default_var=None)
            if last:
                cursor_ts = _parse_iso_ts(last).astimezone(timezone.utc)
                self.log.info("Using cursor from Variable %s: %s", self.state_variable, cursor_ts.isoformat())
            else:
                cursor_ts = datetime(1970, 1, 1, tzinfo=timezone.utc)
                self.log.info("No cursor found. Starting from epoch: %s", cursor_ts.isoformat())


        bq_client = self._bq_client(project=self.bq_project)

        mysql_conn = self._mysql_connect()

        try:
            with mysql_conn.cursor() as cur:
                if self.create_table:
                    cur.execute(self._mysql_create_table_sql())
                    mysql_conn.commit()

                total = 0
                while True:
                    rows = self._bq_fetch_chunk(bq_client, cursor_ts)
                    if not rows:
                        self.log.info("No more rows after %s. Total loaded=%d", cursor_ts.isoformat(), total)
                        break

                    values, new_cursor = self._rows_to_values(rows)
                    self._mysql_upsert(cur, values)
                    mysql_conn.commit()

                    total += len(values)
                    cursor_ts = new_cursor
                    Variable.set(self.state_variable, cursor_ts.isoformat())

                    self.log.info(
                        "Loaded chunk=%d total=%d new_cursor=%s",
                        len(values),
                        total,
                        cursor_ts.isoformat(),
                    )

                    if len(rows) < self.chunk_size:
                        self.log.info("Final chunk (< chunk_size). Done.")
                        break
        finally:
            mysql_conn.close()
