from __future__ import annotations

from pathlib import Path
from typing import Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from google.cloud import bigquery, storage
from google.oauth2 import service_account


class ServiceAccountMixin:
    """
    Provides BigQuery and GCS clients using an optional service account file.
    Supports service_account_path (full path) or service_account_name (file in service_accounts_dir).
    """
    service_account_path: Optional[str]
    service_account_name: Optional[str]
    service_accounts_dir: str

    def _repo_root(self):
        return Path(__file__).resolve().parents[3]

    def _resolve_service_account_file(self):
        if getattr(self, "service_account_path", None) and getattr(self, "service_account_name", None):
            raise AirflowException("Provide only one of service_account_path or service_account_name.")

        if getattr(self, "service_account_path", None):
            p = Path(self.service_account_path).expanduser().resolve()
            if not p.exists():
                raise AirflowException(f"service_account_path not found: {p}")
            return p

        name = getattr(self, "service_account_name", None)
        if not name:
            return None

        if not name.endswith(".json"):
            name = f"{name}.json"

        sa_dir = getattr(self, "service_accounts_dir", "service_accounts")
        p = self._repo_root() / sa_dir / name
        if not p.exists():
            available = [x.name for x in (self._repo_root() / sa_dir).glob("*.json")]
            raise AirflowException(
                f"Service account {name} not found in {p.parent}. Available: {available}"
            )
        return p

    def _bq_client(self, project: str):
        sa_file = self._resolve_service_account_file()
        if sa_file:
            creds = service_account.Credentials.from_service_account_file(str(sa_file))
            return bigquery.Client(project=project, credentials=creds)
        return bigquery.Client(project=project)

    def _gcs_client(self, project: Optional[str] = None):
        sa_file = self._resolve_service_account_file()
        if sa_file:
            creds = service_account.Credentials.from_service_account_file(str(sa_file))
            return storage.Client(project=project or creds.project_id, credentials=creds)
        return storage.Client(project=project)
