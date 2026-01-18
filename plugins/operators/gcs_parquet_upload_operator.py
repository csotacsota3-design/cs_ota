from __future__ import annotations

import re
from _base import ServiceAccountMixin
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from airflow.models import BaseOperator
from airflow.utils.context import Context


@dataclass(frozen=True)
class UploadResult:
    uploaded: int
    skipped: int
    gcs_uris: List[str]


class GCSParquetUploadOperator(ServiceAccountMixin, BaseOperator):
    """
    Uploads local parquet files to GCS.
    Supports filename list, date range filtering, and skip-if-exists behavior.
    """
    template_fields = (
        "local_dir",
        "bucket",
        "gcs_prefix",
        "filenames",
        "date_start",
        "date_end",
        "suffix",
        "service_account_name",
        "service_account_path",
        "service_accounts_dir",
        "project",
    )

    def __init__(
        self,
        *,
        local_dir: str,
        bucket: str,
        gcs_prefix: str = "",
        project: Optional[str] = None,
        filenames: Optional[Sequence[str]] = None,
        date_start: Optional[str] = None,
        date_end: Optional[str] = None,
        suffix: Optional[str] = None,
        pattern: str = r"^(?P<date>\d{8})_(?P<suffix>[A-Za-z0-9]+)\.parquet$",
        recursive: bool = False,
        delete_local_after_upload: bool = False,
        skip_if_exists: bool = True,
        service_account_name: Optional[str] = None,
        service_account_path: Optional[str] = None,
        service_accounts_dir: str = "/opt/airflow/service_accounts",
        **kwargs,
    ):
        """
        Initializes the GCS parquet upload operator.
        Arguments:
            local_dir (str): local directory containing parquet files
            bucket (str): GCS bucket name
            gcs_prefix (str): GCS prefix to upload under
            project (str | None): GCP project id for the GCS client
            filenames (list[str] | None): exact filenames to upload
            date_start (str | None): start date filter (YYYYMMDD)
            date_end (str | None): end date filter (YYYYMMDD)
            suffix (str | None): suffix filter used with the filename pattern
            pattern (str): regex pattern for parsing date/suffix from filenames
            recursive (bool): search for parquet files recursively
            delete_local_after_upload (bool): delete local file after upload
            skip_if_exists (bool): skip upload if object already exists in GCS
            service_account_name (str | None): service account file name
            service_account_path (str | None): full path to service account json
            service_accounts_dir (str): directory containing service accounts
        """
        super().__init__(**kwargs)
        self.local_dir = local_dir
        self.bucket = bucket
        self.gcs_prefix = gcs_prefix.strip("/")
        self.project = project

        self.filenames = list(filenames) if filenames else None
        self.date_start = date_start
        self.date_end = date_end
        self.suffix = suffix
        self.pattern = pattern
        self._rx = re.compile(self.pattern)

        self.recursive = recursive
        self.delete_local_after_upload = delete_local_after_upload
        self.skip_if_exists = skip_if_exists

        self.service_account_name = service_account_name
        self.service_account_path = service_account_path
        self.service_accounts_dir = service_accounts_dir

    def _validate_yyyymmdd(self, s: str, field_name: str):
        if not re.fullmatch(r"\d{8}", s):
            raise ValueError(f"{field_name} must be YYYYMMDD (e.g., 20260117). Got: {s}")

    def _list_candidate_files(self):
        base = Path(self.local_dir)
        if not base.exists():
            raise FileNotFoundError(f"local_dir not found: {base}")

        globber = base.rglob("*.parquet") if self.recursive else base.glob("*.parquet")
        files = sorted([p for p in globber if p.is_file()])
        if not files:
            raise FileNotFoundError(f"No .parquet files found in {base} (recursive={self.recursive})")
        return files

    def _select_files(self, files: List[Path]):
        if self.filenames:
            wanted = set(self.filenames)
            selected = [p for p in files if p.name in wanted]
            missing = wanted - {p.name for p in selected}
            if missing:
                raise FileNotFoundError(f"Requested files not found in {self.local_dir}: {sorted(missing)}")
            return sorted(selected, key=lambda p: p.name)

        if self.date_start or self.date_end or self.suffix:
            if self.date_start:
                self._validate_yyyymmdd(self.date_start, "date_start")
            if self.date_end:
                self._validate_yyyymmdd(self.date_end, "date_end")

            selected: List[Path] = []
            for p in files:
                m = self._rx.match(p.name)
                if not m:
                    continue
                d = m.group("date")
                suf = m.group("suffix")
                if self.suffix and suf != self.suffix:
                    continue
                if self.date_start and d < self.date_start:
                    continue
                if self.date_end and d > self.date_end:
                    continue
                selected.append(p)

            if not selected:
                raise FileNotFoundError(
                    f"No matching parquet files in {self.local_dir} for "
                    f"range=[{self.date_start},{self.date_end}] suffix={self.suffix}"
                )
            return sorted(selected, key=lambda p: p.name)

        return files

    def _make_object_name(self, local_path: Path):
        return f"{self.gcs_prefix}/{local_path.name}" if self.gcs_prefix else local_path.name

    def execute(self, context: Context) :
        from google.cloud import storage

        client = self._gcs_client(project=self.project)

        bucket = client.bucket(self.bucket)
        selected = self._select_files(self._list_candidate_files())

        uploaded = 0
        skipped = 0
        gcs_uris: List[str] = []

        for p in selected:
            obj_name = self._make_object_name(p)
            blob = bucket.blob(obj_name)

            if self.skip_if_exists and blob.exists(client=client):
                skipped += 1
                gcs_uris.append(f"gs://{self.bucket}/{obj_name}")
                continue

            blob.upload_from_filename(str(p))
            uploaded += 1
            gcs_uris.append(f"gs://{self.bucket}/{obj_name}")

            if self.delete_local_after_upload:
                try:
                    p.unlink()
                except Exception as e:
                    self.log.warning("Failed to delete %s: %s", p, e)

        res = UploadResult(uploaded, skipped, gcs_uris)
        return {
            "uploaded": res.uploaded,
            "skipped": res.skipped,
            "gcs_uris": res.gcs_uris
        }
