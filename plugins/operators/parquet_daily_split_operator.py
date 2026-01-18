from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
from airflow.models import BaseOperator
from airflow.utils.context import Context


class ParquetDailySplitOperator(BaseOperator):
    """
    Splits a single parquet file into daily parquet files based on a timestamp column.
    Writes one parquet file per day into the output directory.
    """
    template_fields = ("input_path", "output_dir", "suffix", "timestamp_col")

    def __init__(
        self,
        *,
        input_path: str,
        output_dir: str,
        timestamp_col: str = "timestamp",
        suffix: str = "OTA",
        engine: str = "pyarrow",
        **kwargs,
    ):
        """
        Initializes the parquet daily split operator.
        Arguments:
            input_path (str): path to the input parquet file
            output_dir (str): directory to write daily parquet files
            timestamp_col (str): column used to derive the date
            suffix (str): suffix appended to output filenames
            engine (str): parquet engine to use (e.g., pyarrow)
        """
        super().__init__(**kwargs)
        self.input_path = input_path
        self.output_dir = output_dir
        self.timestamp_col = timestamp_col
        self.suffix = suffix
        self.engine = engine

    def execute(self, context: Context) :
        in_path = Path(self.input_path)
        out_dir = Path(self.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        if not in_path.exists():
            raise FileNotFoundError(f"Input parquet not found: {in_path}")

        df = pd.read_parquet(in_path, engine=self.engine)

        if self.timestamp_col not in df.columns:
            raise ValueError(f"Column '{self.timestamp_col}' not found in parquet")

        df[self.timestamp_col] = pd.to_datetime(df[self.timestamp_col], errors="raise")
        df["_date"] = df[self.timestamp_col].dt.strftime("%Y%m%d")

        self.log.info(
            "Splitting %s rows into daily parquets by '%s' -> %s",
            len(df),
            self.timestamp_col,
            out_dir,
        )

        produced = []
        for date_key, group in df.groupby("_date", sort=True):
            out_file = out_dir / f"{date_key}_{self.suffix}.parquet"
            group.drop(columns=["_date"]).to_parquet(out_file, engine=self.engine, index=False)
            produced.append(out_file.name)

        produced.sort()

        self.log.info("Produced %d file(s). Sample: %s", len(produced), produced[:5])
        return {"output_dir": str(out_dir), "num_files": len(produced), "files_sample": produced[:10]}
