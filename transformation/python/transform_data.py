"""
TRANSFORMATION MODULE
=====================

Purpose
-------
Transform raw ingested datasets into analytics-ready,
schema-consistent data suitable for SQL-based querying.

This layer represents the transition from:
- Raw / Bronze data
to
- Clean / Silver data

Responsibilities
----------------
- Schema enforcement
- Type casting
- Deduplication
- Data quality safeguards
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

LOG = logging.getLogger("transform_data")

REQUIRED_COLUMNS = [
    "source",
    "region",
    "metric_date",
    "metric_name",
    "metric_value",
    "unit",
]


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def load_latest_raw_partition(dataset_dir: Path) -> Path:
    """
    Select the most recent ingestion partition.
    """
    partitions = sorted(dataset_dir.glob("ingested_at=*"))
    if not partitions:
        raise FileNotFoundError("No ingestion partitions found")
    return partitions[-1] / "part-000.parquet"


def transform_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply schema normalization and quality checks.
    """
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    df = df[REQUIRED_COLUMNS].copy()

    df["metric_date"] = pd.to_datetime(df["metric_date"], errors="coerce").dt.date
    df["metric_value"] = pd.to_numeric(df["metric_value"], errors="coerce")

    df = df.drop_duplicates(
        subset=["region", "metric_name", "metric_date"],
        keep="last",
    )

    df["processed_at_utc"] = datetime.now(timezone.utc).isoformat()
    return df


def write_processed_dataset(
    df: pd.DataFrame,
    base_dir: Path,
    dataset_name: str,
) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    target_dir = base_dir / "processed" / dataset_name / f"processed_at={timestamp}"
    target_dir.mkdir(parents=True, exist_ok=True)

    output_path = target_dir / "part-000.parquet"
    df.to_parquet(output_path, index=False)
    return output_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Transform raw datasets")
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--log-level", default="INFO")

    args = parser.parse_args()
    setup_logging(args.log_level)

    try:
        raw_dir = Path(args.data_dir) / "raw" / args.dataset
        raw_file = load_latest_raw_partition(raw_dir)
        df = pd.read_parquet(raw_file)

        transformed = transform_dataset(df)
        out_path = write_processed_dataset(transformed, Path(args.data_dir), args.dataset)

        LOG.info("Transformation completed. Rows=%s Output=%s", len(transformed), out_path)
        return 0
    except Exception as exc:
        LOG.exception("Transformation failed: %s", exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
