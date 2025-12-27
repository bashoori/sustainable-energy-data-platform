"""
API INGESTION MODULE
===================

Purpose
-------
Acquire, ingest, and validate structured data from REST APIs provided by
government agencies and third-party data providers.

This module is designed for environments where:
- Data sources are external and heterogeneous
- Reliability and schema consistency are required
- Raw data must be landed in cloud storage (AWS S3–style layout)

This directly supports:
- Lead acquisition and ingestion of strategic data sources
- Data ingestion from APIs
- Maintainable and auditable pipelines

Tech Stack
----------
Python, Requests, Pandas
AWS S3 (simulated via local folder structure)
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests


# -----------------------------
# Logging Configuration
# -----------------------------

LOG = logging.getLogger("api_ingestion")


def setup_logging(level: str = "INFO") -> None:
    """
    Configure structured logging.

    Logging is critical for:
    - Debugging ingestion failures
    - Auditing data acquisition
    - Production observability
    """
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


# -----------------------------
# HTTP Utilities
# -----------------------------

def request_with_retries(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    timeout_s: int = 30,
    max_retries: int = 4,
    backoff_factor: float = 1.5,
) -> requests.Response:
    """
    Perform a GET request with retry logic.

    Retries are applied for transient API failures such as:
    - Rate limiting (429)
    - Temporary server errors (5xx)

    This improves ingestion reliability when working with
    external government or third-party APIs.
    """
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout_s)
            response.raise_for_status()
            return response
        except Exception as exc:
            if attempt == max_retries:
                raise
            sleep_seconds = backoff_factor ** (attempt - 1)
            LOG.warning(
                "API request failed (attempt %s/%s). Retrying in %.1fs. Error: %s",
                attempt, max_retries, sleep_seconds, exc,
            )
            time.sleep(sleep_seconds)

    raise RuntimeError("Unreachable retry logic")


# -----------------------------
# Data Normalization
# -----------------------------

def normalize_records(
    records: List[Dict[str, Any]],
    required_columns: List[str],
) -> pd.DataFrame:
    """
    Normalize raw JSON records into a schema-consistent DataFrame.

    Responsibilities:
    - Flatten nested JSON
    - Enforce required columns
    - Handle schema drift safely
    """
    df = pd.json_normalize(records)

    # Ensure required schema exists
    for col in required_columns:
        if col not in df.columns:
            df[col] = pd.NA

    df = df[required_columns]

    # Light cleansing for string fields
    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].astype("string").str.strip()

    return df


# -----------------------------
# Storage Layer
# -----------------------------

def write_raw_dataset(
    df: pd.DataFrame,
    base_dir: Path,
    dataset_name: str,
) -> Path:
    """
    Write raw data to an S3-style partitioned directory.

    Layout:
    data/raw/<dataset_name>/ingested_at=YYYYMMDDTHHMMSSZ/part-000.parquet

    This mirrors real AWS S3 ingestion patterns.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    target_dir = base_dir / "raw" / dataset_name / f"ingested_at={timestamp}"
    target_dir.mkdir(parents=True, exist_ok=True)

    output_path = target_dir / "part-000.parquet"
    df.to_parquet(output_path, index=False)

    return output_path


# -----------------------------
# Ingestion Orchestration
# -----------------------------

def ingest_api_dataset(
    url: str,
    dataset_name: str,
    output_dir: Path,
    record_path: Optional[str] = None,
) -> Path:
    """
    Orchestrate end-to-end API ingestion.

    Steps:
    1. Acquire data from API
    2. Extract records
    3. Normalize schema
    4. Persist raw dataset
    """
    LOG.info("Starting data acquisition from API: %s", url)

    response = request_with_retries(url)
    payload = response.json()

    records: Any = payload
    if record_path:
        for key in record_path.split("."):
            records = records.get(key, {})

    if not isinstance(records, list):
        raise ValueError("Expected API response records to be a list")

    required_columns = [
        "source",
        "region",
        "metric_date",
        "metric_name",
        "metric_value",
        "unit",
    ]

    df = normalize_records(records, required_columns)
    df["source"] = url

    output_path = write_raw_dataset(df, output_dir, dataset_name)

    LOG.info("API ingestion complete. Rows=%s Path=%s", len(df), output_path)
    return output_path


# -----------------------------
# CLI Entry Point
# -----------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest data from an external API")
    parser.add_argument("--url", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--record-path", default=None)
    parser.add_argument("--log-level", default="INFO")

    args = parser.parse_args()
    setup_logging(args.log_level)

    try:
        ingest_api_dataset(
            url=args.url,
            dataset_name=args.dataset,
            output_dir=Path(args.data_dir),
            record_path=args.record_path,
        )
        return 0
    except Exception as exc:
        LOG.exception("API ingestion failed: %s", exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
