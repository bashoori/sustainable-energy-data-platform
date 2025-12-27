"""
Analytics Runner
================

Purpose:
- Load processed sustainability data
- Persist it into a lightweight SQLite database
- Execute SQL-based analytics queries
- Print results for validation and review

This approach avoids external DB dependencies and works
consistently in GitHub Codespaces and CI environments.
"""

import sqlite3
from pathlib import Path
import pandas as pd


DATA_DIR = Path("data")
PROCESSED_DIR = DATA_DIR / "processed" / "energy_metrics"
DB_PATH = DATA_DIR / "analytics.db"


def load_latest_processed() -> pd.DataFrame:
    partitions = sorted(PROCESSED_DIR.glob("processed_at=*"))
    if not partitions:
        raise FileNotFoundError("No processed data found. Run transformation first.")
    latest_file = partitions[-1] / "part-000.parquet"
    return pd.read_parquet(latest_file)


def create_connection() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def load_fact_table(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    df = df[["region", "metric_date", "metric_name", "metric_value"]]
    df.to_sql(
        "fact_sustainability_metric",
        conn,
        if_exists="replace",
        index=False,
    )


def run_queries(conn: sqlite3.Connection) -> None:
    print("\n=== Total Energy Consumption by Region ===")
    q1 = """
    SELECT
        region,
        substr(metric_date, 1, 7) AS month,
        SUM(metric_value) AS total_energy_mwh
    FROM fact_sustainability_metric
    WHERE metric_name = 'energy_mwh'
    GROUP BY region, substr(metric_date, 1, 7)
    ORDER BY region, month;
    """
    print(pd.read_sql(q1, conn))

    print("\n=== Energy Trend (Month over Month) ===")
    q2 = """
    WITH monthly_energy AS (
        SELECT
            region,
            substr(metric_date, 1, 7) AS month,
            SUM(metric_value) AS energy_mwh
        FROM fact_sustainability_metric
        WHERE metric_name = 'energy_mwh'
        GROUP BY region, substr(metric_date, 1, 7)
    )
    SELECT
        region,
        month,
        energy_mwh,
        energy_mwh - LAG(energy_mwh) OVER (
            PARTITION BY region ORDER BY month
        ) AS month_over_month_change
    FROM monthly_energy
    ORDER BY region, month;
    """
    print(pd.read_sql(q2, conn))


def main() -> None:
    df = load_latest_processed()
    conn = create_connection()
    load_fact_table(conn, df)
    run_queries(conn)
    conn.close()


if __name__ == "__main__":
    main()
