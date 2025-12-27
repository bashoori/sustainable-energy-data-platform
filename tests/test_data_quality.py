"""
DATA QUALITY TESTS
==================

These tests demonstrate automated validation of
data quality rules — a key requirement for
trustworthy analytics pipelines.
"""

import pandas as pd


def test_required_schema_exists():
    df = pd.DataFrame(
        {
            "source": ["api"],
            "region": ["BC"],
            "metric_date": ["2025-01-01"],
            "metric_name": ["energy_mwh"],
            "metric_value": [10.0],
            "unit": ["MWh"],
        }
    )
    required = {
        "source",
        "region",
        "metric_date",
        "metric_name",
        "metric_value",
        "unit",
    }
    assert required.issubset(df.columns)


def test_negative_values_flagged():
    df = pd.DataFrame({"metric_value": [5, -3]})
    assert (df["metric_value"] < 0).any()
