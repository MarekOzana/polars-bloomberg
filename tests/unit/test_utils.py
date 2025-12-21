from datetime import UTC, datetime

import polars as pl
import pytest

from polars_bloomberg import BQuery
from polars_bloomberg.plbbg import SITable

pytestmark = pytest.mark.no_bbg


def test_format_datetime_naive():
    dt = datetime(2024, 1, 2, 9, 30)
    assert BQuery._format_datetime(dt) == "2024-01-02T09:30:00"


def test_format_datetime_tzaware():
    dt = datetime(2024, 1, 2, 9, 30, tzinfo=UTC)
    assert BQuery._format_datetime(dt) == "2024-01-02T09:30:00Z"


def test_format_datetime_passthrough_string():
    assert BQuery._format_datetime("2024-01-02T09:30:00") == "2024-01-02T09:30:00"


def test_save_debug_case_writes_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    bq = BQuery()
    tables = [
        SITable(
            name="example",
            data={"ID": ["A"], "value": [1]},
            schema={"ID": pl.Utf8, "value": pl.Int64},
        )
    ]

    bq._save_debug_case({"example": {}}, tables)

    debug_dir = tmp_path / "debug_cases"
    saved = list(debug_dir.glob("bql_parse_results_*.json"))
    assert len(saved) == 1
