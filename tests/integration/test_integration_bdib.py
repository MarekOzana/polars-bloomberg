from datetime import datetime

import polars as pl
import pytest

pytestmark = pytest.mark.requires_bbg


def test_bdib_live(live_bq):
    df = live_bq.bdib(
        "OMX Index",
        event_type="TRADE",
        interval=60,
        start_datetime=datetime(2025, 11, 5),
        end_datetime=datetime(2025, 11, 5, 12),
    )

    assert isinstance(df, pl.DataFrame)
    assert df.columns == [
        "security",
        "time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "numEvents",
        "value",
    ]
    assert df["time"].to_list() == sorted(df["time"].to_list())
