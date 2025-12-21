from datetime import date

import polars as pl
import pytest

pytestmark = pytest.mark.requires_bbg


def test_bdp_live(live_bq):
    df = live_bq.bdp(
        ["OMX Index", "SPX Index"],
        ["PX_LAST", "NAME"],
    )

    assert isinstance(df, pl.DataFrame)
    assert df.columns == ["security", "PX_LAST", "NAME"]
    assert set(df["security"].to_list()) == {"OMX Index", "SPX Index"}


def test_bdh_live(live_bq):
    start_dt = date(2024, 1, 1)
    end_dt = date(2024, 1, 30)
    df = live_bq.bdh(
        ["OMX Index", "SEBA SS Equity"],
        ["PX_LAST", "DIVIDEND_INDICATED_YIELD"],
        start_dt,
        end_dt,
    )

    assert isinstance(df, pl.DataFrame)
    assert df.columns == ["security", "date", "PX_LAST", "DIVIDEND_INDICATED_YIELD"]
    assert set(df["security"].unique().to_list()) == {
        "OMX Index",
        "SEBA SS Equity",
    }
    assert df["date"].min() >= start_dt
    assert df["date"].max() <= end_dt
