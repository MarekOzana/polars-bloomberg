import polars as pl
import pytest

pytestmark = pytest.mark.requires_bbg


def test_bsrch_live(live_bq):
    df = live_bq.bsrch(
        "BI:TPD",
        overrides={
            "BIKEY": "DKOCVGXJVU8II8M90W8JSQEKR",
            "LIMIT": 20000,
        },
    )

    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert len(df.columns) > 0
