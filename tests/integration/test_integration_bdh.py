"""bdh: history, like =BDH() in Excel.

bq.bdh(securities, fields, start_date, end_date) gives one row per security and
date: `security`, `date`, then one column per field. Dates can be `date` objects or
"YYYY-MM-DD" / "YYYYMMDD" strings. `options` takes Bloomberg's request settings,
for example the periodicity or how to fill days without trading.

Run:  pytest --bbg tests/integration/test_integration_bdh.py
"""

from datetime import date

import polars as pl
import pytest
from polars.testing import assert_frame_equal

pytestmark = pytest.mark.requires_bbg

JAN_2, JAN_3, JAN_4, JAN_5 = (date(2024, 1, day) for day in (2, 3, 4, 5))


def test_bdh_gives_one_row_per_security_and_trading_day(bq):
    df = bq.bdh(
        ["OMX Index", "SPX Index"],
        ["PX_LAST", "PX_VOLUME"],
        start_date=JAN_2,
        end_date=JAN_5,
    )

    expected = pl.DataFrame(
        [
            ("OMX Index", JAN_2, 2394.929, 92026171.0),
            ("OMX Index", JAN_3, 2356.639, 100011646.0),
            ("OMX Index", JAN_4, 2361.899, 115471756.0),
            ("OMX Index", JAN_5, 2337.864, 58157796.0),
            ("SPX Index", JAN_2, 4742.83, 707023582.0),
            ("SPX Index", JAN_3, 4704.81, 696024599.0),
            ("SPX Index", JAN_4, 4688.68, 693014905.0),
            ("SPX Index", JAN_5, 4697.24, 650594292.0),
        ],
        schema=["security", "date", "PX_LAST", "PX_VOLUME"],
        orient="row",
    )
    assert_frame_equal(df.sort("security", "date"), expected)


def test_bdh_pivots_to_one_column_per_security(bq):
    df = bq.bdh(["OMX Index", "SPX Index"], "PX_LAST", "2024-01-02", "2024-01-05")

    wide = df.pivot(on="security", index="date", values="PX_LAST").sort("date")

    assert wide.columns == ["date", "OMX Index", "SPX Index"]
    assert wide.row(0) == (JAN_2, 2394.929, 4742.83)


def test_bdh_options_ask_for_monthly_values(bq):
    df = bq.bdh(
        "OMX Index",
        "PX_LAST",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 3, 31),
        options={"periodicitySelection": "MONTHLY"},
    )

    assert df.select("date", "PX_LAST").rows() == [
        (date(2024, 1, 31), 2356.601),
        (date(2024, 2, 29), 2452.805),
        (date(2024, 3, 29), 2518.275),  # the last trading day of March
    ]


def test_bdh_options_fill_a_weekend_with_fridays_value(bq):
    df = bq.bdh(
        "SPX Index",
        "PX_LAST",
        start_date=date(2024, 1, 5),
        end_date=date(2024, 1, 8),
        options={
            "nonTradingDayFillOption": "ALL_CALENDAR_DAYS",
            "nonTradingDayFillMethod": "PREVIOUS_VALUE",
        },
    )

    assert df.select("date", "PX_LAST").rows() == [
        (date(2024, 1, 5), 4697.24),  # Friday
        (date(2024, 1, 6), 4697.24),  # Saturday
        (date(2024, 1, 7), 4697.24),  # Sunday
        (date(2024, 1, 8), 4763.54),  # Monday
    ]


def test_bdh_without_trading_days_gives_an_empty_frame_with_its_columns(bq):
    weekend = date(2024, 1, 6), date(2024, 1, 7)

    df = bq.bdh("OMX Index", "PX_LAST", *weekend)

    assert df.is_empty()
    assert df.columns == ["security", "date", "PX_LAST"]
