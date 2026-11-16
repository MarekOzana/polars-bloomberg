"""bdib() end to end: scripted bars go in, a DataFrame comes out."""

from datetime import datetime

import pytest
from recordings import SCENARIOS
from support import answer, fake_bq, replay

pytestmark = pytest.mark.no_bbg


def bar(hour, price):
    """One bar of barTickData as Bloomberg sends it, all four prices equal."""
    return {
        "time": datetime(2024, 1, 2, hour),
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "volume": 100,
        "numEvents": 10,
        "value": 100 * price,
    }


# --- bdib() end to end, through the fake session ---


def test_bdib_no_bars_returns_empty_frame_with_nine_columns():
    bq = fake_bq([answer({"barData": {"barTickData": []}})])

    df = bq.bdib(
        "OMX Index", "TRADE", 60, datetime(2024, 1, 2, 8), datetime(2024, 1, 2, 12)
    )

    assert df.shape == (0, 9)
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


def test_bdib_rows_are_sorted_by_time():
    later_bar_first = [bar(9, 2400.0), bar(8, 2390.0)]
    bq = fake_bq([answer({"barData": {"barTickData": later_bar_first}})])

    df = bq.bdib(
        "OMX Index", "TRADE", 60, datetime(2024, 1, 2, 8), datetime(2024, 1, 2, 10)
    )

    assert df["time"].to_list() == [datetime(2024, 1, 2, 8), datetime(2024, 1, 2, 9)]
    assert df["open"].to_list() == [2390.0, 2400.0]


# --- the security column ---


def test_bdib_security_column_holds_the_requested_security():
    # Bloomberg's barData names no security (none of the bdib recordings has the
    # key), so every bar gets the security that was asked for.
    bq, _ = replay("bdib/trade_bars_60min")

    df = SCENARIOS["bdib/trade_bars_60min"](bq)

    assert df.height > 0
    assert df["security"].unique().to_list() == ["OMX Index"]


def test_bdib_security_named_in_bar_data_is_used():
    # Should barData ever name the security, the frame takes Bloomberg's name
    # over the identifier that was asked for.
    bar_data = {"security": "IBM US Equity", "barTickData": [bar(8, 160.0)]}
    bq = fake_bq([answer({"barData": bar_data})])

    df = bq.bdib(
        "/isin/US4592001014",
        "TRADE",
        60,
        datetime(2024, 1, 2, 8),
        datetime(2024, 1, 2, 9),
    )

    assert df["security"].to_list() == ["IBM US Equity"]
