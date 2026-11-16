from datetime import datetime

import pytest
from support import answer, fake_bq

pytestmark = pytest.mark.no_bbg


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
    def bar(hour, price):
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

    later_bar_first = [bar(9, 2400.0), bar(8, 2390.0)]
    bq = fake_bq([answer({"barData": {"barTickData": later_bar_first}})])

    df = bq.bdib(
        "OMX Index", "TRADE", 60, datetime(2024, 1, 2, 8), datetime(2024, 1, 2, 10)
    )

    assert df["time"].to_list() == [datetime(2024, 1, 2, 8), datetime(2024, 1, 2, 9)]
    assert df["open"].to_list() == [2390.0, 2400.0]
