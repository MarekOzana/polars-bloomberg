from datetime import datetime

import pytest
from support import answer, fake_bq

from polars_bloomberg import BQuery

pytestmark = pytest.mark.no_bbg


def test_parse_bdib_responses():
    bq = BQuery()
    first_bar_time = datetime(2024, 1, 2, 9, 30)
    second_bar_time = datetime(2024, 1, 2, 9, 35)

    mock_responses = [
        {
            "barData": {
                "security": "MSFT US Equity",
                "eventType": "TRADE",
                "barTickData": [
                    {
                        "barTickData": {
                            "time": first_bar_time,
                            "open": 370.0,
                            "high": 370.5,
                            "low": 369.8,
                            "close": 370.2,
                            "volume": 800,
                            "numEvents": 8,
                            "value": 296160.0,
                        }
                    }
                ],
            }
        },
        {
            "barData": {
                "barTickData": [
                    {
                        "time": second_bar_time,
                        "open": 370.3,
                        "high": 371.0,
                        "low": 370.1,
                        "close": 370.9,
                        "volume": 600,
                        "numEvents": 6,
                        "value": 222540.0,
                    }
                ]
            }
        },
    ]

    result = bq._parse_bdib_responses(mock_responses, fallback_security="MSFT US Equity")

    assert result == [
        {
            "security": "MSFT US Equity",
            "time": first_bar_time,
            "open": 370.0,
            "high": 370.5,
            "low": 369.8,
            "close": 370.2,
            "volume": 800,
            "numEvents": 8,
            "value": 296160.0,
        },
        {
            "security": "MSFT US Equity",
            "time": second_bar_time,
            "open": 370.3,
            "high": 371.0,
            "low": 370.1,
            "close": 370.9,
            "volume": 600,
            "numEvents": 6,
            "value": 222540.0,
        },
    ]


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
