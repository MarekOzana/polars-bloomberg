import json
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from polars.testing import assert_frame_equal

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


def test_bdib_returns_dataframe():
    """bdib should build request, parse responses, and return a sorted DataFrame."""
    bq = BQuery()
    mock_request = MagicMock()
    bar_rows = [
        {
            "security": "AAPL US Equity",
            "time": datetime(2024, 1, 2, 9, 35),
            "open": 189.5,
            "high": 190.0,
            "low": 189.4,
            "close": 189.9,
            "volume": 1200,
            "numEvents": 10,
            "value": 227400.0,
        },
        {
            "security": "AAPL US Equity",
            "time": datetime(2024, 1, 2, 9, 30),
            "open": 189.0,
            "high": 189.7,
            "low": 188.9,
            "close": 189.6,
            "volume": 1500,
            "numEvents": 12,
            "value": 284400.0,
        },
    ]

    with (
        patch.object(bq, "_create_intraday_bar_request", return_value=mock_request),
        patch.object(bq, "_send_request", return_value=["raw_response"]),
        patch.object(bq, "_parse_bdib_responses", return_value=bar_rows),
    ):
        df = bq.bdib(
            security="AAPL US Equity",
            event_type="TRADE",
            interval=5,
            start_datetime=datetime(2024, 1, 2, 9, 30),
            end_datetime=datetime(2024, 1, 2, 9, 40),
        )

    assert isinstance(df, pl.DataFrame)
    assert df.shape == (2, 9)
    assert df["time"].to_list() == sorted(df["time"].to_list())
    assert df["open"].to_list() == [189.0, 189.5]


def test_bdib_uses_recorded_response():
    """Replay a captured Bloomberg response to validate bdib() end-to-end."""
    bdib_fixture = Path("tests/data/bdib/bdib_raw_response.json")

    with bdib_fixture.open() as f:
        events = json.load(f)

    payloads = [
        event["payload"]
        for event in events
        if event.get("message_type") == "IntradayBarResponse"
    ]
    assert payloads, "Recorded file must include an IntradayBarResponse payload."

    def _restore_datetime_payloads(payload_list: list[dict]) -> None:
        """Bloomberg API returns Python datetimes, but our JSON stores strings."""
        for payload in payload_list:
            bar_data = payload.get("barData", {})
            entries = bar_data.get("barTickData", [])
            for entry in entries:
                bar_entry = entry.get("barTickData", entry)
                timestamp = bar_entry.get("time")
                if isinstance(timestamp, str):
                    clean_ts = timestamp[:-1] if timestamp.endswith("Z") else timestamp
                    bar_entry["time"] = datetime.fromisoformat(clean_ts)

    _restore_datetime_payloads(payloads)

    bq = BQuery()
    mock_request = MagicMock()

    with (
        patch.object(bq, "_create_intraday_bar_request", return_value=mock_request),
        patch.object(bq, "_send_request", return_value=payloads),
    ):
        df = bq.bdib(
            "OMX Index",
            event_type="TRADE",
            interval=60,
            start_datetime=datetime(2025, 11, 5),
            end_datetime=datetime(2025, 11, 5, 12),
        )

    df_exp = pl.DataFrame(
        {
            "security": ["OMX Index"] * 4,
            "time": [
                datetime(2025, 11, 5, 8, 0),
                datetime(2025, 11, 5, 9, 0),
                datetime(2025, 11, 5, 10, 0),
                datetime(2025, 11, 5, 11, 0),
            ],
            "open": [2726.603, 2739.466, 2733.747, 2731.721],
            "high": [2742.014, 2739.706, 2734.827, 2742.015],
            "low": [2721.481, 2730.696, 2730.298, 2730.662],
            "close": [2739.321, 2733.836, 2731.724, 2741.185],
            "volume": [0, 0, 0, 0],
            "numEvents": [3591, 3600, 3600, 3600],
            "value": [0.0, 0.0, 0.0, 0.0],
        }
    )

    assert_frame_equal(df, df_exp)
