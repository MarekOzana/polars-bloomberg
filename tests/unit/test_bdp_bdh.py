from datetime import date
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import yaml

from polars_bloomberg import BQuery

pytestmark = pytest.mark.no_bbg


def test_bdh_leading_nulls():
    """Test on dataset with leading nulls in a field."""
    bq = BQuery()

    with (
        patch.object(bq, "_create_request", return_value=MagicMock()),
        patch.object(bq, "_send_request", return_value="mocked_responses"),
    ):
        with open("tests/data/bdh/bdh_data_leading_nulls.yaml") as f:
            mock_data = yaml.safe_load(f)

        with patch.object(bq, "_parse_bdh_responses", return_value=mock_data):
            df = bq.bdh(
                ["BFGHICE LX Equity", "I00185US Index"],
                ["BX115", "BX213", "PX_LAST"],
                start_date=date(2024, 8, 1),
                end_date=date(2025, 1, 10),
            )

    assert isinstance(df, pl.DataFrame)
    assert df.shape == (222, 5)


def test_parse_bdp_responses():
    bq = BQuery()
    mock_responses = [
        {
            "securityData": [
                {
                    "security": "IBM US Equity",
                    "fieldData": {"PX_LAST": 125.32, "DS002": 0.85},
                },
                {
                    "security": "AAPL US Equity",
                    "fieldData": {"PX_LAST": 150.75, "DS002": 1.10},
                },
            ]
        }
    ]
    expected_output = [
        {"security": "IBM US Equity", "PX_LAST": 125.32, "DS002": 0.85},
        {"security": "AAPL US Equity", "PX_LAST": 150.75, "DS002": 1.10},
    ]

    result = bq._parse_bdp_responses(mock_responses, fields=["PX_LAST", "DS002"])
    assert result == expected_output


def test_parse_bdh_responses():
    bq = BQuery()
    mock_responses = [
        {
            "securityData": {
                "security": "IBM US Equity",
                "fieldData": [
                    {"date": "2023-01-01", "PX_LAST": 125.32, "VOLUME": 1000000},
                    {"date": "2023-01-02", "PX_LAST": 126.50, "VOLUME": 1100000},
                ],
            }
        },
        {
            "securityData": {
                "security": "AAPL US Equity",
                "fieldData": [
                    {"date": "2023-01-01", "PX_LAST": 150.75, "VOLUME": 2000000},
                    {"date": "2023-01-02", "PX_LAST": 151.20, "VOLUME": 2100000},
                ],
            }
        },
    ]
    expected_output = [
        {
            "security": "IBM US Equity",
            "date": "2023-01-01",
            "PX_LAST": 125.32,
            "VOLUME": 1000000,
        },
        {
            "security": "IBM US Equity",
            "date": "2023-01-02",
            "PX_LAST": 126.50,
            "VOLUME": 1100000,
        },
        {
            "security": "AAPL US Equity",
            "date": "2023-01-01",
            "PX_LAST": 150.75,
            "VOLUME": 2000000,
        },
        {
            "security": "AAPL US Equity",
            "date": "2023-01-02",
            "PX_LAST": 151.20,
            "VOLUME": 2100000,
        },
    ]

    result = bq._parse_bdh_responses(mock_responses, fields=["PX_LAST", "VOLUME"])
    assert result == expected_output


def test_bdp_calls_components():
    bq = BQuery()
    mock_request = MagicMock()
    parsed = [{"security": "IBM US Equity", "PX_LAST": 123.4}]

    with (
        patch.object(bq, "_create_request", return_value=mock_request) as create_mock,
        patch.object(bq, "_send_request", return_value=["raw"]) as send_mock,
        patch.object(bq, "_parse_bdp_responses", return_value=parsed) as parse_mock,
    ):
        df = bq.bdp(["IBM US Equity"], ["PX_LAST"])

    assert isinstance(df, pl.DataFrame)
    assert df.shape == (1, 2)
    assert df["security"].to_list() == ["IBM US Equity"]
    create_mock.assert_called_once_with(
        "ReferenceDataRequest", ["IBM US Equity"], ["PX_LAST"], None, None
    )
    send_mock.assert_called_once_with(mock_request)
    parse_mock.assert_called_once_with(["raw"], ["PX_LAST"])
