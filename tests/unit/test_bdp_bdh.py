
import pytest

from polars_bloomberg import BQuery

pytestmark = pytest.mark.no_bbg


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

