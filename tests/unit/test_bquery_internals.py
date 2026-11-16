from unittest.mock import MagicMock, patch

import pytest
from support import answer, fake_bq

from polars_bloomberg import BQuery

pytestmark = pytest.mark.no_bbg


def test_enter_start_failure():
    session_mock = MagicMock()
    session_mock.start.return_value = False

    with (
        patch("polars_bloomberg.plbbg.blpapi.SessionOptions"),
        patch("polars_bloomberg.plbbg.blpapi.Session", return_value=session_mock),
    ):
        bq = BQuery()
        with pytest.raises(
            ConnectionError, match=r"Failed to start Bloomberg session\."
        ):
            bq.__enter__()


@pytest.mark.parametrize(
    ("service_name", "message"),
    [
        ("//blp/refdata", "Failed to open service //blp/refdata."),
        ("//blp/bqlsvc", "Failed to open service //blp/bqlsvc."),
        ("//blp/exrsvc", "Failed to open service //blp/exrsvc."),
    ],
)
def test_enter_open_service_failure(service_name, message):
    session_mock = MagicMock()
    session_mock.start.return_value = True

    def open_service(name):
        return name != service_name

    session_mock.openService.side_effect = open_service

    with (
        patch("polars_bloomberg.plbbg.blpapi.SessionOptions"),
        patch("polars_bloomberg.plbbg.blpapi.Session", return_value=session_mock),
    ):
        bq = BQuery()
        with pytest.raises(ConnectionError, match=message):
            bq.__enter__()


def test_send_request_writes_debug_file(tmp_path, monkeypatch):
    bq = fake_bq([answer({"securityData": []})], debug=True)
    monkeypatch.chdir(tmp_path)

    bq.bdp(["IBM US Equity"], ["NAME"])

    saved = list((tmp_path / "debug_cases").glob("responses_*.json"))
    assert len(saved) == 1


def test_parse_result_saves_debug_case():
    bq = BQuery(debug=True)
    bq._save_debug_case = MagicMock()

    results = {
        "px_last": {
            "idColumn": {"values": ["IBM US Equity"], "type": "STRING"},
            "valuesColumn": {"values": [123.4], "type": "DOUBLE"},
            "secondaryColumns": [],
        }
    }

    tables = bq._parse_result(results)
    assert tables
    bq._save_debug_case.assert_called_once_with(results, tables)
