from unittest.mock import MagicMock, patch

import blpapi
import pytest

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
    bq = BQuery(timeout=5000, debug=True)
    session_mock = MagicMock()
    bq.session = session_mock

    response_event = MagicMock()
    response_event.eventType.return_value = blpapi.Event.RESPONSE
    response_message = MagicMock()
    response_message.hasElement.return_value = False
    response_message.toPy.return_value = {"final": "data"}
    response_event.__iter__.return_value = iter([response_message])
    session_mock.nextEvent.return_value = response_event

    monkeypatch.chdir(tmp_path)
    bq._send_request(MagicMock())

    debug_dir = tmp_path / "debug_cases"
    saved = list(debug_dir.glob("responses_*.json"))
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
