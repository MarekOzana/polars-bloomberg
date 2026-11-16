from unittest.mock import MagicMock, patch

import pytest
from support import FakeSession, answer, fake_bq

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


# --- session lifecycle: start(), stop(), services opened on first use ---


@pytest.fixture()
def new_session():
    """The FakeSession that the next blpapi.Session(...) call returns."""
    ibm = {"security": "IBM US Equity", "fieldData": {"NAME": "IBM"}}
    session = FakeSession([answer({"securityData": [ibm]})])
    with patch("polars_bloomberg.plbbg.blpapi.Session", return_value=session):
        yield session


def test_bdp_without_started_session_raises_connection_error():
    bq = BQuery()

    with pytest.raises(ConnectionError, match="session is not started"):
        bq.bdp(["IBM US Equity"], ["NAME"])


def test_start_and_stop_work_without_a_with_block(new_session):
    bq = BQuery().start()
    df = bq.bdp(["IBM US Equity"], ["NAME"])
    bq.stop()

    assert df["NAME"].to_list() == ["IBM"]
    assert new_session.stopped
    assert bq.session is None


def test_stop_twice_is_safe(new_session):
    bq = BQuery().start()

    bq.stop()
    bq.stop()

    assert new_session.stopped


def test_bdp_opens_only_the_refdata_service():
    bq = fake_bq([answer({"securityData": []})])

    bq.bdp(["IBM US Equity"], ["NAME"])

    assert bq.session.opened == ["//blp/refdata"]


def test_service_is_opened_once_for_two_calls():
    bq = fake_bq([answer({"securityData": []}), answer({"securityData": []})])

    bq.bdp(["IBM US Equity"], ["NAME"])
    bq.bdp(["IBM US Equity"], ["NAME"])

    assert bq.session.opened == ["//blp/refdata"]


def test_bql_raises_connection_error_when_its_service_cannot_be_opened():
    bq = BQuery()
    bq.session = FakeSession(unavailable={"//blp/bqlsvc"})

    with pytest.raises(ConnectionError, match="//blp/bqlsvc"):
        bq.bql("get(name) for(['IBM US Equity'])")


def test_session_is_stopped_when_a_call_fails_inside_a_with_block(new_session):
    new_session.events.clear()  # no answer: the call times out

    with pytest.raises(TimeoutError), BQuery() as bq:
        bq.bdp(["IBM US Equity"], ["NAME"])

    assert new_session.stopped


# --- debug ---


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
