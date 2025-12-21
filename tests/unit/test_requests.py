from datetime import datetime
from unittest.mock import MagicMock

import blpapi
import pytest

from polars_bloomberg import BQuery

pytestmark = pytest.mark.no_bbg


def _make_request_with_elements():
    request_mock = MagicMock()
    securities_element = MagicMock()
    fields_element = MagicMock()
    overrides_element = MagicMock()

    def get_element(name: str):
        if name == "securities":
            return securities_element
        if name == "fields":
            return fields_element
        if name == "overrides":
            return overrides_element
        return MagicMock()

    request_mock.getElement.side_effect = get_element
    return request_mock, securities_element, fields_element, overrides_element


def test_create_request(bq: BQuery):
    request_mock, securities_element, fields_element, _ = _make_request_with_elements()
    service_mock = MagicMock()
    service_mock.createRequest.return_value = request_mock
    bq.session.getService.return_value = service_mock

    request = bq._create_request(
        request_type="ReferenceDataRequest",
        securities=["OMX Index", "SPX Index"],
        fields=["PX_LAST"],
    )

    assert request is request_mock
    securities_element.appendValue.assert_any_call("OMX Index")
    securities_element.appendValue.assert_any_call("SPX Index")
    fields_element.appendValue.assert_called_once_with("PX_LAST")


def test_create_request_with_overrides(bq: BQuery):
    request_mock, _, _, overrides_element = _make_request_with_elements()
    service_mock = MagicMock()
    service_mock.createRequest.return_value = request_mock
    bq.session.getService.return_value = service_mock

    override_entry = MagicMock()
    overrides_element.appendElement.return_value = override_entry

    bq._create_request(
        request_type="ReferenceDataRequest",
        securities=["OMX Index", "SPX Index"],
        fields=["PX_LAST"],
        overrides=[("EQY_FUND_CRNCY", "SEK")],
    )

    override_entry.setElement.assert_any_call("fieldId", "EQY_FUND_CRNCY")
    override_entry.setElement.assert_any_call("value", "SEK")


def test_create_request_with_options(bq: BQuery):
    request_mock, _, _, _ = _make_request_with_elements()
    service_mock = MagicMock()
    service_mock.createRequest.return_value = request_mock
    bq.session.getService.return_value = service_mock

    bq._create_request(
        request_type="HistoricalDataRequest",
        securities=["OMX Index", "SPX Index"],
        fields=["PX_LAST"],
        options={"adjustmentSplit": True},
    )

    request_mock.set.assert_called_once_with("adjustmentSplit", True)


def test_create_intraday_bar_request(bq: BQuery):
    request_mock = MagicMock()
    overrides_element = MagicMock()
    override_entry = MagicMock()
    overrides_element.appendElement.return_value = override_entry
    request_mock.getElement.return_value = overrides_element

    service_mock = MagicMock()
    service_mock.createRequest.return_value = request_mock
    bq.session.getService.return_value = service_mock

    start_dt = datetime(2024, 1, 2, 9, 30)
    end_dt = datetime(2024, 1, 2, 16, 0)
    overrides = [("PRICE_OVERRIDE", "SETTLE")]
    options = {"gapFillInitialBar": True}

    result = bq._create_intraday_bar_request(
        security="AAPL US Equity",
        event_type="TRADE",
        interval=5,
        start_datetime=start_dt,
        end_datetime=end_dt,
        overrides=overrides,
        options=options,
    )

    bq.session.getService.assert_called_once_with("//blp/refdata")
    service_mock.createRequest.assert_called_once_with("IntradayBarRequest")
    assert result is request_mock

    request_mock.set.assert_any_call("security", "AAPL US Equity")
    request_mock.set.assert_any_call("eventType", "TRADE")
    request_mock.set.assert_any_call("interval", 5)
    request_mock.set.assert_any_call("startDateTime", "2024-01-02T09:30:00")
    request_mock.set.assert_any_call("endDateTime", "2024-01-02T16:00:00")
    request_mock.set.assert_any_call("gapFillInitialBar", True)

    overrides_element.appendElement.assert_called_once()
    override_entry.setElement.assert_any_call("fieldId", "PRICE_OVERRIDE")
    override_entry.setElement.assert_any_call("value", "SETTLE")


def test_create_bql_request_sets_client_context(bq: BQuery):
    session_mock = MagicMock()
    service_mock = MagicMock()
    request_mock = MagicMock()
    client_context_mock = MagicMock()

    session_mock.getService.return_value = service_mock
    service_mock.createRequest.return_value = request_mock
    request_mock.getElement.return_value = client_context_mock
    bq.session = session_mock

    expression = "get(px_last) for(['AAPL US Equity'])"
    result = bq._create_bql_request(expression)

    session_mock.getService.assert_called_once_with("//blp/bqlsvc")
    service_mock.createRequest.assert_called_once_with("sendQuery")
    request_mock.set.assert_called_once_with("expression", expression)
    client_context_mock.setElement.assert_called_once_with("appName", "EXCEL")
    assert result is request_mock


def test_create_bql_request_missing_client_context(bq: BQuery):
    session_mock = MagicMock()
    service_mock = MagicMock()
    request_mock = MagicMock()

    session_mock.getService.return_value = service_mock
    service_mock.createRequest.return_value = request_mock
    request_mock.getElement.side_effect = blpapi.NotFoundException(0, "missing")
    bq.session = session_mock

    expression = "get(px_last) for(['AAPL US Equity'])"
    result = bq._create_bql_request(expression)

    request_mock.set.assert_called_once_with("expression", expression)
    assert result is request_mock
