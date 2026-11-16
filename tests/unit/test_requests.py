"""The requests that bdp, bdh, bdib and bql build before sending them."""

from datetime import UTC, date, datetime, timedelta, timezone
from unittest.mock import MagicMock

import blpapi
import pytest
from support import answer, fake_bq

from polars_bloomberg import BQuery

pytestmark = pytest.mark.no_bbg


def request_sent_by(call):
    """Run `call(bq)` on a fake session and return the request it sent, as a dict.

    The request is built on the reduced test schema of //blp/refdata, so it is checked
    the way Bloomberg would check it. The scripted answer is empty.
    """
    bq = fake_bq([answer({})])
    call(bq)
    return bq.session.sent[0].asElement().toPy()


# --- bdp, bdh, bdib: real requests ---


def test_bdp_request_has_securities_and_fields():
    request = request_sent_by(
        lambda bq: bq.bdp(["OMX Index", "SPX Index"], ["PX_LAST", "NAME"])
    )

    assert request == {
        "securities": ["OMX Index", "SPX Index"],
        "fields": ["PX_LAST", "NAME"],
    }


def test_bdp_request_has_overrides():
    request = request_sent_by(
        lambda bq: bq.bdp(
            ["OMX Index"], ["PX_LAST"], overrides=[("EQY_FUND_CRNCY", "SEK")]
        )
    )

    assert request["overrides"] == [{"fieldId": "EQY_FUND_CRNCY", "value": "SEK"}]


def test_bdp_request_has_options():
    request = request_sent_by(
        lambda bq: bq.bdp(["OMX Index"], ["PX_LAST"], options={"returnEids": True})
    )

    assert request["returnEids"] is True


def test_bdh_request_has_dates_as_yyyymmdd_and_options():
    request = request_sent_by(
        lambda bq: bq.bdh(
            ["OMX Index"],
            ["PX_LAST"],
            date(2024, 1, 2),
            date(2024, 3, 28),
            options={"periodicitySelection": "MONTHLY"},
        )
    )

    assert request == {
        "securities": ["OMX Index"],
        "fields": ["PX_LAST"],
        "startDate": "20240102",
        "endDate": "20240328",
        "periodicitySelection": "MONTHLY",
    }


def test_bdib_request_with_naive_datetimes_sends_them_unchanged():
    request = request_sent_by(
        lambda bq: bq.bdib(
            "OMX Index",
            "TRADE",
            60,
            datetime(2024, 1, 2, 8, 0),
            datetime(2024, 1, 2, 12, 0),
            overrides=[("PRICE_OVERRIDE", "SETTLE")],
            options={"gapFillInitialBar": True},
        )
    )

    assert request == {
        "security": "OMX Index",
        "eventType": "TRADE",
        "interval": 60,
        "startDateTime": datetime(2024, 1, 2, 8, 0),
        "endDateTime": datetime(2024, 1, 2, 12, 0),
        "gapFillInitialBar": True,
        "overrides": [{"fieldId": "PRICE_OVERRIDE", "value": "SETTLE"}],
    }


def test_bdib_request_with_aware_datetimes_sends_them_in_utc():
    stockholm_winter = timezone(timedelta(hours=1))

    request = request_sent_by(
        lambda bq: bq.bdib(
            "OMX Index",
            "TRADE",
            60,
            datetime(2024, 1, 2, 9, 0, tzinfo=stockholm_winter),
            datetime(2024, 1, 2, 13, 0, tzinfo=stockholm_winter),
        )
    )

    assert request["startDateTime"] == datetime(2024, 1, 2, 8, 0, tzinfo=UTC)
    assert request["endDateTime"] == datetime(2024, 1, 2, 12, 0, tzinfo=UTC)


# --- bql: the bqlsvc schema is not available offline ---


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
