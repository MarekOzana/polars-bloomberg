from unittest.mock import MagicMock, patch

import blpapi
import pytest

from polars_bloomberg import BQuery

pytestmark = pytest.mark.no_bbg


class TestBQuerySendRequest:
    @pytest.fixture
    def bquery(self):
        with patch("polars_bloomberg.plbbg.blpapi.Session") as mock_session_class:
            mock_session_instance = MagicMock()
            mock_session_instance.start.return_value = True
            mock_session_instance.openService.return_value = True
            mock_session_class.return_value = mock_session_instance
            with BQuery(timeout=5000) as bquery:
                yield bquery

    def test_send_request_success(self, bquery: BQuery):
        partial_event = MagicMock()
        partial_event.eventType.return_value = blpapi.Event.PARTIAL_RESPONSE

        final_event = MagicMock()
        final_event.eventType.return_value = blpapi.Event.RESPONSE

        partial_message = MagicMock()
        partial_message.hasElement.return_value = False
        partial_message.toPy.return_value = {"partial": "data"}

        final_message = MagicMock()
        final_message.hasElement.return_value = False
        final_message.toPy.return_value = {"final": "data"}

        partial_event.__iter__.return_value = iter([partial_message])
        final_event.__iter__.return_value = iter([final_message])

        bquery.session.nextEvent.side_effect = [partial_event, final_event]

        mock_request = MagicMock()
        responses = bquery._send_request(mock_request)

        bquery.session.sendRequest.assert_called_with(mock_request)
        assert responses == [{"partial": "data"}, {"final": "data"}]
        assert bquery.session.nextEvent.call_count == 2
        bquery.session.nextEvent.assert_any_call(5000)

    def test_send_request_timeout(self, bquery: BQuery):
        timeout_event = MagicMock()
        timeout_event.eventType.return_value = blpapi.Event.TIMEOUT
        timeout_event.__iter__.return_value = iter([])

        bquery.session.nextEvent.return_value = timeout_event
        mock_request = MagicMock()

        with pytest.raises(
            TimeoutError, match="Request timed out after 5000 milliseconds"
        ):
            bquery._send_request(mock_request)

        bquery.session.sendRequest.assert_called_with(mock_request)
        bquery.session.nextEvent.assert_called_once_with(5000)

    def test_send_request_with_response_error(self, bquery: BQuery):
        response_event = MagicMock()
        response_event.eventType.return_value = blpapi.Event.RESPONSE

        error_message = MagicMock()
        error_message.hasElement.return_value = True
        error_element = MagicMock()
        error_element.getElementAsString.return_value = "Invalid field"
        error_message.getElement.return_value = error_element

        response_event.__iter__.return_value = iter([error_message])
        bquery.session.nextEvent.return_value = response_event

        mock_request = MagicMock()
        with pytest.raises(Exception, match="Response error: Invalid field"):
            bquery._send_request(mock_request)

        bquery.session.sendRequest.assert_called_with(mock_request)
        bquery.session.nextEvent.assert_called_once_with(5000)
