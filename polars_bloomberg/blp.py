"""
Polars interface to Bloomberg Open API
======================================

This module provides a Polars-based interface to interact with the Bloomberg Open API.

Usage
-----

.. code-block:: python

    from datetime import date
    from polars_bloomberg import BQuery

    with BQuery() as bq:
        df_ref = bq.bdp(['AAPL US Equity', 'MSFT US Equity'], ['PX_LAST'])
        df_hist = bq.bdh(['AAPL US Equity'], ['PX_LAST'], date(2020, 1, 1), date(2020, 1, 30))

:author: Marek Ozana
:date: 2024-11
"""

from typing import Dict, List
import blpapi
import polars as pl
from datetime import date

class BQuery:
    """
    Simplified Bloomberg Query class for fetching reference and historical data.

    Usage:
        with BQuery() as bq:
            # Reference Data (BDP)
            df_bdp = bq.bdp(['OMX Index'], ['PX_LAST'])

            # Historical Data (BDH)
            df_bdh = bq.bdh(['OMX Index'], ['PX_LAST'], date(2020, 1, 1), date(2020, 1, 30))
    """

    def __init__(self, host: str = 'localhost', port: int = 8194):
        self.host = host
        self.port = port
        self.session = None

    def __enter__(self):
        options = blpapi.SessionOptions()
        options.setServerHost(self.host)
        options.setServerPort(self.port)
        self.session = blpapi.Session(options)

        if not self.session.start():
            raise ConnectionError("Failed to start Bloomberg session.")
        if not self.session.openService("//blp/refdata"):
            raise ConnectionError("Failed to open service //blp/refdata.")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            self.session.stop()

    def bdp(self, securities: List[str], fields: List[str]) -> pl.DataFrame:
        """Fetch reference data for given securities and fields."""
        request = self._create_request("ReferenceDataRequest", securities, fields)
        responses = self._send_request(request)
        data = self._parse_bdp_responses(responses, fields)
        return pl.DataFrame(data)

    def bdh(
        self,
        securities: List[str],
        fields: List[str],
        start_date: date,
        end_date: date
    ) -> pl.DataFrame:
        """Fetch historical data for given securities and fields between dates."""
        request = self._create_request("HistoricalDataRequest", securities, fields)
        request.set("startDate", start_date.strftime("%Y%m%d"))
        request.set("endDate", end_date.strftime("%Y%m%d"))
        request.set("periodicitySelection", "DAILY")
        responses = self._send_request(request)
        data = self._parse_bdh_responses(responses, fields)
        return pl.DataFrame(data)

    def _create_request(self, request_type: str, securities: List[str], fields: List[str]):
        service = self.session.getService("//blp/refdata")
        request = service.createRequest(request_type)
        for security in securities:
            request.getElement("securities").appendValue(security)
        for field in fields:
            request.getElement("fields").appendValue(field)
        return request

    def _send_request(self, request) -> List[Dict]:
        self.session.sendRequest(request)
        responses = []
        while True:
            event = self.session.nextEvent()
            for msg in event:
                responses.append(msg.toPy())
            if event.eventType() == blpapi.Event.RESPONSE:
                break
        return responses

    def _parse_bdp_responses(self, responses: List[Dict], fields: List[str]) -> List[Dict]:
        data = []
        for response in responses:
            security_data = response.get("securityData", [])
            for sec in security_data:
                security = sec.get("security")
                field_data = sec.get("fieldData", {})
                record = {"security": security}
                for field in fields:
                    record[field] = field_data.get(field)
                data.append(record)
        return data

    def _parse_bdh_responses(self, responses: List[Dict], fields: List[str]) -> List[Dict]:
        data = []
        for response in responses:
            security_data = response.get("securityData", {})
            security = security_data.get("security")
            field_data_array = security_data.get("fieldData", [])
            for entry in field_data_array:
                record = {"security": security, "date": entry.get("date")}
                for field in fields:
                    record[field] = entry.get(field)
                data.append(record)
        return data
