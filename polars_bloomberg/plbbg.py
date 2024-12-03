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
        df_rf2 = bq.bdp(["OMX Index", "SPX Index", "SEBA SS Equity"],
                        ["PX_LAST", "SECURITY_DES", "DVD_EX_DT", "CRNCY_ADJ_PX_LAST"],
                        overrides=[("EQY_FUND_CRNCY", "SEK")])
        df_hist = bq.bdh(['AAPL US Equity'], ['PX_LAST'], date(2020, 1, 1), date(2020, 1, 30))
        df_px = bq.bql("get(px_last) for(['IBM US Equity', 'AAPL US Equity'])")

:author: Marek Ozana
:date: 2024-12
"""
import json
import logging
from datetime import date
from typing import Any, Dict, List, Optional, Sequence

import blpapi
import polars as pl

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class BQuery:
    def __init__(self, host: str = "localhost", port: int = 8194):
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

        # Open both required services
        if not self.session.openService("//blp/refdata"):
            raise ConnectionError("Failed to open service //blp/refdata.")
        if not self.session.openService("//blp/bqlsvc"):
            raise ConnectionError("Failed to open service //blp/bqlsvc.")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            self.session.stop()

    def bdp(
        self,
        securities: List[str],
        fields: List[str],
        overrides: Optional[Sequence] = None,
        options: Optional[Dict] = None,
    ) -> pl.DataFrame:
        """
        Bloomberg Data Point, equivalent to Excel BDP() function.
        Fetch reference data for given securities and fields.
        """
        request = self._create_request(
            "ReferenceDataRequest", securities, fields, overrides, options
        )
        responses = self._send_request(request)
        data = self._parse_bdp_responses(responses, fields)
        return pl.DataFrame(data)

    def bdh(
        self,
        securities: List[str],
        fields: List[str],
        start_date: date,
        end_date: date,
        overrides: Optional[Sequence] = None,
        options: Optional[Dict] = None,
    ) -> pl.DataFrame:
        """
        Bloomberg Data History, equivalent to Excel BDH() function.
        Fetch historical data for given securities and fields between dates.
        """
        request = self._create_request(
            "HistoricalDataRequest", securities, fields, overrides, options
        )
        request.set("startDate", start_date.strftime("%Y%m%d"))
        request.set("endDate", end_date.strftime("%Y%m%d"))
        request.set("periodicitySelection", "DAILY")
        responses = self._send_request(request)
        data = self._parse_bdh_responses(responses, fields)
        return pl.DataFrame(data)

    def bql(
        self,
        expression: str,
        overrides: Optional[Sequence] = None,
        options: Optional[Dict] = None,
    ) -> pl.DataFrame:
        """Fetch data using a BQL expression."""
        request = self._create_bql_request(expression, overrides, options)
        responses = self._send_request(request)
        data = self._parse_bql_responses(responses)
        return pl.DataFrame(data)

    def _create_request(
        self,
        request_type: str,
        securities: List[str],
        fields: List[str],
        overrides: Optional[Sequence] = None,
        options: Optional[Dict] = None,
    ) -> blpapi.Request:
        """
        Create a Bloomberg request with support for overrides and additional options.

        Parameters:
            request_type (str): Type of the request (e.g., 'ReferenceDataRequest').
            securities (List[str]): List of securities to include in the request.
            fields (List[str]): List of fields to include in the request.
            overrides (List[Tuple[str, Any]], optional): List of overrides.
            options (Dict, optional): Additional options as key-value pairs.

        Returns:
            blpapi.Request: The constructed Bloomberg request.
        """
        service = self.session.getService("//blp/refdata")
        request = service.createRequest(request_type)

        # Add securities
        securities_element = request.getElement("securities")
        for security in securities:
            securities_element.appendValue(security)

        # Add fields
        fields_element = request.getElement("fields")
        for field in fields:
            fields_element.appendValue(field)

        # Add overrides if provided
        if overrides:
            overrides_element = request.getElement("overrides")
            for field_id, value in overrides:
                override_element = overrides_element.appendElement()
                override_element.setElement("fieldId", field_id)
                override_element.setElement("value", value)

        # Add additional options if provided
        if options:
            for key, value in options.items():
                request.set(key, value)

        return request

    def _create_bql_request(
        self,
        expression: str,
        overrides: Optional[Sequence] = None,
        options: Optional[Dict] = None,
    ) -> blpapi.Request:
        """Create a BQL request."""
        service = self.session.getService("//blp/bqlsvc")
        request = service.createRequest("sendQuery")
        request.set("expression", expression)

        # Add overrides if provided
        if overrides:
            overrides_element = request.getElement("overrides")
            for field_id, value in overrides:
                override_element = overrides_element.appendElement()
                override_element.setElement("fieldId", field_id)
                override_element.setElement("value", value)

        # Add additional options if provided
        if options:
            for key, value in options.items():
                request.set(key, value)

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

    def _parse_bdp_responses(
        self, responses: List[Dict], fields: List[str]
    ) -> List[Dict]:
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

    def _parse_bdh_responses(
        self, responses: List[Dict], fields: List[str]
    ) -> List[Dict]:
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

    def _parse_bql_responses(self, responses: List[Any]) -> List[Dict]:
        """Parse BQL responses list. IT consists of dictionaries
        and string with embedded json.

        1. Iterate over list of responses and only extract those with 'results' key.
        Example responses: [
        {...}, 
        {...},
        "{'results': {'px_last': {}, 'valuesColumn': {}, 'secondaryColumns': [{}]}}, 'error': None}"]
        ]

        2. pass results dictionary to _parse_bql_response_dict method
        Example results:
        {
            'px_last': {
                'idColumn': {'values': ['IBM US Equity', 'AAPL US Equity']},
                'valuesColumn': {'values': [227.615, 239.270]},
                'secondaryColumns': [
                    {'name': 'DATE', 'values': ['2024-12-02T00:00:00Z', '2024-12-02T00:00:00Z']},
                    {'name': 'CURRENCY', 'values': ['USD', 'USD']}
                ]
            } 
        }
        """
        data = []
        for response in responses:
            # Parse JSON string if necessary
            if isinstance(response, str):
                try:
                    response_dict = json.loads(response.replace("'", "\""))
                except json.JSONDecodeError:
                    logger.warning("Invalid JSON string in response.")
                    continue
            elif isinstance(response, dict):
                response_dict = response
            else:
                logger.warning(f"Unexpected response type: {type(response)}.")
                continue

            results = response_dict.get("results", {})

            self._parse_bql_response_dict(data, results)
        logger.info(f"Total records parsed: {len(data)}")
        return data

    def _parse_bql_response_dict(self, data, results):
        """Parse BQL response dictionary.

        Example results:
        {
            'px_last': {
                'idColumn': {'values': ['IBM US Equity', 'AAPL US Equity']},
                'valuesColumn': {'values': [227.615, 239.270]},
                'secondaryColumns': [
                    {'name': 'DATE', 'values': ['2024-12-02T00:00:00Z', '2024-12-02T00:00:00Z']},
                    {'name': 'CURRENCY', 'values': ['USD', 'USD']}
                ]
            }
        }
        """
        if len(results) == 0:
            return
        
        if logger.getEffectiveLevel() <= logging.DEBUG:
            with open("tests/data/results.json", "w") as f:
                json.dump(results, f)
                logger.debug("Results saved to results.json")
        for field, content in results.items():
            id_values = content.get("idColumn", {}).get("values", [])
            value_values = content.get("valuesColumn", {}).get("values", [])
            secondary_columns = content.get("secondaryColumns", [])

                # Extract secondary column data
            secondary_data = {col['name']: col['values'] for col in secondary_columns if 'name' in col and 'values' in col}

            for i in range(len(id_values)):
                record = {
                        "security": id_values[i],
                        field: value_values[i] if i < len(value_values) else None
                    }
                for sec_name, sec_values in secondary_data.items():
                    record[sec_name] = sec_values[i] if i < len(sec_values) else None
                data.append(record)
                logger.debug(f"Added record: {record}")
