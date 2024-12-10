"""Polars interface to Bloomberg Open API.

This module provides a Polars-based interface to interact with the Bloomberg Open API.

Usage
-----
.. code-block:: python

    from datetime import date
    from polars_bloomberg import BQuery

    with BQuery() as bq:
        df_ref = bq.bdp(['AAPL US Equity', 'MSFT US Equity'], ['PX_LAST'])
        df_rf2 = bq.bdp(
            ["OMX Index", "SPX Index", "SEBA SS Equity"],
            ["PX_LAST", "SECURITY_DES", "DVD_EX_DT", "CRNCY_ADJ_PX_LAST"],
            overrides=[("EQY_FUND_CRNCY", "SEK")]
        )
        df_hist = bq.bdh(
            ['AAPL US Equity'],
            ['PX_LAST'],
            date(2020, 1, 1),
            date(2020, 1, 30)
        )
        df_px = bq.bql("get(px_last) for(['IBM US Equity', 'AAPL US Equity'])")

:author: Marek Ozana
:date: 2024-12
"""

import json
import logging
from collections.abc import Sequence
from datetime import date, datetime
from typing import Any

import blpapi
import polars as pl

# Configure logging
# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class BQuery:
    """Interface for interacting with the Bloomberg Open API using Polars."""

    def __init__(self, host: str = "localhost", port: int = 8194, timeout: int = 32_000):
        """Initialize a BQuery instance with connection parameters.

        Parameters
        ----------
        host : str
            The hostname for the Bloomberg API server.
        port : int
            The port number for the Bloomberg API server.
        timeout : int
            Timeout in milliseconds for API requests.

        """
        self.host = host
        self.port = port
        self.timeout = timeout  # Timeout in milliseconds
        self.session = None

    def __enter__(self):
        """Enter the runtime context related to this object."""
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
        """Exit the context manager and stop the Bloomberg session."""
        if self.session:
            self.session.stop()

    def bdp(
        self,
        securities: list[str],
        fields: list[str],
        overrides: Sequence | None = None,
        options: dict | None = None,
    ) -> pl.DataFrame:
        """Bloomberg Data Point, equivalent to Excel BDP() function.

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
        securities: list[str],
        fields: list[str],
        start_date: date,
        end_date: date,
        overrides: Sequence | None = None,
        options: dict | None = None,
    ) -> pl.DataFrame:
        """Bloomberg Data History, equivalent to Excel BDH() function.

        Fetch historical data for given securities and fields between dates.
        """
        request = self._create_request(
            "HistoricalDataRequest", securities, fields, overrides, options
        )
        request.set("startDate", start_date.strftime("%Y%m%d"))
        request.set("endDate", end_date.strftime("%Y%m%d"))
        responses = self._send_request(request)
        data = self._parse_bdh_responses(responses, fields)
        return pl.DataFrame(data)

    def bql(self, expression: str) -> pl.DataFrame:
        """Fetch data using a BQL expression."""
        request = self._create_bql_request(expression)
        responses = self._send_request(request)
        data, schema = self._parse_bql_responses(responses)
        return pl.DataFrame(data, schema=schema, strict=True)

    def _create_request(
        self,
        request_type: str,
        securities: list[str],
        fields: list[str],
        overrides: Sequence | None = None,
        options: dict | None = None,
    ) -> blpapi.Request:
        """Create a Bloomberg request with support for overrides and additional options.

        Parameters
        ----------
        request_type: str
            Type of the request (e.g., 'ReferenceDataRequest').
        securities: List[str]
            List of securities to include in the request.
        fields: List[str]
            List of fields to include in the request.
        overrides: Optional[Sequence]
            List of overrides.
        options: Optional[Dict]
            Additional options as key-value pairs.

        Returns
        -------
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

    def _create_bql_request(self, expression: str) -> blpapi.Request:
        """Create a BQL request."""
        service = self.session.getService("//blp/bqlsvc")
        request = service.createRequest("sendQuery")
        request.set("expression", expression)
        return request

    def _send_request(self, request) -> list[dict]:
        """Send a Bloomberg request and collect responses with timeout handling.

        Returns:
            List[Dict]: The list of responses.

        Raises:
            TimeoutError: If the request times out.

        """
        self.session.sendRequest(request)
        responses = []
        while True:
            # Wait for an event with the specified timeout
            event = self.session.nextEvent(self.timeout)
            if event.eventType() == blpapi.Event.TIMEOUT:
                # Handle the timeout scenario
                raise TimeoutError(
                    f"Request timed out after {self.timeout} milliseconds"
                )
            for msg in event:
                # Check for errors in the message
                if msg.hasElement("responseError"):
                    error = msg.getElement("responseError")
                    error_message = error.getElementAsString("message")
                    raise Exception(f"Response error: {error_message}")
                responses.append(msg.toPy())
            # Break the loop when the final response is received
            if event.eventType() == blpapi.Event.RESPONSE:
                break
        return responses

    def _parse_bdp_responses(
        self, responses: list[dict], fields: list[str]
    ) -> list[dict]:
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
        self, responses: list[dict], fields: list[str]
    ) -> list[dict]:
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

    def _parse_bql_responses(self, responses: list[Any]):
        """Parse BQL responses list.

        I consists of dictionaries and string with embedded json.

        1. Iterate over list of responses and only extract those with 'results' key.
        Example responses: [
        {...},
        {...},
        "{'results': {'px_last': {}, 'valuesColumn': {}, "
        "'secondaryColumns': [{}]}}, 'error': None}"]
        ]

        2. pass results dictionary to _parse_bql_response_dict method
        Example results:
        {
            'px_last': {
                'idColumn': {'values': ['IBM US Equity', 'AAPL US Equity']},
                'valuesColumn': {'values': [227.615, 239.270]},
                'secondaryColumns': [
                    {'name': 'DATE', 'values': [
                        '2024-12-02T00:00:00Z',
                        '2024-12-02T00:00:00Z'
                    ]},
                    {'name': 'CURRENCY', 'values': ['USD', 'USD']}
                ]
            }
        }
        """
        data: dict[str, list] = {}  # Column name -> list of values
        all_column_types: dict[str, str] = {}  # Column name -> type

        # Process each response in the list
        for response in responses:
            response_dict = response
            # Parse string responses as JSON
            if isinstance(response, str):
                try:
                    response_dict = json.loads(response.replace("'", '"'))
                except json.JSONDecodeError as e:
                    logger.error(
                        "JSON decoding failed for response: %s. Error: %s", response, e
                    )
                    continue

            # Get the 'results' section from the response
            results = response_dict.get("results")
            if not results:
                continue

            # Parse the results and collect column types
            cols, col_types = self._parse_bql_response_dict(results)
            # Extend existing columns in data dictionary
            for col_name, values in cols.items():
                data.setdefault(col_name, []).extend(values)
            all_column_types.update(col_types)

        schema = self._map_column_types_to_schema(all_column_types)
        data = self._convert_dates_and_handle_nans(data, schema)

        return data, schema

    def _convert_dates_and_handle_nans(self, data, schema):
        fmt = "%Y-%m-%dT%H:%M:%SZ"
        for col, values in data.items():
            if schema.get(col) == pl.Date:
                data[col] = data[col] = [
                    # 'v' can be None, need to handle it
                    datetime.strptime(v, fmt).date() if isinstance(v, str) else None
                    for v in values
                ]
            elif schema.get(col) in [pl.Float64, pl.Int64]:
                data[col] = [None if x == "NaN" else x for x in values]
        return data

    def _map_column_types_to_schema(
        self, all_column_types: dict[str, str]
    ) -> dict[str, pl.DataType]:
        """Map column types from string representation to Polars data types.

        Parameters
        ----------
        all_column_types : dict[str, str]
            A dictionary mapping column names to their string type representations.

        Returns
        -------
        dict
            A dictionary mapping column names to Polars data types.

        """
        type_mapping = {
            "STRING": pl.Utf8,
            "DOUBLE": pl.Float64,
            "INT": pl.Int64,
            "DATE": pl.Date,
        }
        return {
            col_name: type_mapping.get(col_type, pl.Utf8)
            for col_name, col_type in all_column_types.items()
        }

    def _parse_bql_response_dict(self, results: dict[str, Any]):
        """Parse BQL response dictionary into a table format.

        Parameters
        ----------
        results: Dict[str, Any]
            The 'results' dictionary from the BQL response.

        Returns
        -------
            List[Dict]: The list of records.
            Dict[str, str]: A dictionary mapping column names to their types.

        Strategy:
        - Iterate over all fields in 'results'.
        - Use 'idColumn' values as primary keys.
        - Merge data from multiple fields based on 'ID'.
        - Include secondary columns with field-specific prefixes.

        Example:
        For each 'ID', the record will contain:
        {
            'ID': ...,
            'Field1': ...,
            'Field1.SecondaryCol1': ...,
            'Field2': ...,
            'Field2.SecondaryCol1': ...,
            ...
        }

        """
        col_types = {}  # Column name -> type
        cols: dict[str, list] = {}  # Column name -> list of values

        for field_name, content in results.items():
            id_column = content.get("idColumn", {})
            value_column = content.get("valuesColumn", {})
            secondary_columns = content.get("secondaryColumns", [])

            cols["ID"] = id_column.get("values", [])
            cols[field_name] = value_column.get("values", [])

            col_types["ID"] = id_column.get("type", str)
            col_types[field_name] = value_column.get("type", str)

            # Process secondary columns
            for sec_col in secondary_columns:
                sec_col_name = sec_col.get("name", "")
                sec_col_values = sec_col.get("values", [])
                # Use a composite key with field name to avoid conflicts
                full_sec_col_name = f"{field_name}.{sec_col_name}"
                cols[full_sec_col_name] = sec_col_values
                col_types[full_sec_col_name] = sec_col.get("type", str)

        return cols, col_types
