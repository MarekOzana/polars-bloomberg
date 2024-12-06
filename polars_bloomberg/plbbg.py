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
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence

import blpapi
import polars as pl

# Configure logging
# logging.basicConfig(level=logging.DEBUG)
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

    def bql(self, expression: str) -> pl.DataFrame:
        """Fetch data using a BQL expression."""
        request = self._create_bql_request(expression)
        responses = self._send_request(request)
        data, schema = self._parse_bql_responses(responses)
        return pl.DataFrame(data, schema=schema)

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

    def _create_bql_request(self, expression: str) -> blpapi.Request:
        """Create a BQL request."""
        service = self.session.getService("//blp/bqlsvc")
        request = service.createRequest("sendQuery")
        request.set("expression", expression)
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
        all_col_types = {}
        for response in responses:
            # Parse JSON string if necessary
            if isinstance(response, str):
                try:
                    response_dict = json.loads(response.replace("'", '"'))
                except json.JSONDecodeError:
                    logger.warning("Invalid JSON string in response.")
                    continue
            elif isinstance(response, dict):
                response_dict = response
            else:
                logger.warning(f"Unexpected response type: {type(response)}.")
                continue

            results = response_dict.get("results", {})

            col_type = self._parse_bql_response_dict(data, results)
            all_col_types.update(col_type)

        # Define mapping from string types to Polars types
        type_mapping = {
            "STRING": pl.String,
            "DOUBLE": pl.Float64,
            "INT": pl.Int64,
            "DATE": pl.Date,
        }
        # Create polars schema
        schema = {
            col: type_mapping.get(dtype, pl.Utf8)
            for col, dtype in all_col_types.items()
        }
        # convert DATE-typed strings into datetime.date
        for col, dtype in schema.items():
            if dtype == pl.Date:
                # Convert the column data to datetime.date objects
                for entry in data:
                    entry[col] = datetime.strptime(entry[col], "%Y-%m-%dT%H:%M:%SZ").date()

        logger.info(f"Total records parsed: {len(data)}")
        logger.debug(f"schemas: {schema}")
        return data, schema

    def _parse_bql_response_dict(
        self, data: List[Dict], results: Dict[str, Any]
    ) -> Dict[str, str]:
        """
        Parse BQL response dictionary into a table format.

        Parameters:
            data (List[Dict]): The list to append parsed records to.
            results (Dict[str, Any]): The 'results' dictionary from the BQL response.

        Returns:
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
        col_types = {}
        if not results:
            return col_types

        # Initialize a dictionary to hold records by 'ID'
        id_to_record = {}

        for field_name, content in results.items():
            id_column = content.get("idColumn", {})
            value_column = content.get("valuesColumn", {})
            secondary_columns = content.get("secondaryColumns", [])

            id_values = id_column.get("values", [])
            value_values = value_column.get("values", [])

            col_types["ID"] = id_column.get("type", str)
            col_types[field_name] = value_column.get("type", str)

            # Process secondary columns
            secondary_data = {}
            for sec_col in secondary_columns:
                sec_col_name = sec_col.get("name", "")
                sec_col_values = sec_col.get("values", [])
                # Use a composite key with field name to avoid conflicts
                full_sec_col_name = f"{field_name}.{sec_col_name}"
                secondary_data[full_sec_col_name] = sec_col_values
                col_types[full_sec_col_name] = sec_col.get("type", str)

            for idx, id_value in enumerate(id_values):
                # Initialize record if not already present
                if id_value not in id_to_record:
                    id_to_record[id_value] = {"ID": id_value}

                record = id_to_record[id_value]
                # Add main value
                main_value = value_values[idx] if idx < len(value_values) else None
                record[field_name] = main_value

                # Add secondary values
                for sec_col_name, sec_values in secondary_data.items():
                    sec_value = sec_values[idx] if idx < len(sec_values) else None
                    record[sec_col_name] = sec_value

                logger.debug(f"Record for ID '{id_value}': {record}")

        # Convert records to a list
        data.extend(id_to_record.values())
        return col_types
