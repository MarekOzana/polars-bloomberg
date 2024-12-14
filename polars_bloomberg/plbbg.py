"""Polars interface to Bloomberg Open API.

This module provides a Polars-based interface to interact with the Bloomberg Open API.

Usage
-----
.. code-block:: python

    from datetime import date
    from polars_bloomberg import BQuery

    with BQuery() as bq:
        df_ref = bq.bdp(['AAPL US Equity', 'MSFT US Equity'], ['PX_LAST'])
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
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import blpapi
import polars as pl

# Configure logging
# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@dataclass
class SITable:
    """Holds data and schema for a Single Item response Table."""

    name: str  # data item name
    data: dict[str, list[Any]]  # column_name -> list of values
    schema: dict[str, pl.DataType]  # column_name -> Polars datatype


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
        overrides: list[tuple] | None = None,
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
        overrides: list[tuple] | None = None,
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
        """Fetch data using a BQL expression.

        Returns
        -------
        list[pl.DataFrame]

        """
        request = self._create_bql_request(expression)
        responses = self._send_request(request)
        tables = self._parse_bql_responses(responses)
        return [
            pl.DataFrame(table.data, schema=table.schema, strict=True)
            for table in tables
        ]

    def _create_request(
        self,
        request_type: str,
        securities: list[str],
        fields: list[str],
        overrides: Sequence | None = None,
        options: dict | None = None,
    ) -> blpapi.Request:
        """Create a Bloomberg request with support for overrides and additional options."""
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
        """Send a Bloomberg request and collect responses with timeout handling."""
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
        """Parse BQL responses into a list of SITable objects."""
        tables: list[SITable] = []
        results: list[dict] = self._extract_results(responses)

        for result in results:
            tables.extend(self._parse_result(result))
        return [self._apply_schema(table) for table in tables]

    def _apply_schema(self, table: SITable) -> SITable:
        """Convert data based on the schema (e.g., str -> date, 'NaN' -> None)."""
        date_format = "%Y-%m-%dT%H:%M:%SZ"
        for col, dtype in table.schema.items():
            if dtype == pl.Date:
                table.data[col] = [
                    datetime.strptime(v, date_format).date()
                    if isinstance(v, str)
                    else None
                    for v in table.data[col]
                ]
            elif dtype in {pl.Float64, pl.Int64}:
                table.data[col] = [None if x == "NaN" else x for x in table.data[col]]
        return table

    def _extract_results(self, responses: list[Any]) -> list[dict]:
        """Extract the 'results' section from each response, handling JSON strings."""
        extracted = []
        for response in responses:
            resp_dict = response
            if isinstance(response, str):
                try:
                    resp_dict = json.loads(response.replace("'", '"'))
                except json.JSONDecodeError as e:
                    logger.error("Failed to decode JSON: %s. Error: %s", response, e)
                    continue
            results = resp_dict.get("results")
            if results:
                extracted.append(results)
        return extracted

    def _parse_result(self, results: dict[str, Any]) -> list[SITable]:
        """Convert a single BQL results dictionary into a list[SITable]."""
        tables: list[SITable] = []
        for field, content in results.items():
            data = {}
            schema_str = {}

            data["ID"] = content.get("idColumn", {}).get("values", [])
            data[field] = content.get("valuesColumn", {}).get("values", [])

            schema_str["ID"] = content.get("idColumn", {}).get("type", "STRING")
            schema_str[field] = content.get("valuesColumn", {}).get("type", "STRING")

            # Process secondary columns
            for sec_col in content.get("secondaryColumns", []):
                name = sec_col.get("name", "")
                data[name] = sec_col.get("values", [])
                schema_str[name] = sec_col.get("type", str)
            schema = self._map_types(schema_str)
            tables.append(SITable(name=field, data=data, schema=schema))

        return tables

    def _map_types(self, type_map: dict[str, str]) -> dict[str, pl.DataType]:
        """Map string-based types to Polars data types. Default to Utf8."""
        mapping = {
            "STRING": pl.Utf8,
            "DOUBLE": pl.Float64,
            "INT": pl.Int64,
            "DATE": pl.Date,
        }
        return {col: mapping.get(t.upper(), pl.Utf8) for col, t in type_map.items()}


def chain(dataframes: list[pl.DataFrame]) -> pl.DataFrame:
    """Chain full outer join a list of Polars DataFrames on their common columns.

    Parameters
    ----------
    dataframes: list[pl.DataFrame]
        A list of Polars DataFrames to join.

    Returns
    -------
        pl.DataFrame: The resulting DataFrame after chaining full outer joins.

    Raises
    ------
        ValueError: If the input list is empty or if no common columns are found during a join.

    Usage
    -----
    >>> df_lst = bq.bql("get(px_last, name) for(members('OMX Index')))
    >>> df = plbbg.chain(df_lst)

    TODO: test
    """

    def _get_common_cols(left: pl.DataFrame, right: pl.DataFrame) -> List[str]:
        """Return a list of common column names between two DataFrames."""
        return list(set(left.columns) & set(right.columns))

    if not dataframes:
        raise ValueError("The list of DataFrames is empty.")

    result_df: pl.DataFrame = dataframes[0]  # Initialize with the first DataFrame
    for df in dataframes[1:]:
        common_cols: list[str] = _get_common_cols(result_df, df)  # Find common columns
        if not common_cols:
            raise ValueError("No common columns to join on.")
        # Perform full outer join on common columns with coalescence
        result_df = result_df.join(df, on=common_cols, how="full", coalesce=True)
    return result_df
