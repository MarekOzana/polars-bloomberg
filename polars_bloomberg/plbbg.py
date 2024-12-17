"""Polars interface to Bloomberg Open API.

polars-bloomberg is a Python library that fetches Bloomberg financial data directly into
Polars DataFrames. It offers user-friendly methods such as `bdp()`, `bdh()`, and `bql()`
for efficient data retrieval and analysis.

Usage
-----
```python
from datetime import date
from polars_bloomberg import BQuery

with BQuery() as bq:
    # Fetch reference data
    df_ref = bq.bdp(['AAPL US Equity', 'MSFT US Equity'], ['PX_LAST'])

    # Fetch historical data
    df_hist = bq.bdh(
        ['AAPL US Equity'],
        ['PX_LAST'],
        date(2020, 1, 1),
        date(2020, 1, 30)
    )

    # Execute BQL query
    df_lst = bq.bql("get(px_last) for(['IBM US Equity', 'AAPL US Equity'])")
```

:author: Marek Ozana
:date: 2024-12
"""

import json
import logging
import os
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


@dataclass
class BqlResult:
    """Holds the result of a BQL query: list of Polars DataFrames."""

    dataframes: list[pl.DataFrame]
    names: list[str]  # data-item names

    def combine(self) -> pl.DataFrame:
        """Combine all dataframes into one by joining on common columns.

        Raises
        ------
        ValueError:
            If no common columns exist or no dataframes are present.

        """
        if not self.dataframes:
            raise ValueError("No DataFrames to combine.")

        result = self.dataframes[0]  # Initialize with the first DataFrame
        for df in self.dataframes[1:]:
            common_cols = set(result.columns) & set(df.columns)
            if not common_cols:
                raise ValueError("No common columns found to join on.")
            result = result.join(df, on=list(common_cols), how="full", coalesce=True)
        return result

    def __getitem__(self, idx: int) -> pl.DataFrame:
        """Access individual DataFrames by index."""
        return self.dataframes[idx]

    def __len__(self) -> int:
        """Return the number of dataframes."""
        return len(self.dataframes)

    def __iter__(self):
        """Return an iterator over the dataframes."""
        return iter(self.dataframes)


class BQuery:
    """Interface for interacting with the Bloomberg Open API using Polars."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8194,
        timeout: int = 32_000,
        debug: bool = False,
    ):
        """Initialize a BQuery instance with connection parameters.

        Parameters
        ----------
        host : str
            The hostname for the Bloomberg API server.
        port : int
            The port number for the Bloomberg API server.
        timeout : int
            Timeout in milliseconds for API requests.
        debug: bool
            Enable debug logging/saving of intermediate results.

        """
        self.host = host
        self.port = port
        self.timeout = timeout  # Timeout in milliseconds
        self.session = None
        self.debug = debug  # Enable/disable debug logging of intermediate results.

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

    def bql(self, expression: str) -> BqlResult:
        """Execute a BQL (Bloomberg Query Language) expression and retrieve the results.

        Parameters
        ----------
        expression : str
            The BQL query expression to execute.

        Returns
        -------
        BqlResult
            An object containing a list of Polars DataFrames and helper methods.

        Examples
        --------
        Fetch the last price for multiple securities:

        >>> from polars_bloomberg import BQuery
        >>> with BQuery() as bq:
        ...     result = bq.bql("get(px_last) for(['AAPL US Equity', 'MSFT US Equity'])")
        >>> df = result.combine()
        >>> print(df)
        shape: (2, 2)
        ┌───────────────┬─────────┐
        │ ID            ┆ PX_LAST │
        │ ---           ┆ ---     │
        │ str           ┆ f64     │
        ╞═══════════════╪═════════╡
        │ AAPL US Equity┆ 150.25  │
        │ MSFT US Equity┆ 250.80  │
        └───────────────┴─────────┘

        Access individual DataFrames:

        >>> df_px_last = result[0]
        >>> print(df_px_last)
        shape: (2, 2)
        ┌───────────────┬─────────┐
        │ ID            ┆ PX_LAST │
        │ ---           ┆ ---     │
        │ str           ┆ f64     │
        ╞═══════════════╪═════════╡
        │ AAPL US Equity┆ 150.25  │
        │ MSFT US Equity┆ 250.80  │
        └───────────────┴─────────┘

        Fetch multiple fields and combine results:

        >>> result = bq.bql("get(px_last, px_volume) for('AAPL US Equity')")
        >>> df_combined = result.combine()
        >>> print(df_combined)
        shape: (1, 3)
        ┌───────────────┬─────────┬────────────┐
        │ ID            ┆ PX_LAST ┆ PX_VOLUME  │
        │ ---           ┆ ---     ┆ ---        │
        │ str           ┆ f64     ┆ f64        │
        ╞═══════════════╪═════════╪════════════╡
        │ AAPL US Equity┆ 150.25  ┆ 30000000.0 │
        └───────────────┴─────────┴────────────┘

        Iterate over individual DataFrames:

        >>> for df in result:
        ...     print(df)

        """
        request = self._create_bql_request(expression)
        responses = self._send_request(request)
        tables = self._parse_bql_responses(responses)
        dataframes = [
            pl.DataFrame(table.data, schema=table.schema, strict=True)
            for table in tables
        ]
        names = [table.name for table in tables]
        return BqlResult(dataframes, names)

    def _create_request(
        self,
        request_type: str,
        securities: list[str],
        fields: list[str],
        overrides: Sequence | None = None,
        options: dict | None = None,
    ) -> blpapi.Request:
        """Create a Bloomberg request with support for overrides and options."""
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
                    (
                        datetime.strptime(v, date_format).date()
                        if isinstance(v, str)
                        else None
                    )
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

        # If debug mode is on, save the input and output for reproducibility
        if self.debug:
            self._save_debug_case(results, tables)

        return tables

    def _map_types(self, type_map: dict[str, str]) -> dict[str, pl.DataType]:
        """Map string-based types to Polars data types. Default to Utf8."""
        mapping = {
            "STRING": pl.Utf8,
            "DOUBLE": pl.Float64,
            "INT": pl.Int64,
            "DATE": pl.Date,
            "BOOLEAN": pl.Boolean,
        }
        return {col: mapping.get(t.upper(), pl.Utf8) for col, t in type_map.items()}

    def _save_debug_case(self, in_results: dict, tables: list[SITable]):
        """Save input and output to a JSON file for debugging and test generation."""
        # Create a directory for debug cases if it doesn't exist
        os.makedirs("debug_cases", exist_ok=True)

        # Create a unique filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"debug_cases/bql_parse_results_{timestamp}.json"

        # Prepare serializable data
        out_tables = []
        for t in tables:
            out_tables.append(
                {
                    "name": t.name,
                    "data": t.data,
                    "schema": {col: str(dtype) for col, dtype in t.schema.items()},
                }
            )

        to_save = {"in_results": in_results, "out_tables": out_tables}

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(to_save, f, indent=2)

        logger.debug("Saved debug case to %s", filename)
