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
from datetime import date, datetime, timezone
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
    """Holds the result of a BQL query as a list of Polars DataFrames.

    This class encapsulates the results of a Bloomberg Query Language (BQL) query,
    providing methods to access and manipulate the data.

    Attributes:
        dataframes (list[pl.DataFrame]): List of query result dataframes.
        names (list[str]): List of data-item names corresponding to dataframes.

    Example:
        Execute a BQL query and combine the results:

        ```python
        from polars_bloomberg import BQuery

        with BQuery() as bq:
            result = bq.bql("get(px_last) for(['IBM US Equity', 'MSFT US Equity'])")
            df = result.combine()
            print(df)
        ```

        Expected output:
        ```python
        shape: (2, 4)
        ┌───────────────┬─────────┐
        │ ID            ┆ PX_LAST │
        │ ---           ┆ ---     │
        │ str           ┆ f64     │
        ╞═══════════════╪═════════╡
        │ IBM US Equity ┆ 125.34  │
        │ MSFT US Equity┆ 232.33  │
        └───────────────┴─────────┘
        ```

        Iterate over the list of DataFrames:

        ```python
        for df in result:
            print(df)
        ```

        Access individual DataFrames by index:

        ```python
        first_df = result[0]
        print(first_df)
        ```

        Get the number of DataFrames:

        ```python
        num_dfs = len(result)
        print(f"Number of DataFrames: {num_dfs}")
        ```

    Methods:
        combine: Combine all dataframes into one by joining on common columns.

    """

    dataframes: list[pl.DataFrame]
    names: list[str]

    def combine(self) -> pl.DataFrame:
        """Combine all dataframes into one by joining on common columns.

        This method merges all the DataFrames in the `dataframes` attribute into a single
        DataFrame by performing a full join on the common columns. If no common columns
        are found, it raises a ValueError.

        Returns:
            pl.DataFrame: Combined dataframe joined on common columns.

        Raises:
            ValueError: If no common columns exist or no dataframes are present.

        Example:
            Combine results of a BQL query:

            ```python
            from polars_bloomberg import BQuery

            with BQuery() as bq:
                result = bq.bql("get(px_last, px_volume) for(['AAPL US Equity', 'MSFT US Equity'])")
                df = result.combine()
                print(df)
            ```

            Expected output:
            ```python
            shape: (2, 3)
            ┌────────────────┬──────────┬────────────┐
            │ ID             ┆ PX_LAST  ┆ PX_VOLUME  │
            │ ---            ┆ ---      ┆ ---        │
            │ str            ┆ f64      ┆ f64        │
            ╞════════════════╪══════════╪════════════╡
            │ AAPL US Equity ┆ 150.25   ┆ 30000000.0 │
            │ MSFT US Equity ┆ 250.80   ┆ 20000000.0 │
            └────────────────┴──────────┴────────────┘
            ```

            Handle no common columns:

            ```python
            with BQuery() as bq:
                result = bq.bql("get(px_last) for(['AAPL US Equity'])")
                try:
                    df = result.combine()
                except ValueError as e:
                    print(e)
            ```

            Expected output:
            ```
            No common columns found to join on.
            ```

        """  # noqa: E501
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
    """Provides methods to query Bloomberg API and return data as Polars DataFrames.

    Example:
        Create a BQuery instance and fetch last price for Apple stock:

        ```python
        from polars_bloomberg import BQuery

        with BQuery() as bq:
            df = bq.bdp(['AAPL US Equity'], ['PX_LAST'])
        print(df)
        ```

        Expected output:
        ```python
        shape: (1, 2)
        ┌────────────────┬──────────┐
        │ security       ┆ PX_LAST  │
        │ ---            ┆ ---      │
        │ str            ┆ f64      │
        ╞════════════════╪══════════╡
        │ AAPL US Equity ┆ 171.32   │
        └────────────────┴──────────┘
        ```

    """

    session: blpapi.Session

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8194,
        timeout: int = 32_000,
        debug: bool = False,
    ) -> None:
        """Initialize a BQuery instance with connection parameters.

        Args:
            host (str, optional):
                The hostname for the Bloomberg API server.
                Defaults to "localhost".
            port (int, optional):
                The port number for the Bloomberg API server.
                Defaults to 8194.
            timeout (int, optional):
                Timeout in milliseconds for API requests.
                Defaults to 32000.
            debug (bool, optional):
                Enable debug logging/saving of intermediate results.
                Defaults to False.

        Raises:
            ConnectionError: If unable to establish connection to Bloomberg API.

        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self.debug = debug

    def __enter__(self):  # noqa: D105
        # Enter the runtime context related to this object.
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

    def __exit__(self, exc_type, exc_val, exc_tb):  # noqa: D105
        # Exit the context manager and stop the Bloomberg session.
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

        Args:
            securities (list[str]): List of security identifiers (e.g. 'AAPL US Equity').
            fields (list[str]): List of data fields to retrieve (e.g., 'PX_LAST').
            overrides (list[tuple], optional): List of tuples for field overrides. Defaults to None.
            options (dict, optional): Additional request options. Defaults to None.

        Returns:
            pl.DataFrame: A Polars DataFrame containing the requested reference data.

        Raises:
            ConnectionError: If there is an issue with the Bloomberg session.
            ValueError: If the request parameters are invalid.

        Example:
            Fetch last price for Apple and Microsoft stocks:

            ```python
            from polars_bloomberg import BQuery

            with BQuery() as bq:
                df = bq.bdp(['AAPL US Equity', 'MSFT US Equity'], ['PX_LAST'])
            print(df)
            ```

            Expected output:
            ```python
            shape: (2, 2)
            ┌────────────────┬──────────┐
            │ security       ┆ PX_LAST  │
            │ ---            ┆ ---      │
            │ str            ┆ f64      │
            ╞════════════════╪══════════╡
            │ AAPL US Equity ┆ 171.32   │
            │ MSFT US Equity ┆ 232.33   │
            └────────────────┴──────────┘
            ```

        """  # noqa: E501
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

        Args:
            securities (list[str]): List of security identifiers (e.g., 'AAPL US Equity').
            fields (list[str]): List of data fields to retrieve (e.g., 'PX_LAST').
            start_date (date): Start date for the historical data.
            end_date (date): End date for the historical data.
            overrides (list[tuple], optional): List of tuples for field overrides. Defaults to None.
            options (dict, optional): Additional request options. Defaults to None.

        Returns:
            pl.DataFrame: A Polars DataFrame containing the requested historical data.

        Raises:
            ConnectionError: If there is an issue with the Bloomberg session.
            ValueError: If the request parameters are invalid.

        Example:
            Fetch historical closing prices for TLT:

            ```python
            from datetime import date
            from polars_bloomberg import BQuery

            with BQuery() as bq:
                df = bq.bdh(
                    ["TLT US Equity"],
                    ["PX_LAST"],
                    start_date=date(2019, 1, 1),
                    end_date=date(2019, 1, 7),
                )
            print(df)
            ```

            Expected output:
            ```python
            shape: (4, 3)
            ┌───────────────┬────────────┬─────────┐
            │ security      ┆ date       ┆ PX_LAST │
            │ ---           ┆ ---        ┆ ---     │
            │ str           ┆ date       ┆ f64     │
            ╞═══════════════╪════════════╪═════════╡
            │ TLT US Equity ┆ 2019-01-02 ┆ 122.15  │
            │ TLT US Equity ┆ 2019-01-03 ┆ 123.54  │
            │ TLT US Equity ┆ 2019-01-04 ┆ 122.11  │
            │ TLT US Equity ┆ 2019-01-07 ┆ 121.75  │
            └───────────────┴────────────┴─────────┘
            ```

        """  # noqa: E501
        request = self._create_request(
            "HistoricalDataRequest", securities, fields, overrides, options
        )
        request.set("startDate", start_date.strftime("%Y%m%d"))
        request.set("endDate", end_date.strftime("%Y%m%d"))
        responses = self._send_request(request)
        data = self._parse_bdh_responses(responses, fields)
        return pl.DataFrame(data, infer_schema_length=None)

    def bdib(  # noqa: PLR0913
        self,
        security: str,
        event_type: str,
        interval: int,
        start_datetime: datetime,
        end_datetime: datetime,
        overrides: Sequence | None = None,
        options: dict | None = None,
    ) -> pl.DataFrame:
        """Fetch intraday bars from Bloomberg, mirroring Excel's BDIB() function.

        Args:
            security (str): Instrument identifier (for example 'AAPL US Equity').
            event_type (str): One of TRADE, BID, ASK, BEST_BID, BEST_ASK.
            interval (int): Bar length in minutes (1-1440).
            start_datetime (datetime): First bar timestamp; naive vals are treated as UTC
                tz-aware values are converted to UTC before the request is sent.
            end_datetime (datetime): Last bar timestamp; handled same way as start_dtm
            overrides (Sequence | None, optional): Sequence of (field, value) overrides.
            options (dict | None, optional): Additional Bloomberg request options.

        Returns:
            pl.DataFrame: Bars sorted by security/time with columns
                ['security', 'time', 'open', 'high', 'low', 'close', 'volume',
                'numEvents', 'value']. Bloomberg emits `time` in UTC and the DataFrame
                preserves that timezone.

        Example:
            ```python
            from datetime import datetime
            from polars_bloomberg import BQuery

            with BQuery() as bq:
                df = bq.bdib(
                    "OMX Index",
                    event_type="TRADE",
                    interval=60,
                    start_datetime=datetime(2025, 11, 5),
                    end_datetime=datetime(2025, 11, 6),
                )
                print(df)
            ```

             Expected output:
            ```python
            shape: (4, 3)
            ┌───────────┬──────────────┬──────────┬──────────┬───┬──────────┬────────┬───────────┬───────┐
            │ security  ┆ time         ┆ open     ┆ high     ┆ … ┆ close    ┆ volume ┆ numEvents ┆ value │
            │ ---       ┆ ---          ┆ ---      ┆ ---      ┆   ┆ ---      ┆ ---    ┆ ---       ┆ ---   │
            │ str       ┆ datetime[μs] ┆ f64      ┆ f64      ┆   ┆ f64      ┆ i64    ┆ i64       ┆ f64   │
            ╞═══════════╪══════════════╪══════════╪══════════╪═══╪══════════╪════════╪═══════════╪═══════╡
            │ OMX Index ┆ 2025-11-05   ┆ 2726.603 ┆ 2742.014 ┆ … ┆ 2739.321 ┆ 0      ┆ 3591      ┆ 0.0   │
            │           ┆ 08:00:00     ┆          ┆          ┆   ┆          ┆        ┆           ┆       │
            │ OMX Index ┆ 2025-11-05   ┆ 2739.466 ┆ 2739.706 ┆ … ┆ 2733.836 ┆ 0      ┆ 3600      ┆ 0.0   │
            │           ┆ 09:00:00     ┆          ┆          ┆   ┆          ┆        ┆           ┆       │
            │ OMX Index ┆ 2025-11-05   ┆ 2733.747 ┆ 2734.827 ┆ … ┆ 2731.724 ┆ 0      ┆ 3600      ┆ 0.0   │
            │           ┆ 10:00:00     ┆          ┆          ┆   ┆          ┆        ┆           ┆       │
            │ OMX Index ┆ 2025-11-05   ┆ 2731.721 ┆ 2742.015 ┆ … ┆ 2741.185 ┆ 0      ┆ 3600      ┆ 0.0   │
            │           ┆ 11:00:00     ┆          ┆          ┆   ┆          ┆        ┆           ┆       │
            │ OMX Index ┆ 2025-11-05   ┆ 2741.256 ┆ 2747.291 ┆ … ┆ 2747.291 ┆ 0      ┆ 3600      ┆ 0.0   │
            │           ┆ 12:00:00     ┆          ┆          ┆   ┆          ┆        ┆           ┆       │
            │ OMX Index ┆ 2025-11-05   ┆ 2747.291 ┆ 2748.815 ┆ … ┆ 2748.287 ┆ 0      ┆ 3600      ┆ 0.0   │
            │           ┆ 13:00:00     ┆          ┆          ┆   ┆          ┆        ┆           ┆       │
            │ OMX Index ┆ 2025-11-05   ┆ 2748.273 ┆ 2752.301 ┆ … ┆ 2752.181 ┆ 0      ┆ 3600      ┆ 0.0   │
            │           ┆ 14:00:00     ┆          ┆          ┆   ┆          ┆        ┆           ┆       │
            │ OMX Index ┆ 2025-11-05   ┆ 2752.181 ┆ 2758.978 ┆ … ┆ 2752.495 ┆ 0      ┆ 3600      ┆ 0.0   │
            │           ┆ 15:00:00     ┆          ┆          ┆   ┆          ┆        ┆           ┆       │
            │ OMX Index ┆ 2025-11-05   ┆ 2752.402 ┆ 2752.85  ┆ … ┆ 2751.404 ┆ 0      ┆ 2100      ┆ 0.0   │
            │           ┆ 16:00:00     ┆          ┆          ┆   ┆          ┆        ┆           ┆       │
            └───────────┴──────────────┴──────────┴──────────┴───┴──────────┴────────┴───────────┴───────┘
            ```

        """  # noqa: E501
        request = self._create_intraday_bar_request(
            security=security,
            event_type=event_type,
            interval=interval,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            overrides=overrides,
            options=options,
        )
        responses = self._send_request(request)
        data = self._parse_bdib_responses(responses, fallback_security=security)
        schema = {
            "security": pl.Utf8,
            "time": pl.Datetime,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Int64,
            "numEvents": pl.Int64,
            "value": pl.Float64,
        }
        df = pl.DataFrame(data, schema=schema, strict=False, infer_schema_length=None)
        return df.sort(["security", "time"]) if not df.is_empty() else df

    def bql(self, expression: str) -> BqlResult:
        """Execute a Bloomberg Query Language (BQL) query.

        BQL is Bloomberg's domain-specific language for complex financial queries. It allows
        for advanced data retrieval, screening, and analysis.

        Args:
            expression (str): The BQL query expression to execute. Can include functions like
                get(), let(), for(), filter(), etc.

        Returns:
            BqlResult: An object containing:
                - List of Polars DataFrames (one for each item in BQL get statement)
                - Helper methods like combine() to merge DataFrames on common columns

        Raises:
            ConnectionError: If there is an issue with the Bloomberg session.
            ValueError: If the BQL query syntax is invalid.

        Example:
            Simple query to fetch last price:

            ```python
            from polars_bloomberg import BQuery

            with BQuery() as bq:
                # Get last price for multiple securities
                result = bq.bql("get(px_last) for(['IBM US Equity', 'MSFT US Equity'])")
                df = result.combine()
                print(df)
            ```

            Expected output:
            ```python
            shape: (2, 4)
            ┌───────────────┬─────────┐
            │ ID            ┆ PX_LAST │
            │ ---           ┆ ---     │
            │ str           ┆ f64     │
            ╞═══════════════╪═════════╡
            │ AAPL US Equity┆ 150.25  │
            │ MSFT US Equity┆ 250.80  │
            └───────────────┴─────────┘
            ```

            Access individual DataFrames:
            ```python
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
            ```
            Fetch multiple fields and combine results:
            ```python
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
            ```
            Iterate over individual DataFrames:
            ```python
            >>> for df in result:
            ...     print(df)
            ```

        """  # noqa: E501
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

    def _create_intraday_bar_request(  # noqa: PLR0913
        self,
        security: str,
        event_type: str,
        interval: int,
        start_datetime: datetime,
        end_datetime: datetime,
        overrides: Sequence | None,
        options: dict | None,
    ) -> blpapi.Request:
        """Create an IntradayBarRequest with overrides and options support."""
        service = self.session.getService("//blp/refdata")
        request = service.createRequest("IntradayBarRequest")
        request.set("security", security)
        request.set("eventType", event_type)
        request.set("interval", interval)
        request.set("startDateTime", self._format_datetime(start_datetime))
        request.set("endDateTime", self._format_datetime(end_datetime))

        if overrides:
            overrides_element = request.getElement("overrides")
            for field_id, value in overrides:
                override_element = overrides_element.appendElement()
                override_element.setElement("fieldId", field_id)
                override_element.setElement("value", value)

        if options:
            for key, value in options.items():
                request.set(key, value)

        return request

    def _create_bql_request(self, expression: str) -> blpapi.Request:
        """Create a BQL request."""
        service = self.session.getService("//blp/bqlsvc")
        request = service.createRequest("sendQuery")
        request.set("expression", expression)
        # BLPAPI requires setting sub-elements on the sequence element.
        try:
            ctx = request.getElement("clientContext")
            ctx.setElement("appName", "EXCEL")
        except blpapi.NotFoundException:
            logger.debug(
                "BQL request has no 'clientContext' element in this SDK/schema."
            )

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

    def _parse_bdib_responses(
        self, responses: list[dict], fallback_security: str | None = None
    ) -> list[dict]:
        bars: list[dict[str, Any]] = []
        for response in responses:
            bar_data = response.get("barData", {})
            security = bar_data.get("security") or fallback_security
            entries = bar_data.get("barTickData", [])
            for entry in entries:
                bar_entry = entry.get("barTickData", entry)
                record = {
                    "security": security,
                    "time": bar_entry.get("time"),
                    "open": bar_entry.get("open"),
                    "high": bar_entry.get("high"),
                    "low": bar_entry.get("low"),
                    "close": bar_entry.get("close"),
                    "volume": bar_entry.get("volume"),
                    "numEvents": bar_entry.get("numEvents"),
                    "value": bar_entry.get("value"),
                }
                bars.append(record)
        return bars

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
                    resp_dict = json.loads(response)
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

    @staticmethod
    def _format_datetime(value: datetime | str) -> str:
        """Convert datetime objects to Bloomberg's ISO8601 string format."""
        if isinstance(value, str):
            return value
        if value.tzinfo is None:
            return value.strftime("%Y-%m-%dT%H:%M:%S")
        return value.astimezone(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

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
