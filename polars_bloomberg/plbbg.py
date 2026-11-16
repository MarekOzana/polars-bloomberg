"""Polars interface to Bloomberg Open API.

polars-bloomberg is a Python library that fetches Bloomberg financial data directly into
Polars DataFrames through five methods: `bdp()`, `bdh()`, `bdib()`, `bql()` and
`bsrch()`, each one Bloomberg request in, one DataFrame (or `BqlResult`) out.

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

import dataclasses
import json
import logging
import os
from collections.abc import Collection, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from types import TracebackType
from typing import Any

import blpapi
import polars as pl

logger = logging.getLogger(__name__)


class BloombergError(ValueError):
    """Bloomberg rejected or failed a request."""


@dataclass
class BqlResult:
    """Holds the result of a BQL query as a list of Polars DataFrames.

    This class encapsulates the results of a Bloomberg Query Language (BQL) query,
    providing methods to access and manipulate the data.

    Attributes:
        dataframes (list[pl.DataFrame]): List of query result dataframes.
        names (list[str]): List of data-item names corresponding to dataframes.
        errors (list[str]): Error messages BQL returned with the answer; empty when
            the query succeeded. A failed query has no dataframes and its message
            here.

    Example:
        Execute a BQL query and combine the results:

        ```python
        from polars_bloomberg import BQuery

        with BQuery() as bq:
            result = bq.bql("get(px_last) for(['IBM US Equity', 'MSFT US Equity'])")
            df = result.combine(on="ID")
            print(df)
        ```

        Expected output:
        ```python
        shape: (2, 4)
        ┌────────────────┬─────────┬────────────┬──────────┐
        │ ID             ┆ px_last ┆ DATE       ┆ CURRENCY │
        │ ---            ┆ ---     ┆ ---        ┆ ---      │
        │ str            ┆ f64     ┆ date       ┆ str      │
        ╞════════════════╪═════════╪════════════╪══════════╡
        │ IBM US Equity  ┆ 247.13  ┆ 2026-11-16 ┆ USD      │
        │ MSFT US Equity ┆ 523.88  ┆ 2026-11-16 ┆ USD      │
        └────────────────┴─────────┴────────────┴──────────┘
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
        combine: Combine all dataframes into one; ``combine(on="ID")`` gives one
            row per security.

    """

    dataframes: list[pl.DataFrame]
    names: list[str]
    errors: list[str] = dataclasses.field(default_factory=list)

    def combine(
        self,
        on: str | Sequence[str] | None = None,
        *,
        how: str = "full",
        allow_common_columns: bool = True,
    ) -> pl.DataFrame:
        """Combine all dataframes into one.

        With no arguments, this method preserves the legacy behavior exactly: it
        merges all DataFrames by performing a full join on every column they share,
        not only ``ID``. A row whose shared columns hold nulls matches nothing, so it
        appears twice: once from each side. This happens, for example, when BQL has
        no ``DATE`` for a security in two items. If no common columns are found, it
        raises a ValueError.

        Use ``combine(on="ID")`` for one row per security. When ``on`` is provided,
        only those columns are used as join keys. Other overlapping columns are
        preserved with a suffix unless ``allow_common_columns`` is false.

        Args:
            on: Explicit column or columns to join on. If omitted, legacy common-column
                joining is used.
            how: Polars join strategy to use when ``on`` is provided.
            allow_common_columns: Whether overlapping non-key columns are allowed when
                ``on`` is provided. If true, right-hand overlapping columns are suffixed
                with the corresponding BQL item name.

        Returns:
            pl.DataFrame: Combined dataframe.

        Raises:
            ValueError: If no dataframes are present (the message includes the BQL
                error, if any), if no common columns exist in legacy mode, if
                explicit join keys are missing, or if overlapping non-key columns
                are disallowed.

        Example:
            One row per security; the ``DATE`` of the second item gets a suffix:

            ```python
            from polars_bloomberg import BQuery

            with BQuery() as bq:
                result = bq.bql(
                    "get(px_last, px_volume) "
                    "for(['IBM US Equity', 'NOT A SECURITY Equity'])"
                )
            print(result.combine(on="ID"))
            ```

            Expected output:
            ```python
            shape: (2, 6)
            ┌───────────────────────┬─────────┬────────────┬──────────┬───────────┬────────────────┐
            │ ID                    ┆ px_last ┆ DATE       ┆ CURRENCY ┆ px_volume ┆ DATE_px_volume │
            │ ---                   ┆ ---     ┆ ---        ┆ ---      ┆ ---       ┆ ---            │
            │ str                   ┆ f64     ┆ date       ┆ str      ┆ f64       ┆ date           │
            ╞═══════════════════════╪═════════╪════════════╪══════════╪═══════════╪════════════════╡
            │ IBM US Equity         ┆ 247.13  ┆ 2026-11-16 ┆ USD      ┆ 4750.0    ┆ 2026-11-16     │
            │ NOT A SECURITY Equity ┆ null    ┆ null       ┆ null     ┆ null      ┆ null           │
            └───────────────────────┴─────────┴────────────┴──────────┴───────────┴────────────────┘
            ```

            Without ``on``, the join uses ``ID`` and ``DATE``; the security without
            a ``DATE`` appears twice:

            ```python
            print(result.combine())
            ```

            Expected output:
            ```python
            shape: (3, 5)
            ┌───────────────────────┬─────────┬────────────┬──────────┬───────────┐
            │ ID                    ┆ px_last ┆ DATE       ┆ CURRENCY ┆ px_volume │
            │ ---                   ┆ ---     ┆ ---        ┆ ---      ┆ ---       │
            │ str                   ┆ f64     ┆ date       ┆ str      ┆ f64       │
            ╞═══════════════════════╪═════════╪════════════╪══════════╪═══════════╡
            │ IBM US Equity         ┆ 247.13  ┆ 2026-11-16 ┆ USD      ┆ 4750.0    │
            │ NOT A SECURITY Equity ┆ null    ┆ null       ┆ null     ┆ null      │
            │ NOT A SECURITY Equity ┆ null    ┆ null       ┆ null     ┆ null      │
            └───────────────────────┴─────────┴────────────┴──────────┴───────────┘
            ```

        """  # noqa: E501
        if not self.dataframes:
            if self.errors:
                raise ValueError(
                    "No DataFrames to combine. BQL error: " + "; ".join(self.errors)
                )
            raise ValueError("No DataFrames to combine.")

        if on is not None:
            return self._combine_on(
                on, how=how, allow_common_columns=allow_common_columns
            )

        if how != "full":
            raise ValueError("The 'how' parameter requires explicit join keys via 'on'.")
        if not allow_common_columns:
            raise ValueError(
                "The 'allow_common_columns' parameter requires explicit join keys "
                "via 'on'."
            )

        result = self.dataframes[0]
        for df in self.dataframes[1:]:
            common_cols = [col for col in result.columns if col in df.columns]
            if not common_cols:
                raise ValueError("No common columns found to join on.")
            result = result.join(df, on=common_cols, how="full", coalesce=True)
        return result

    def _combine_on(
        self,
        on: str | Sequence[str],
        *,
        how: str,
        allow_common_columns: bool,
    ) -> pl.DataFrame:
        """Combine dataframes using explicit join keys."""
        join_keys = [on] if isinstance(on, str) else list(on)
        if not join_keys:
            raise ValueError("At least one join column must be provided.")
        if not all(isinstance(col, str) for col in join_keys):
            raise ValueError("Join columns must be strings.")
        if len(join_keys) != len(set(join_keys)):
            raise ValueError("Join columns must be unique.")

        allowed_join_strategies = {"full", "inner", "left", "right"}
        if how not in allowed_join_strategies:
            raise ValueError(
                f"Unsupported join strategy {how!r}. Supported strategies are: "
                f"{sorted(allowed_join_strategies)}."
            )

        for idx, df in enumerate(self.dataframes):
            missing_cols = [col for col in join_keys if col not in df.columns]
            if missing_cols:
                name = self.names[idx] if idx < len(self.names) else str(idx)
                raise ValueError(
                    f"Join columns not found in DataFrame {idx} ({name}): {missing_cols}"
                )

        result = self.dataframes[0]
        join_key_set = set(join_keys)
        for idx, df in enumerate(self.dataframes[1:], start=1):
            overlapping_cols = [
                col
                for col in df.columns
                if col in result.columns and col not in join_key_set
            ]
            if overlapping_cols and not allow_common_columns:
                name = self.names[idx] if idx < len(self.names) else str(idx)
                raise ValueError(
                    f"Overlapping non-key columns found in DataFrame {idx} ({name}): "
                    f"{overlapping_cols}"
                )

            right = self._rename_overlapping_columns(df, idx, result, overlapping_cols)
            result = result.join(right, on=join_keys, how=how, coalesce=True)

        return result

    def _rename_overlapping_columns(
        self,
        df: pl.DataFrame,
        idx: int,
        result: pl.DataFrame,
        overlapping_cols: Sequence[str],
    ) -> pl.DataFrame:
        """Rename right-hand non-key overlaps before explicit-key joins."""
        if not overlapping_cols:
            return df

        label = (
            self.names[idx] if idx < len(self.names) and self.names[idx] else str(idx)
        )
        existing_cols = set(result.columns) | set(df.columns)
        rename_map: dict[str, str] = {}
        for col in overlapping_cols:
            candidate = f"{col}_{label}"
            if candidate in existing_cols:
                candidate = f"{col}_{idx}"

            counter = 2
            while candidate in existing_cols:
                candidate = f"{col}_{idx}_{counter}"
                counter += 1

            rename_map[col] = candidate
            existing_cols.add(candidate)

        return df.rename(rename_map)

    def __getitem__(self, idx: int) -> pl.DataFrame:
        """Access individual DataFrames by index."""
        return self.dataframes[idx]

    def __len__(self) -> int:
        """Return the number of dataframes."""
        return len(self.dataframes)

    def __iter__(self) -> Iterator[pl.DataFrame]:
        """Return an iterator over the dataframes."""
        return iter(self.dataframes)


# --- debug dump: one replayable JSON file per request ---

_EVENT_NAMES = {
    getattr(blpapi.Event, name): name
    for name in ("PARTIAL_RESPONSE", "RESPONSE", "REQUEST_STATUS")
}


def _json_default(value: Any) -> Any:
    """json.dump hook: write dates so that a dump can be replayed with its types."""
    if isinstance(value, datetime):
        return {"$datetime": value.isoformat()}
    if isinstance(value, date):
        return {"$date": value.isoformat()}
    if isinstance(value, time):
        return {"$time": value.isoformat()}
    return str(value)


def _write_debug_dump(
    directory: str | os.PathLike, request: blpapi.Request, events: list[dict]
) -> None:
    """Write one request and the messages it received to a replayable JSON file."""
    element = request.asElement()
    dump = {
        "format": 1,
        "recorded": {
            "date": date.today().isoformat(),
            "blpapi": blpapi.__version__,
            "polars": pl.__version__,
        },
        "request": {"operation": str(element.name()), "body": element.toPy()},
        "events": [event for event in events if event["messages"]],
    }
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    path = os.path.join(directory, f"{element.name()}_{stamp}.json")
    try:
        os.makedirs(directory, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(dump, f, default=_json_default, indent=1, ensure_ascii=False)
            f.write("\n")
    except OSError as error:
        logger.warning("Could not write debug dump %s: %s", path, error)


# --- inputs ---


def _as_list(value: str | Sequence[str]) -> list[str]:
    """Accept one name or several."""
    return [value] if isinstance(value, str) else list(value)


def _yyyymmdd(value: date | str) -> str:
    """Return a date as Bloomberg's YYYYMMDD; accept YYYYMMDD and ISO strings."""
    if isinstance(value, str):
        return value.replace("-", "")
    return value.strftime("%Y%m%d")


def _iso_datetime(value: datetime | date | str) -> str:
    """Return Bloomberg's ISO 8601 text: a date is midnight, a str is sent as is."""
    if isinstance(value, str):
        return value
    if not isinstance(value, datetime):  # a plain date means midnight
        value = datetime.combine(value, time())
    if value.tzinfo is None:
        return value.strftime("%Y-%m-%dT%H:%M:%S")
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _override_pairs(overrides: Mapping[str, Any] | Sequence | None) -> list[tuple]:
    """Return overrides as (name, value) pairs; accept a dict or a sequence of pairs."""
    if not overrides:
        return []
    pairs = overrides.items() if isinstance(overrides, Mapping) else overrides
    checked = []
    for pair in pairs:
        if isinstance(pair, str) or len(pair) != 2:  # noqa: PLR2004
            raise TypeError(
                "overrides must be a dict or a sequence of (name, value) pairs"
            )
        name, value = pair
        if value is None:
            raise TypeError(f"override {name!r} has no value")
        if isinstance(value, date):  # also covers datetime
            value = _yyyymmdd(value)
        checked.append((name, value))
    return checked


def _append_values(request: blpapi.Request, name: str, values: list[str]) -> None:
    """Append each value to an array element of the request, e.g. "securities"."""
    element = request.getElement(name)
    for value in values:
        element.appendValue(value)


def _set_overrides(
    request: blpapi.Request, overrides: Mapping[str, Any] | Sequence | None
) -> None:
    """Check the overrides and write them to a //blp/refdata request."""
    if not overrides:
        return
    overrides_element = request.getElement("overrides")
    for field_id, value in _override_pairs(overrides):
        override = overrides_element.appendElement()
        override.setElement("fieldId", field_id)
        override.setElement("value", value)


def _set_options(request: blpapi.Request, options: dict | None) -> None:
    """Set each option as an element of the request, under Bloomberg's own name."""
    for name, value in (options or {}).items():
        request.set(name, value)


def _check_no_field_is_named(fields: list[str], *key_columns: str) -> None:
    """Refuse a field that would overwrite one of the key columns of the result."""
    if clash := [key for key in key_columns if key in fields]:
        names = " and ".join(repr(key) for key in clash)
        raise ValueError(f"{names} cannot be requested as fields")


def _set_bql_client_context(request: blpapi.Request) -> None:
    """Say that the query comes from Excel, as Bloomberg's own Excel add-in does."""
    # BLPAPI requires setting sub-elements on the sequence element.
    try:
        client_context = request.getElement("clientContext")
        client_context.setElement("appName", "EXCEL")
    except blpapi.NotFoundException:
        logger.debug("BQL request has no 'clientContext' element in this SDK/schema.")


def _set_bsrch_overrides(request: blpapi.Request, overrides: list[tuple]) -> None:
    """Write (name, value) pairs to an ExcelGetGridRequest, which takes them as text."""
    if not overrides:
        return
    overrides_element = request.getElement("Overrides")
    for name, value in overrides:
        override = overrides_element.appendElement()
        override.setElement("name", str(name))
        override.setElement("value", str(value))


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

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8194,
        timeout: int = 32_000,
        debug: bool | str | os.PathLike = False,
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
                The longest wait, in milliseconds, for the next event from
                Bloomberg; it is not a limit on the whole request. ``0`` waits
                forever. Slow BQL queries may need more than the default.
                Defaults to 32000.
            debug (bool | str | os.PathLike, optional):
                Write every request and the raw messages Bloomberg sent back to
                a JSON file, also when the request fails. ``True`` writes to
                ``debug_cases/`` in the working directory; a path writes to that
                directory. Defaults to False.

        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self.debug = debug
        self._debug_dir: str | os.PathLike | None = (
            "debug_cases" if debug is True else debug or None
        )
        self.session: blpapi.Session | None = None
        self._open_services: set[str] = set()

    def start(self) -> "BQuery":
        """Connect to Bloomberg; ``with BQuery() as bq`` calls this for you.

        Use ``start()`` and ``stop()`` where a ``with`` block does not fit, for
        example when one connection is kept for a whole notebook. Services
        (reference data, BQL, search) are opened when a method first needs them.
        On an instance that is already started, ``start()`` does nothing; call
        ``stop()`` first to reconnect.

        Returns:
            BQuery: This instance, so that ``bq = BQuery().start()`` works.

        Raises:
            ConnectionError: If unable to establish connection to Bloomberg API.

        Example:
            ```python
            from polars_bloomberg import BQuery

            bq = BQuery().start()
            df = bq.bdp(['IBM US Equity'], ['NAME'])
            bq.stop()
            ```

        """
        if self.session is not None:
            return self  # already started: a second session would leak
        options = blpapi.SessionOptions()
        options.setServerHost(self.host)
        options.setServerPort(self.port)
        session = blpapi.Session(options)
        if not session.start():
            raise ConnectionError("Failed to start Bloomberg session.")
        self.session = session
        return self

    def stop(self) -> None:
        """Disconnect from Bloomberg; safe to call more than once.

        ``with BQuery() as bq`` calls this for you when the block ends, also when
        it ends with an error.

        Example:
            ```python
            from polars_bloomberg import BQuery

            bq = BQuery().start()
            try:
                df = bq.bdp(['IBM US Equity'], ['NAME'])
            finally:
                bq.stop()
            ```

        """
        if self.session is not None:
            self.session.stop()
            self.session = None
            self._open_services.clear()

    def __enter__(self) -> "BQuery":  # noqa: D105
        return self.start()

    def __exit__(  # noqa: D105
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.stop()

    def _create_request(self, service_name: str, operation: str) -> blpapi.Request:
        """Return an empty request, opening its service the first time it is needed."""
        if self.session is None:
            raise ConnectionError(
                "Bloomberg session is not started. "
                "Use 'with BQuery() as bq:' or call bq.start()."
            )
        if service_name not in self._open_services:
            if not self.session.openService(service_name):
                raise ConnectionError(f"Failed to open service {service_name}.")
            self._open_services.add(service_name)
        return self.session.getService(service_name).createRequest(operation)

    def _send_request(self, request: blpapi.Request) -> list[Any]:
        """Send a request and return the payloads (``msg.toPy()``) of its responses."""
        cid = self.session.sendRequest(request)
        responses: list[Any] = []
        events: list[dict] = []  # what the debug dump records
        try:
            finished = False
            while not finished:
                event = self.session.nextEvent(self.timeout)
                if event.eventType() == blpapi.Event.TIMEOUT:
                    raise TimeoutError(
                        f"Request timed out after {self.timeout} milliseconds"
                    )
                received = {
                    "type": _EVENT_NAMES.get(event.eventType(), "OTHER"),
                    "messages": [],
                }
                events.append(received)
                for msg in event:
                    if msg.messageType() == blpapi.Names.SESSION_TERMINATED:
                        raise ConnectionError("Bloomberg session terminated.")
                    if cid not in msg.correlationIds():
                        continue  # status message, or late answer to an old request
                    payload = msg.toPy()
                    received["messages"].append(
                        {"type": str(msg.messageType()), "payload": payload}
                    )
                    if msg.messageType() == blpapi.Names.REQUEST_FAILURE:
                        reason = payload.get("reason", {})
                        raise BloombergError(
                            f"Request failed: {reason.get('description')} "
                            f"({reason.get('category')})"
                        )
                    if isinstance(payload, dict) and "responseError" in payload:
                        error = payload["responseError"]
                        raise BloombergError(
                            f"Response error: {error.get('message')} "
                            f"({error.get('category')})"
                        )
                    responses.append(payload)
                    finished = event.eventType() == blpapi.Event.RESPONSE
        except BaseException:
            self._cancel_quietly(cid)
            raise
        finally:
            if self._debug_dir:
                _write_debug_dump(self._debug_dir, request, events)
        return responses

    def _cancel_quietly(self, cid: blpapi.CorrelationId) -> None:
        """Tell Bloomberg to drop a request; never hide the error being raised."""
        try:
            self.session.cancel(cid)
        except Exception:  # the session may already be gone
            logger.debug("Could not cancel request %s", cid)

    # --- bdp ---

    def bdp(
        self,
        securities: str | Sequence[str],
        fields: str | Sequence[str],
        overrides: Mapping[str, Any] | Sequence[tuple[str, Any]] | None = None,
        options: dict | None = None,
    ) -> pl.DataFrame:
        """Bloomberg Data Point, equivalent to Excel BDP() function.

        Fetch reference data for given securities and fields.

        Args:
            securities (str | Sequence[str]): One security identifier or several
                (e.g. 'AAPL US Equity').
            fields (str | Sequence[str]): One data field or several (e.g. 'PX_LAST').
                ``security`` cannot be a field name.
            overrides (dict | Sequence[tuple], optional): Field overrides as a dict
                (``{"REFERENCE_DATE": date(2020, 7, 15)}``) or as (name, value)
                pairs. A ``date`` value is sent as ``YYYYMMDD``; ``None`` is not
                allowed. Defaults to None.
            options (dict, optional): Additional request options. Defaults to None.

        Returns:
            pl.DataFrame: A Polars DataFrame containing the requested reference data,
                with a ``security`` column and one column per field. Rows may come
                back in a different order than ``securities``; Bloomberg answers
                large requests in batches. In an empty result, the field columns
                have dtype ``Null``.

            An invalid security gives a row of nulls and an invalid field a column
            of nulls; Bloomberg's reason is logged as one warning per call.

        Raises:
            BloombergError: If Bloomberg rejects or fails the request.
            ConnectionError: If the session is not started or is lost.
            TimeoutError: If Bloomberg sends nothing for ``timeout`` milliseconds.
            ValueError: If a field name is a key column name.
            TypeError: If the overrides are not a dict or (name, value) pairs, or
                an override has no value.

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

        """
        securities = _as_list(securities)
        fields = _as_list(fields)
        _check_no_field_is_named(fields, "security")

        request = self._create_request("//blp/refdata", "ReferenceDataRequest")
        _append_values(request, "securities", securities)
        _append_values(request, "fields", fields)
        _set_overrides(request, overrides)
        _set_options(request, options)

        payloads = self._send_request(request)
        rows = _parse_bdp(payloads, fields)
        schema = {"security": pl.String, **dict.fromkeys(fields)}
        return pl.DataFrame(rows, schema=schema, infer_schema_length=None)

    # --- bdh ---

    def bdh(
        self,
        securities: str | Sequence[str],
        fields: str | Sequence[str],
        start_date: date | str,
        end_date: date | str,
        overrides: Mapping[str, Any] | Sequence[tuple[str, Any]] | None = None,
        options: dict | None = None,
    ) -> pl.DataFrame:
        """Bloomberg Data History, equivalent to Excel BDH() function.

        Fetch historical data for given securities and fields between dates.

        Args:
            securities (str | Sequence[str]): One security identifier or several
                (e.g. 'AAPL US Equity').
            fields (str | Sequence[str]): One data field or several (e.g. 'PX_LAST').
                ``security`` and ``date`` cannot be field names.
            start_date (date | str): Start date for the historical data, as a
                ``date`` or a ``YYYYMMDD`` or ``YYYY-MM-DD`` string.
            end_date (date | str): End date for the historical data, same formats.
            overrides (dict | Sequence[tuple], optional): Field overrides as a dict
                (``{"REFERENCE_DATE": date(2020, 7, 15)}``) or as (name, value)
                pairs. A ``date`` value is sent as ``YYYYMMDD``; ``None`` is not
                allowed. Defaults to None.
            options (dict, optional): Additional request options. Defaults to None.

        Returns:
            pl.DataFrame: A Polars DataFrame containing the requested historical data,
                with ``security`` and ``date`` columns and one column per field. In
                an empty result, the field columns have dtype ``Null``.

            An invalid security gives no rows and an invalid field a column of
            nulls; Bloomberg's reason is logged as one warning per call.

        Raises:
            BloombergError: If Bloomberg rejects or fails the request.
            ConnectionError: If the session is not started or is lost.
            TimeoutError: If Bloomberg sends nothing for ``timeout`` milliseconds.
            ValueError: If a field name is a key column name.
            TypeError: If the overrides are not a dict or (name, value) pairs, or
                an override has no value.

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

        """
        securities = _as_list(securities)
        fields = _as_list(fields)
        _check_no_field_is_named(fields, "security", "date")

        request = self._create_request("//blp/refdata", "HistoricalDataRequest")
        _append_values(request, "securities", securities)
        _append_values(request, "fields", fields)
        _set_overrides(request, overrides)
        _set_options(request, options)
        request.set("startDate", _yyyymmdd(start_date))
        request.set("endDate", _yyyymmdd(end_date))

        payloads = self._send_request(request)
        rows = _parse_bdh(payloads, fields)
        schema = {"security": pl.String, "date": pl.Date, **dict.fromkeys(fields)}
        return pl.DataFrame(rows, schema=schema, infer_schema_length=None)

    # --- bdib ---

    def bdib(  # noqa: PLR0913
        self,
        security: str,
        event_type: str,
        interval: int,
        start_datetime: datetime | date | str,
        end_datetime: datetime | date | str,
        overrides: Mapping[str, Any] | Sequence[tuple[str, Any]] | None = None,
        options: dict | None = None,
    ) -> pl.DataFrame:
        """Fetch intraday bars from Bloomberg, mirroring Excel's BDIB() function.

        Args:
            security (str): Instrument identifier (for example 'AAPL US Equity').
            event_type (str): One of TRADE, BID, ASK, BEST_BID, BEST_ASK.
            interval (int): Bar length in minutes (1-1440).
            start_datetime (datetime | date | str): First bar timestamp; naive
                values are treated as UTC, tz-aware values are converted to UTC
                before the request is sent, a ``date`` means midnight, and an ISO
                8601 string (``"2024-01-02T08:00:00"``) is sent as it is.
            end_datetime (datetime | date | str): Last bar timestamp; handled the
                same way as start_datetime.
            overrides (dict | Sequence[tuple], optional): Field overrides as a dict
                (``{"REFERENCE_DATE": date(2020, 7, 15)}``) or as (name, value)
                pairs. A ``date`` value is sent as ``YYYYMMDD``; ``None`` is not
                allowed. Defaults to None.
            options (dict | None, optional): Additional Bloomberg request options.

        Returns:
            pl.DataFrame: Bars sorted by security/time with columns
                ['security', 'time', 'open', 'high', 'low', 'close', 'volume',
                'numEvents', 'value']. ``time`` is the start of each bar, as a naive
                datetime holding UTC. A window without bars gives an empty frame
                with these columns.

        Raises:
            BloombergError: If Bloomberg rejects or fails the request, for example
                for an unknown security or an interval outside 1-1440.
            ConnectionError: If the session is not started or is lost.
            TimeoutError: If Bloomberg sends nothing for ``timeout`` milliseconds.
            TypeError: If the overrides are not a dict or (name, value) pairs, or
                an override has no value.

        Example:
            ```python
            from datetime import datetime
            from polars_bloomberg import BQuery

            with BQuery() as bq:
                df = bq.bdib(
                    "OMX Index",
                    event_type="TRADE",
                    interval=60,
                    start_datetime=datetime(2026, 11, 13),
                    end_datetime=datetime(2026, 11, 13, 12),
                )
                print(df)
            ```

            Expected output:
            ```python
            shape: (4, 9)
            ┌───────────┬───────────────┬──────────┬──────────┬───┬──────────┬────────┬───────────┬───────┐
            │ security  ┆ time          ┆ open     ┆ high     ┆ … ┆ close    ┆ volume ┆ numEvents ┆ value │
            │ ---       ┆ ---           ┆ ---      ┆ ---      ┆   ┆ ---      ┆ ---    ┆ ---       ┆ ---   │
            │ str       ┆ datetime[μs]  ┆ f64      ┆ f64      ┆   ┆ f64      ┆ i64    ┆ i64       ┆ f64   │
            ╞═══════════╪═══════════════╪══════════╪══════════╪═══╪══════════╪════════╪═══════════╪═══════╡
            │ OMX Index ┆ 2026-11-13    ┆ 3351.464 ┆ 3353.647 ┆ … ┆ 3350.421 ┆ 0      ┆ 3591      ┆ 0.0   │
            │           ┆ 08:00:00      ┆          ┆          ┆   ┆          ┆        ┆           ┆       │
            │ OMX Index ┆ 2026-11-13    ┆ 3350.322 ┆ 3359.014 ┆ … ┆ 3358.95  ┆ 0      ┆ 3600      ┆ 0.0   │
            │           ┆ 09:00:00      ┆          ┆          ┆   ┆          ┆        ┆           ┆       │
            │ OMX Index ┆ 2026-11-13    ┆ 3358.95  ┆ 3366.616 ┆ … ┆ 3365.006 ┆ 0      ┆ 3600      ┆ 0.0   │
            │           ┆ 10:00:00      ┆          ┆          ┆   ┆          ┆        ┆           ┆       │
            │ OMX Index ┆ 2026-11-13    ┆ 3365.006 ┆ 3365.888 ┆ … ┆ 3362.596 ┆ 0      ┆ 3600      ┆ 0.0   │
            │           ┆ 11:00:00      ┆          ┆          ┆   ┆          ┆        ┆           ┆       │
            └───────────┴───────────────┴──────────┴──────────┴───┴──────────┴────────┴───────────┴───────┘
            ```

        """  # noqa: E501
        request = self._create_request("//blp/refdata", "IntradayBarRequest")
        request.set("security", security)
        request.set("eventType", event_type)
        request.set("interval", interval)
        request.set("startDateTime", _iso_datetime(start_datetime))
        request.set("endDateTime", _iso_datetime(end_datetime))
        _set_overrides(request, overrides)
        _set_options(request, options)

        payloads = self._send_request(request)
        rows = _parse_bdib(payloads, requested_security=security)
        df = pl.DataFrame(
            rows, schema=_BAR_SCHEMA, strict=False, infer_schema_length=None
        )
        return df.sort(["security", "time"]) if not df.is_empty() else df

    # --- bql ---

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
                - ``errors``: the messages BQL returned with the answer
                Returns an empty result if the query fails, for example on invalid
                BQL syntax; the message is logged and kept in ``errors``. A screen
                that matches nothing is also empty; its message ("No results
                match ...") is logged as a warning. Results that BQL returns
                together with an error are kept.

        Raises:
            BloombergError: If Bloomberg rejects or fails the request, or an item
                of the answer cannot be turned into a DataFrame (for example a
                date that is not ISO text); the message names the item.
            ConnectionError: If the session is not started or is lost.
            TimeoutError: If Bloomberg sends nothing for ``timeout`` milliseconds.

        Example:
            Simple query to fetch last price:

            ```python
            from polars_bloomberg import BQuery

            with BQuery() as bq:
                # Get last price for multiple securities
                result = bq.bql("get(px_last) for(['IBM US Equity', 'MSFT US Equity'])")
            df = result.combine(on="ID")
            print(df)
            ```

            Expected output:
            ```python
            shape: (2, 4)
            ┌────────────────┬─────────┬────────────┬──────────┐
            │ ID             ┆ px_last ┆ DATE       ┆ CURRENCY │
            │ ---            ┆ ---     ┆ ---        ┆ ---      │
            │ str            ┆ f64     ┆ date       ┆ str      │
            ╞════════════════╪═════════╪════════════╪══════════╡
            │ IBM US Equity  ┆ 247.13  ┆ 2026-11-16 ┆ USD      │
            │ MSFT US Equity ┆ 523.88  ┆ 2026-11-16 ┆ USD      │
            └────────────────┴─────────┴────────────┴──────────┘
            ```

            Access individual DataFrames; with one item, ``result[0]`` is the same
            frame:
            ```python
            >>> df_px_last = result[0]
            >>> print(df_px_last.shape)
            (2, 4)
            ```
            Fetch multiple fields and combine results:
            ```python
            >>> result = bq.bql("get(px_last, px_volume) for('AAPL US Equity')")
            >>> df_combined = result.combine(on="ID")
            >>> print(df_combined)
            shape: (1, 6)
            ┌────────────────┬─────────┬────────────┬──────────┬───────────┬────────────────┐
            │ ID             ┆ px_last ┆ DATE       ┆ CURRENCY ┆ px_volume ┆ DATE_px_volume │
            │ ---            ┆ ---     ┆ ---        ┆ ---      ┆ ---       ┆ ---            │
            │ str            ┆ f64     ┆ date       ┆ str      ┆ f64       ┆ date           │
            ╞════════════════╪═════════╪════════════╪══════════╪═══════════╪════════════════╡
            │ AAPL US Equity ┆ 356.27  ┆ 2026-11-16 ┆ USD      ┆ 112441.0  ┆ 2026-11-16     │
            └────────────────┴─────────┴────────────┴──────────┴───────────┴────────────────┘
            ```
            Iterate over individual DataFrames:
            ```python
            >>> for df in result:
            ...     print(df)
            ```

        """  # noqa: E501
        request = self._create_request("//blp/bqlsvc", "sendQuery")
        request.set("expression", expression)
        _set_bql_client_context(request)

        payloads = self._send_request(request)
        items, errors = _parse_bql(payloads)
        dataframes = [_bql_dataframe(item) for item in items]
        return BqlResult(dataframes, [item.name for item in items], errors)

    # --- bsrch ---

    def bsrch(
        self,
        domain: str,
        overrides: Mapping[str, Any] | Sequence[tuple[str, Any]] | None = None,
        options: dict | None = None,
    ) -> pl.DataFrame:
        r"""Bloomberg SRCH (search) via ExcelGetGridRequest on //blp/exrsvc.

        Args:
            domain: Domain string, e.g. ``\"FI:SRCHEX.@COCO\"``.
            overrides: Optional overrides as a dict (e.g. ``{\"LIMIT\": 20000}``)
                or as (name, value) pairs.
            options: Additional request options applied directly to the request.

        Returns:
            pl.DataFrame with one row per search record and columns from the grid.
                Column types follow what Bloomberg sends (``StringValue`` or
                ``DoubleValue``): a number sent as text stays a string, and a blank
                cell in a numeric column becomes null. A search without results
                keeps its columns. A repeated column title gets a suffix (``Name``,
                ``Name_1``) and a warning is logged.

        Raises:
            BloombergError: When Bloomberg rejects the request, or the search
                returns an error and no rows (a subclass of ValueError). An error
                that comes with rows is logged instead.
            ConnectionError: If the session is not started or is lost.
            TimeoutError: If Bloomberg sends nothing for ``timeout`` milliseconds.

        Example:
            Fetch Contingent Convertible bonds based on Example Search @COCO.
            For the sake of the example, limit the number of bonds to two; with
            ``LIMIT`` given, the "reached internal limit" warning is not logged.

            ```python
            from polars_bloomberg import BQuery

            with BQuery() as bq:
                df = bq.bsrch("FI:SRCHEX.@COCO", {"LIMIT": 2})
                print(df)
            ```

            Expected output:
            ```python
            shape: (2, 1)
            ┌───────────────┐
            │ id            │
            │ ---           │
            │ str           │
            ╞═══════════════╡
            │ DS990344 Corp │
            │ DT301785 Corp │
            └───────────────┘
            ```

        """
        overrides = _override_pairs(overrides)

        request = self._create_request("//blp/exrsvc", "ExcelGetGridRequest")
        request.set("Domain", domain)
        _set_bsrch_overrides(request, overrides)
        _set_options(request, options)

        payloads = self._send_request(request)
        rows, columns = _parse_bsrch(payloads, limit_given="LIMIT" in dict(overrides))
        return pl.DataFrame(
            rows, schema=columns or None, infer_schema_length=None, strict=False
        )


# -------------------------------------------------------------------------------------
# bdp and bdh: reference data and history
# -------------------------------------------------------------------------------------


def _parse_bdp(payloads: list[dict], fields: list[str]) -> list[dict]:
    """Return one row per security: its name and the value of each requested field.

    A ReferenceDataResponse payload lists several securities:
    ``{"securityData": [{"security": ..., "fieldData": {FIELD: value}}, ...]}``.
    """
    rows = []
    errors = []
    for payload in payloads:
        for security_data in payload.get("securityData", []):
            errors += _security_errors(security_data)
            # fieldData is keyed by the field names as the request spelled them
            # (recording bdp/lowercase_field), and has no key for a field without
            # a value.
            field_data = security_data.get("fieldData", {})
            row = {"security": security_data.get("security")}
            row |= {field: field_data.get(field) for field in fields}
            rows.append(row)
    _warn_security_errors(errors)
    return rows


def _parse_bdh(payloads: list[dict], fields: list[str]) -> list[dict]:
    """Return one row per security and date, with the value of each requested field.

    A HistoricalDataResponse payload holds one security and a list of its days:
    ``{"securityData": {"security": ..., "fieldData": [{"date": ..., FIELD: value}]}}``.
    """
    rows = []
    errors = []
    for payload in payloads:
        security_data = payload.get("securityData", {})
        errors += _security_errors(security_data)
        security = security_data.get("security")
        for day in security_data.get("fieldData", []):
            row = {"security": security, "date": day.get("date")}
            row |= {field: day.get(field) for field in fields}
            rows.append(row)
    _warn_security_errors(errors)
    return rows


def _security_errors(security_data: dict) -> list[str]:
    """Describe the errors Bloomberg reported inside one securityData element.

    An unknown security or field does not fail the request. Bloomberg answers as
    usual and adds a ``securityError`` for the security, or an entry in
    ``fieldExceptions`` for the field (recordings bdp/invalid_security_mixed and
    bdp/invalid_field_mixed; bdh has the same two).
    """
    security = security_data.get("security")
    lines = []
    if error := security_data.get("securityError"):
        lines.append(f"{security}: {error.get('message')} ({error.get('category')})")
    for exception in security_data.get("fieldExceptions") or []:
        info = exception.get("errorInfo") or {}
        lines.append(
            f"{security} / {exception.get('fieldId')}: "
            f"{info.get('message')} ({info.get('category')})"
        )
    return lines


_MAX_LOGGED_ERRORS = 10


def _warn_security_errors(lines: list[str]) -> None:
    """Log the errors of one call as a single warning, at most ten lines of them."""
    if not lines:
        return
    shown = lines[:_MAX_LOGGED_ERRORS]
    if len(lines) > _MAX_LOGGED_ERRORS:
        shown.append(f"... and {len(lines) - _MAX_LOGGED_ERRORS} more")
    logger.warning(
        "Bloomberg reported %d error(s):\n  %s", len(lines), "\n  ".join(shown)
    )


# -------------------------------------------------------------------------------------
# bdib: intraday bars
# -------------------------------------------------------------------------------------

# The columns of a bdib frame. All but "security" are the keys of one bar in
# Bloomberg's barTickData, under Bloomberg's names.
_BAR_SCHEMA = {
    "security": pl.String,
    "time": pl.Datetime,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Int64,
    "numEvents": pl.Int64,
    "value": pl.Float64,
}


def _parse_bdib(payloads: list[dict], requested_security: str) -> list[dict]:
    """Return one row per bar, under the requested security.

    An IntradayBarResponse payload is ``{"barData": {"barTickData": [bar, ...]}}``.
    barData names no security (see recording bdib/trade_bars_60min), so each row
    gets the security that was asked for.
    """
    rows = []
    for payload in payloads:
        bar_data = payload.get("barData", {})
        security = bar_data.get("security") or requested_security
        for bar in bar_data.get("barTickData", []):
            row = {"security": security}
            row |= {key: bar.get(key) for key in _BAR_SCHEMA if key != "security"}
            rows.append(row)
    return rows


# -------------------------------------------------------------------------------------
# bql: Bloomberg Query Language
# -------------------------------------------------------------------------------------


@dataclass
class _BqlItem:
    """One item of a BQL answer, laid out as the columns of its DataFrame.

    ``get(name, px_last)`` asks for two items, and each becomes one DataFrame.
    """

    name: str
    columns: dict[str, list[Any]]  # column name -> values
    schema: dict[str, pl.DataType | None]  # column name -> dtype; None: Polars infers


def _parse_bql(payloads: list[Any]) -> tuple[list[_BqlItem], list[str]]:
    """Return the items of a BQL answer and the error messages BQL sent with it.

    The answer is JSON (see recording bql/two_items):
    ``{"results": {ITEM: {"idColumn": ..., "valuesColumn": ...,
    "secondaryColumns": [...]}}, "responseExceptions": [...]}``.
    A query that fails has null results and its reason in responseExceptions
    (recording bql/syntax_error). The messages are logged here and returned, not
    raised: bql() hands them to the user as ``BqlResult.errors``.
    """
    answers, errors = _load_bql_answers(payloads)

    exceptions = [
        exception
        for answer in answers
        for exception in answer.get("responseExceptions") or []
        if isinstance(exception, dict)
    ]
    # A screen that matches nothing is not a failure. Bloomberg reports it as an
    # exception of type BENIGN, "No results match ..." (recording
    # bql/empty_universe), so it is logged as a warning and not as an error.
    only_benign = not errors and all(e.get("type") == "BENIGN" for e in exceptions)
    errors += [
        exception.get("message") or exception.get("internalMessage") or "Unknown error"
        for exception in exceptions
    ]
    if errors and only_benign:
        logger.warning("BQL returned no results: %s", "; ".join(errors))
    elif errors:
        logger.error("BQL error: %s", "; ".join(errors))

    # Results that arrive together with exceptions are kept.
    items = [
        _bql_item(name, content)
        for answer in answers
        for name, content in (answer.get("results") or {}).items()
        if content is not None
    ]
    for item in items:
        _read_text_values(item)
    return items, errors


def _load_bql_answers(payloads: list[Any]) -> tuple[list[dict], list[str]]:
    """Return the BQL answer as parsed JSON, and an error message if it is not JSON.

    BQL is the one service that answers with JSON text instead of a structure, and
    Bloomberg cuts a long text into several messages. Five years of daily prices
    for the members of SPX Index arrived as 12 messages of 4 MiB each, none of
    them valid JSON on its own. So the text payloads are joined before they are
    parsed. A payload that is not text is taken as an answer already parsed.
    """
    answers = [payload for payload in payloads if not isinstance(payload, str)]
    errors = []
    text = "".join(payload for payload in payloads if isinstance(payload, str))
    if text:
        try:
            answers.append(json.loads(text))
        except json.JSONDecodeError as error:
            errors.append(f"Failed to decode JSON: {error}")
    return answers, errors


def _bql_item(name: str, content: dict) -> _BqlItem:
    """Lay out one entry of "results" as named columns with Polars dtypes.

    BQL sends an item as an ID column, a values column and any number of secondary
    columns such as DATE and CURRENCY, each ``{"type": ..., "values": [...]}``. The
    values column takes the name of the item.
    """
    bql_columns = [
        ("ID", content.get("idColumn") or {}),
        (name, content.get("valuesColumn") or {}),
        *(
            (column.get("name", ""), column)
            for column in content.get("secondaryColumns") or []
        ),
    ]
    columns: dict[str, list[Any]] = {}
    bql_types: dict[str, str] = {}
    for column_name, column in bql_columns:
        # An item can be named like another of its columns: get(ID) gives two
        # columns named ID (recording bql/item_named_id). The later one is renamed.
        unique = _unique_name(column_name, taken=columns)
        if unique != column_name:
            logger.warning(
                "BQL item %r has two columns named %r; the second one is renamed %r.",
                name,
                column_name,
                unique,
            )
        columns[unique] = column.get("values") or []
        bql_types[unique] = column.get("type") or "STRING"
    schema = {
        column: _polars_dtype(bql_type, column) for column, bql_type in bql_types.items()
    }
    return _BqlItem(name, columns, schema)


_BQL_DTYPES = {
    "STRING": pl.String,
    "ENUM": pl.String,  # e.g. CPN_TYP of cpn(), the rating of bb_composite()
    "DOUBLE": pl.Float64,
    "INT": pl.Int64,
    "DATE": pl.Date,
    "DATETIME": pl.Datetime,  # e.g. the quote times of axes()
    "BOOLEAN": pl.Boolean,
}


def _polars_dtype(bql_type: str, column: str) -> pl.DataType | None:
    """Return the Polars dtype for a BQL type; None, meaning infer, if it is unknown."""
    dtype = _BQL_DTYPES.get(bql_type.upper())
    if dtype is None:
        logger.warning(
            "Unknown BQL type %r for column %r; Polars infers its dtype.",
            bql_type,
            column,
        )
    return dtype


# JSON has no dates, NaN or infinity, so BQL sends them as text. The three functions
# below read that text; _read_text_values applies them by dtype.


def _bql_number(value: Any) -> Any:
    """Turn BQL's text for NaN and infinity into None and float infinities.

    Recording bql/nan_and_infinity has all three: "NaN", "Infinity", "-Infinity".
    """
    if isinstance(value, str):
        text = value.lower()
        if text == "nan":
            return None
        if text in {"infinity", "inf"}:
            return float("inf")
        if text in {"-infinity", "-inf"}:
            return float("-inf")
    return value


def _bql_date(value: Any) -> date | None:
    """Turn BQL's ISO text (2024-01-02T00:00:00Z) into a date."""
    return datetime.fromisoformat(value).date() if isinstance(value, str) else None


def _bql_datetime(value: Any) -> datetime | None:
    """Turn BQL's ISO text into a naive datetime holding UTC, like bdib's times."""
    if not isinstance(value, str):
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(UTC).replace(tzinfo=None)
    return parsed


_BQL_TEXT_READERS = {
    pl.Date: _bql_date,
    pl.Datetime: _bql_datetime,
    pl.Float64: _bql_number,
    pl.Int64: _bql_number,
}


def _read_text_values(item: _BqlItem) -> None:
    """Replace the text BQL sends for dates, NaN and infinity by values, in place."""
    for column, dtype in item.schema.items():
        read = _BQL_TEXT_READERS.get(dtype)
        if read is None:
            continue
        try:
            item.columns[column] = [read(value) for value in item.columns[column]]
        except ValueError as error:  # a date that is not ISO text
            raise BloombergError(
                f"BQL item {item.name!r}, column {column!r}: {error}"
            ) from error


def _bql_dataframe(item: _BqlItem) -> pl.DataFrame:
    """Build the DataFrame of one item; a failure names the item."""
    try:
        return pl.DataFrame(item.columns, schema=item.schema, strict=True)
    except (pl.exceptions.PolarsError, TypeError, ValueError) as error:
        raise BloombergError(
            f"Cannot build a DataFrame for BQL item {item.name!r}: {error}"
        ) from error


# -------------------------------------------------------------------------------------
# bsrch: search grids
# -------------------------------------------------------------------------------------


def _parse_bsrch(
    payloads: list[Any], *, limit_given: bool
) -> tuple[list[dict], list[str]]:
    """Return the rows of a search and the names of its columns.

    Each payload is one grid (see recording bsrch/coco_limit_2):
    ``{"ColumnTitles": [...], "DataRecords": [{"DataFields": [cell, ...]}, ...],
    "ReachMax": bool}``, plus ``"Error": text`` when the search fails (recording
    bsrch/invalid_domain). The column names are the titles, and a col_<n> invented
    for each cell that has no title.
    """
    rows: list[dict[str, Any]] = []
    titles: list[str] = []
    errors: list[str] = []
    reached_max = False

    for grid in payloads:
        if not isinstance(grid, dict) or "DataRecords" not in grid:
            continue  # not a grid
        titles = _unique_titles(grid.get("ColumnTitles") or []) or titles
        reached_max = reached_max or bool(grid.get("ReachMax"))
        if grid.get("Error"):
            errors.append(grid["Error"])
        for record in grid["DataRecords"] or []:
            values = [_cell_value(cell) for cell in record.get("DataFields") or []]
            names = titles + [f"col_{i}" for i in range(len(titles), len(values))]
            rows.append(dict(zip(names, values, strict=False)))

    columns = list(dict.fromkeys([*titles, *(name for row in rows for name in row)]))
    unknown_keys = {
        key
        for row in rows
        for value in row.values()
        if isinstance(value, dict)
        for key in value
    }
    if errors and not rows:
        raise BloombergError(f"BSRCH error: {errors[0]}")
    if errors:
        logger.warning("BSRCH returned rows and an error: %s", "; ".join(errors))
    if unknown_keys:
        logger.warning(
            "BSRCH values with unknown keys %s are kept as they are.",
            sorted(unknown_keys),
        )
    # ReachMax is true also when a LIMIT override cut the search short (recording
    # bsrch/coco_limit_2), so it is worth a warning only when no LIMIT was given.
    if reached_max and not limit_given:
        logger.warning(
            "BSRCH response reached internal limit; consider using LIMIT override."
        )

    _blank_strings_to_none(rows, columns)
    return rows, columns


def _unique_titles(titles: list[str]) -> list[str]:
    """Give each repeat of an earlier title a suffix (Name, Name_1, ...) and log it."""
    unique: list[str] = []
    for title in titles:
        name = _unique_name(title, taken=unique)
        if name != title:
            logger.warning("BSRCH column %r repeated; renamed %r.", title, name)
        unique.append(name)
    return unique


def _cell_value(cell: Any) -> Any:
    """Return the value of one DataFields entry; return other entries unchanged.

    Bloomberg sends each cell as {"StringValue": ...} or {"DoubleValue": ...}.
    """
    if isinstance(cell, dict):
        for key in ("StringValue", "DoubleValue"):
            if key in cell:
                return cell[key]
    return cell


def _blank_strings_to_none(rows: list[dict[str, Any]], columns: list[str]) -> None:
    """In columns that hold only numbers and blanks, make the blanks None, in place.

    Bloomberg sends an empty numeric cell as a blank StringValue; left as is, it
    would make Polars read the whole column as strings. Columns with other text
    are not touched: no string is ever parsed into a number here.
    """

    def is_number_or_blank(value: Any) -> bool:
        if isinstance(value, str):
            return value.strip() == ""
        return value is None or isinstance(value, (int, float))

    for column in columns:
        if all(is_number_or_blank(row.get(column)) for row in rows):
            for row in rows:
                if isinstance(row.get(column), str):
                    row[column] = None


# -------------------------------------------------------------------------------------
# Shared by the parsers
# -------------------------------------------------------------------------------------


def _unique_name(name: str, taken: Collection[str]) -> str:
    """Return the name, or the first of name_1, name_2, ... that is not taken."""
    candidate, n = name, 0
    while candidate in taken:
        n += 1
        candidate = f"{name}_{n}"
    return candidate
