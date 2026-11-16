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

import dataclasses
import json
import logging
import os
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from types import TracebackType
from typing import Any

import blpapi
import polars as pl

# Configure logging
# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class BloombergError(ValueError):
    """Bloomberg rejected or failed a request."""


@dataclass
class SITable:
    """Holds data and schema for a Single Item response Table."""

    name: str  # data item name
    data: dict[str, list[Any]]  # column_name -> list of values
    schema: dict[str, pl.DataType | None]  # column_name -> Polars datatype or infer


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

        result = self.dataframes[0]  # Initialize with the first DataFrame
        for df in self.dataframes[1:]:
            common_cols = set(result.columns) & set(df.columns)
            if not common_cols:
                raise ValueError("No common columns found to join on.")
            result = result.join(df, on=list(common_cols), how="full", coalesce=True)
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


# --- errors Bloomberg reports inside a //blp/refdata answer ---

_MAX_LOGGED_ERRORS = 10


def _item_errors(security_data: dict) -> list[str]:
    """Describe the errors Bloomberg reported for one securityData element."""
    name = security_data.get("security")
    lines = []
    if error := security_data.get("securityError"):
        lines.append(f"{name}: {error.get('message')} ({error.get('category')})")
    for exception in security_data.get("fieldExceptions") or []:
        info = exception.get("errorInfo") or {}
        lines.append(
            f"{name} / {exception.get('fieldId')}: "
            f"{info.get('message')} ({info.get('category')})"
        )
    return lines


def _warn_item_errors(lines: list[str]) -> None:
    """Log the errors of one call as a single warning, at most ten lines of them."""
    if not lines:
        return
    shown = lines[:_MAX_LOGGED_ERRORS]
    if len(lines) > _MAX_LOGGED_ERRORS:
        shown.append(f"... and {len(lines) - _MAX_LOGGED_ERRORS} more")
    logger.warning(
        "Bloomberg reported %d error(s):\n  %s", len(lines), "\n  ".join(shown)
    )


# --- inputs ---


def _as_list(value: str | Sequence[str]) -> list[str]:
    """Accept one name or several."""
    return [value] if isinstance(value, str) else list(value)


def _override_items(overrides: Mapping[str, Any] | Sequence | None) -> list[tuple]:
    """Return overrides as (name, value) pairs; accept a dict or a sequence of pairs."""
    if not overrides:
        return []
    pairs = overrides.items() if isinstance(overrides, Mapping) else overrides
    items = []
    for pair in pairs:
        if isinstance(pair, str) or len(pair) != 2:  # noqa: PLR2004
            raise TypeError(
                "overrides must be a dict or a sequence of (name, value) pairs"
            )
        name, value = pair
        if value is None:
            raise TypeError(f"override {name!r} has no value")
        if isinstance(value, date):  # also covers datetime
            value = value.strftime("%Y%m%d")
        items.append((name, value))
    return items


def _format_date(value: date | str) -> str:
    """Return a date as Bloomberg's YYYYMMDD; accept YYYYMMDD and ISO strings."""
    if isinstance(value, str):
        return value.replace("-", "")
    return value.strftime("%Y%m%d")


def _check_key_columns(fields: list[str], keys: tuple[str, ...]) -> None:
    """Refuse a field that would overwrite one of the key columns of the result."""
    if clash := [key for key in keys if key in fields]:
        names = " and ".join(repr(key) for key in clash)
        raise ValueError(f"{names} cannot be requested as fields")


# --- bsrch column titles ---


def _unique_titles(titles: list[str]) -> list[str]:
    """Give each repeat of an earlier title a suffix (Name, Name_1, ...) and log it."""
    unique: list[str] = []
    for title in titles:
        candidate, n = title, 0
        while candidate in unique:
            n += 1
            candidate = f"{title}_{n}"
        if candidate != title:
            logger.warning("BSRCH column %r repeated; renamed %r.", title, candidate)
        unique.append(candidate)
    return unique


# --- BQL values ---


def _convert_number(value: Any) -> Any:
    """Turn BQL's text for NaN and infinity into None and float infinities."""
    if isinstance(value, str):
        lower_val = value.lower()
        if lower_val == "nan":
            return None
        if lower_val in {"infinity", "inf"}:
            return float("inf")
        if lower_val in {"-infinity", "-inf"}:
            return float("-inf")
    return value


def _convert_datetime(value: Any) -> datetime | None:
    """Turn BQL's ISO text into a naive datetime holding UTC, like bdib's times."""
    if not isinstance(value, str):
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(UTC).replace(tzinfo=None)
    return parsed


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

    def _get_service(self, name: str) -> blpapi.Service:
        """Return a service, opening it the first time it is needed."""
        if self.session is None:
            raise ConnectionError(
                "Bloomberg session is not started. "
                "Use 'with BQuery() as bq:' or call bq.start()."
            )
        if name not in self._open_services:
            if not self.session.openService(name):
                raise ConnectionError(f"Failed to open service {name}.")
            self._open_services.add(name)
        return self.session.getService(name)

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
        _check_key_columns(fields, ("security",))
        request = self._create_request(
            "ReferenceDataRequest", securities, fields, overrides, options
        )
        responses = self._send_request(request)
        data = self._parse_bdp_responses(responses, fields)
        schema = {"security": pl.String, **dict.fromkeys(fields)}
        return pl.DataFrame(data, schema=schema, infer_schema_length=None)

    def _create_request(
        self,
        request_type: str,
        securities: list[str],
        fields: list[str],
        overrides: Mapping[str, Any] | Sequence[tuple[str, Any]] | None = None,
        options: dict | None = None,
    ) -> blpapi.Request:
        """Create a Bloomberg request with support for overrides and options."""
        service = self._get_service("//blp/refdata")
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
            for field_id, value in _override_items(overrides):
                override_element = overrides_element.appendElement()
                override_element.setElement("fieldId", field_id)
                override_element.setElement("value", value)

        # Add additional options if provided
        if options:
            for key, value in options.items():
                request.set(key, value)

        return request

    def _parse_bdp_responses(
        self, responses: list[dict], fields: list[str]
    ) -> list[dict]:
        data = []
        errors = []
        for response in responses:
            security_data = response.get("securityData", [])
            for sec in security_data:
                errors.extend(_item_errors(sec))
                security = sec.get("security")
                field_data = sec.get("fieldData", {})
                record = {"security": security}
                for field in fields:
                    record[field] = field_data.get(field)
                data.append(record)
        _warn_item_errors(errors)
        return data

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
        _check_key_columns(fields, ("security", "date"))
        request = self._create_request(
            "HistoricalDataRequest", securities, fields, overrides, options
        )
        request.set("startDate", _format_date(start_date))
        request.set("endDate", _format_date(end_date))
        responses = self._send_request(request)
        data = self._parse_bdh_responses(responses, fields)
        schema = {
            "security": pl.String,
            "date": pl.Date,
            **dict.fromkeys(fields),
        }
        return pl.DataFrame(data, schema=schema, infer_schema_length=None)

    def _parse_bdh_responses(
        self, responses: list[dict], fields: list[str]
    ) -> list[dict]:
        data = []
        errors = []
        for response in responses:
            security_data = response.get("securityData", {})
            errors.extend(_item_errors(security_data))
            security = security_data.get("security")
            field_data_array = security_data.get("fieldData", [])
            for entry in field_data_array:
                record = {"security": security, "date": entry.get("date")}
                for field in fields:
                    record[field] = entry.get(field)
                data.append(record)
        _warn_item_errors(errors)
        return data

    # --- bdib ---

    def bdib(  # noqa: PLR0913
        self,
        security: str,
        event_type: str,
        interval: int,
        start_datetime: datetime | date,
        end_datetime: datetime | date,
        overrides: Mapping[str, Any] | Sequence[tuple[str, Any]] | None = None,
        options: dict | None = None,
    ) -> pl.DataFrame:
        """Fetch intraday bars from Bloomberg, mirroring Excel's BDIB() function.

        Args:
            security (str): Instrument identifier (for example 'AAPL US Equity').
            event_type (str): One of TRADE, BID, ASK, BEST_BID, BEST_ASK.
            interval (int): Bar length in minutes (1-1440).
            start_datetime (datetime | date): First bar timestamp; naive values are
                treated as UTC, tz-aware values are converted to UTC before the
                request is sent, and a ``date`` means midnight.
            end_datetime (datetime | date): Last bar timestamp; handled same way as
                start_datetime.
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

    def _create_intraday_bar_request(  # noqa: PLR0913
        self,
        security: str,
        event_type: str,
        interval: int,
        start_datetime: datetime | date,
        end_datetime: datetime | date,
        overrides: Mapping[str, Any] | Sequence[tuple[str, Any]] | None,
        options: dict | None,
    ) -> blpapi.Request:
        """Create an IntradayBarRequest with overrides and options support."""
        service = self._get_service("//blp/refdata")
        request = service.createRequest("IntradayBarRequest")
        request.set("security", security)
        request.set("eventType", event_type)
        request.set("interval", interval)
        request.set("startDateTime", self._format_datetime(start_datetime))
        request.set("endDateTime", self._format_datetime(end_datetime))

        if overrides:
            overrides_element = request.getElement("overrides")
            for field_id, value in _override_items(overrides):
                override_element = overrides_element.appendElement()
                override_element.setElement("fieldId", field_id)
                override_element.setElement("value", value)

        if options:
            for key, value in options.items():
                request.set(key, value)

        return request

    @staticmethod
    def _format_datetime(value: datetime | date | str) -> str:
        """Convert datetime objects to Bloomberg's ISO8601 string format."""
        if isinstance(value, str):
            return value
        if not isinstance(value, datetime):  # a plain date means midnight
            value = datetime.combine(value, time())
        if value.tzinfo is None:
            return value.strftime("%Y-%m-%dT%H:%M:%S")
        return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _parse_bdib_responses(
        self, responses: list[dict], fallback_security: str | None = None
    ) -> list[dict]:
        bars: list[dict[str, Any]] = []
        for response in responses:
            bar_data = response.get("barData", {})
            security = bar_data.get("security") or fallback_security
            entries = bar_data.get("barTickData", [])
            for entry in entries:
                record = {
                    "security": security,
                    "time": entry.get("time"),
                    "open": entry.get("open"),
                    "high": entry.get("high"),
                    "low": entry.get("low"),
                    "close": entry.get("close"),
                    "volume": entry.get("volume"),
                    "numEvents": entry.get("numEvents"),
                    "value": entry.get("value"),
                }
                bars.append(record)
        return bars

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
                BQL syntax; the message is logged and kept in ``errors``. Results
                that BQL returns together with an error are kept.

        Raises:
            BloombergError: If Bloomberg rejects or fails the request, or an item
                of the answer cannot be turned into a DataFrame; the message names
                the item.
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
        request = self._create_bql_request(expression)
        responses = self._send_request(request)
        tables, errors = self._parse_bql_responses(responses)
        dataframes = []
        for table in tables:
            try:
                df = pl.DataFrame(table.data, schema=table.schema, strict=True)
            except (pl.exceptions.PolarsError, TypeError, ValueError) as error:
                raise BloombergError(
                    f"Cannot build a DataFrame for BQL item {table.name!r}: {error}"
                ) from error
            dataframes.append(df)
        names = [table.name for table in tables]
        return BqlResult(dataframes, names, errors)

    def _create_bql_request(self, expression: str) -> blpapi.Request:
        """Create a BQL request."""
        service = self._get_service("//blp/bqlsvc")
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

    def _parse_bql_responses(
        self, responses: list[Any]
    ) -> tuple[list[SITable], list[str]]:
        """Parse BQL responses into SITable objects and the BQL error messages."""
        tables: list[SITable] = []
        results, errors = self._extract_results(responses)

        for result in results:
            tables.extend(self._parse_result(result))
        return [self._apply_schema(table) for table in tables], errors

    def _extract_results(self, responses: list[Any]) -> tuple[list[dict], list[str]]:
        """Return the 'results' sections of the responses and the BQL error messages.

        Results that arrive together with responseExceptions are kept. The error
        messages (from responseExceptions, or a payload that is not valid JSON) are
        logged as one error.

        """
        extracted = []
        errors = []
        # Bloomberg splits a large BQL answer over several messages, each a fragment
        # of one JSON text (live check L7), so the strings are joined before parsing.
        payloads = [response for response in responses if not isinstance(response, str)]
        text = "".join(response for response in responses if isinstance(response, str))
        if text:
            try:
                payloads.append(json.loads(text))
            except json.JSONDecodeError as e:
                errors.append(f"Failed to decode JSON: {e}")

        for resp_dict in payloads:
            # BQL errors (e.g. syntax errors) are in responseExceptions
            errors.extend(
                exc.get("message") or exc.get("internalMessage") or "Unknown error"
                for exc in resp_dict.get("responseExceptions") or []
                if isinstance(exc, dict)
            )

            results = resp_dict.get("results")
            if results:
                extracted.append(results)
        if errors:
            logger.error("BQL error: %s", "; ".join(errors))
        return extracted, errors

    def _parse_result(self, results: dict[str, Any]) -> list[SITable]:
        """Convert a single BQL results dictionary into a list[SITable]."""
        tables: list[SITable] = []
        for field, content in results.items():
            if content is None:
                continue
            columns = [
                ("ID", content.get("idColumn") or {}),
                (field, content.get("valuesColumn") or {}),
            ]
            columns += [
                (sec_col.get("name", ""), sec_col)
                for sec_col in content.get("secondaryColumns") or []
            ]
            data = {}
            schema_str = {}
            for name, column in columns:
                unique, n = name, 0
                while unique in data:  # e.g. an item named ID, or like a column
                    n += 1
                    unique = f"{name}_{n}"
                if unique != name:
                    logger.warning(
                        "BQL item %r has two columns named %r; the second one is "
                        "renamed %r.",
                        field,
                        name,
                        unique,
                    )
                data[unique] = column.get("values") or []
                schema_str[unique] = column.get("type") or "STRING"
            schema = self._map_types(schema_str)
            tables.append(SITable(name=field, data=data, schema=schema))

        return tables

    def _map_types(self, type_map: dict[str, str]) -> dict[str, pl.DataType | None]:
        """Map BQL type names to Polars data types; None (infer) for unknown names."""
        mapping = {
            "STRING": pl.Utf8,
            "ENUM": pl.Utf8,  # e.g. CPN_TYP of cpn(), the rating of bb_composite()
            "DOUBLE": pl.Float64,
            "INT": pl.Int64,
            "DATE": pl.Date,
            "DATETIME": pl.Datetime,  # e.g. the quote times of axes()
            "BOOLEAN": pl.Boolean,
        }
        schema = {}
        for col, bql_type in type_map.items():
            name = (bql_type or "STRING").upper()
            if name not in mapping:
                logger.warning(
                    "Unknown BQL type %r for column %r; Polars infers its dtype.",
                    bql_type,
                    col,
                )
            schema[col] = mapping.get(name)
        return schema

    def _apply_schema(self, table: SITable) -> SITable:
        """Convert data based on the schema (e.g., str -> date, 'NaN' -> None)."""
        for col, dtype in table.schema.items():
            if dtype == pl.Date:
                table.data[col] = [
                    datetime.fromisoformat(v).date() if isinstance(v, str) else None
                    for v in table.data[col]
                ]
            elif dtype == pl.Datetime:
                table.data[col] = [_convert_datetime(v) for v in table.data[col]]
            elif dtype in {pl.Float64, pl.Int64}:
                table.data[col] = [_convert_number(x) for x in table.data[col]]
        return table

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
                A search without results keeps its columns. A repeated column title
                gets a suffix (``Name``, ``Name_1``) and a warning is logged.

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
        request = self._create_bsrch_request(domain, overrides, options)
        responses = self._send_request(request)
        limit_applied = any(name == "LIMIT" for name, _ in _override_items(overrides))
        rows, columns = self._parse_bsrch_responses(
            responses, limit_applied=limit_applied
        )
        return pl.DataFrame(
            rows, schema=columns or None, infer_schema_length=None, strict=False
        )

    def _create_bsrch_request(
        self,
        domain: str,
        overrides: Mapping[str, Any] | Sequence[tuple[str, Any]] | None = None,
        options: dict | None = None,
    ) -> blpapi.Request:
        """Create an ExcelGetGridRequest for BSRCH on //blp/exrsvc."""
        service = self._get_service("//blp/exrsvc")
        request = service.createRequest("ExcelGetGridRequest")
        request.set("Domain", domain)

        if overrides:
            overrides_element = request.getElement("Overrides")
            for name, value in _override_items(overrides):
                override = overrides_element.appendElement()
                override.setElement("name", str(name))
                override.setElement("value", str(value))

        if options:
            for key, value in options.items():
                request.set(key, value)

        return request

    def _parse_bsrch_responses(
        self, responses: list[dict], *, limit_applied: bool = False
    ) -> tuple[list[dict], list[str]]:
        """Parse the grids that ExcelGetGridRequest returns; one per message.

        Each payload is the grid itself (live check L8): ColumnTitles, DataRecords,
        ReachMax and, when the search fails, Error. Returns the rows and the column
        names used: the titles, plus any col_<n> invented for an untitled field.
        """
        rows: list[dict[str, Any]] = []
        titles: list[str] = []
        errors: list[str] = []
        reach_max = False

        for grid in responses:
            if not isinstance(grid, dict) or "DataRecords" not in grid:
                continue  # not a grid
            titles = _unique_titles(grid.get("ColumnTitles") or []) or titles
            reach_max = reach_max or bool(grid.get("ReachMax"))
            if grid.get("Error"):
                errors.append(grid["Error"])
            for record in grid["DataRecords"] or []:
                fields = record.get("DataFields") or []
                values = [self._extract_bsrch_field_value(field) for field in fields]
                names = titles + [f"col_{i}" for i in range(len(titles), len(values))]
                rows.append(dict(zip(names, values, strict=False)))

        columns = list(dict.fromkeys([*titles, *(col for row in rows for col in row)]))
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
        if reach_max and not limit_applied:
            logger.warning(
                "BSRCH response reached internal limit; consider using LIMIT override."
            )

        self._coerce_bsrch_numeric_columns(rows)
        return rows, columns

    @staticmethod
    def _extract_bsrch_field_value(field: Any) -> Any:
        """Return the value of a DataFields entry; return other entries unchanged.

        Bloomberg sends {"StringValue": ...} or {"DoubleValue": ...} (live check L8).
        """
        if isinstance(field, dict):
            for key in ("StringValue", "DoubleValue"):
                if key in field:
                    return field[key]
        return field

    @staticmethod
    def _coerce_bsrch_numeric_columns(rows: list[dict[str, Any]]) -> None:
        """Convert empty/whitespace strings to None to allow numeric inference."""
        cols = dict.fromkeys(col for row in rows for col in row)  # in order of first use
        for col in cols:
            values = [row.get(col) for row in rows]
            cleaned: list[Any] = []
            numeric_candidate = True

            for val in values:
                if isinstance(val, str) and val.strip() == "":
                    cleaned.append(None)
                    continue
                cleaned.append(val)
                if val is None or isinstance(val, (int, float)):
                    continue
                numeric_candidate = False
                # leave column untouched if any non-numeric string present
                # other than whitespace
                # Note: do not break early to align cleaned length

            if numeric_candidate:
                for idx, cleaned_val in enumerate(cleaned):
                    rows[idx][col] = cleaned_val
