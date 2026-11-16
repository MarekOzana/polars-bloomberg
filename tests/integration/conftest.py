"""The live tests double as a tour of polars-bloomberg; tests/README.md gives the order.

They need a Bloomberg Terminal on this machine: `pytest --bbg`.
"""

from collections.abc import Iterator

import pytest

from polars_bloomberg import BQuery


@pytest.fixture(scope="session")
def bq() -> Iterator[BQuery]:
    """A BQuery connected to the Terminal, shared by the live tests.

    In your own code you write the same thing around your calls:

        with BQuery() as bq:
            df = bq.bdp("IBM US Equity", "NAME")
    """
    with BQuery() as bq:
        yield bq
