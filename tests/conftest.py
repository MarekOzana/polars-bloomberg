from __future__ import annotations

from collections.abc import Generator
from unittest.mock import MagicMock

import pytest

from polars_bloomberg import BQuery

def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers", "no_bbg: tests that do not require Bloomberg"
    )
    config.addinivalue_line(
        "markers", "requires_bbg: tests that require a live Bloomberg session"
    )


@pytest.fixture()
def bq() -> BQuery:
    """Return a BQuery instance with a mocked session for unit tests."""
    bq_instance = BQuery()
    bq_instance.session = MagicMock()
    return bq_instance


@pytest.fixture()
def live_bq() -> Generator[BQuery, None, None]:
    """Return a live BQuery instance when Bloomberg is available."""
    with BQuery() as bq_instance:
        yield bq_instance
