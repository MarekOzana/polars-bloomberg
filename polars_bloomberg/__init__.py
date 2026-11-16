"""polars_bloomberg package."""

from importlib.metadata import PackageNotFoundError, version

from .plbbg import BloombergError, BqlResult, BQuery

__all__ = ["BQuery", "BloombergError", "BqlResult"]

try:
    __version__ = version("polars-bloomberg")
except PackageNotFoundError:  # running from a source tree that is not installed
    __version__ = "0+unknown"
