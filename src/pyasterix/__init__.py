"""Python connector for AsterixDB."""

# from .asterix_client import AsterixClient

from ._http_client import (
    AsterixDBHttpClient,
    QueryExecutionError,
    ConnectionTimeoutError,
    InvalidJSONResponseError,
)

__version__ = "0.1.0"
__all__ = [
    'AsterixDBHttpClient',
    'QueryExecutionError',
    'ConnectionTimeoutError',
    'InvalidJSONResponseError',
]