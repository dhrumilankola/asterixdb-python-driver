"""Python connector for AsterixDB."""

from .connection import Connection
from .cursor import Cursor
from .exceptions import (
    AsterixError, DatabaseError, InterfaceError, 
    OperationalError, ProgrammingError, NotSupportedError
)

__version__ = "0.1.0"
__all__ = [
    'Connection',
    'Cursor', 
    'AsterixError',
    'DatabaseError',
    'InterfaceError',
    'OperationalError', 
    'ProgrammingError',
    'NotSupportedError',
]