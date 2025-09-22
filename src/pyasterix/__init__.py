"""Python connector for AsterixDB."""

from .connection import Connection, connect
from .cursor import Cursor
from .pool import AsterixConnectionPool, PoolConfig, create_pool
from .exceptions import (
    AsterixError, DatabaseError, InterfaceError, 
    OperationalError, ProgrammingError, NotSupportedError
)
from .observability import (
    ObservabilityConfig, ObservabilityManager, 
    MetricsConfig, TracingConfig, LoggingConfig,
    initialize_observability, get_observability_manager
)

__version__ = "0.1.0"
__all__ = [
    'Connection',
    'connect',
    'Cursor',
    'AsterixConnectionPool',
    'PoolConfig', 
    'create_pool',
    'AsterixError',
    'DatabaseError',
    'InterfaceError',
    'OperationalError', 
    'ProgrammingError',
    'NotSupportedError',
    'ObservabilityConfig',
    'ObservabilityManager',
    'MetricsConfig',
    'TracingConfig', 
    'LoggingConfig',
    'initialize_observability',
    'get_observability_manager',
]