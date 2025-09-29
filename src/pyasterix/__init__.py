"""Python connector for AsterixDB."""

from .connection import Connection, connect
from .cursor import Cursor
from .pool import AsterixConnectionPool, PoolConfig, create_pool
from .exceptions import (
    # Base exceptions
    AsterixError, Warning, Error,
    
    # PEP 249 standard exceptions
    InterfaceError, DatabaseError, DataError, OperationalError,
    IntegrityError, InternalError, ProgrammingError, NotSupportedError,
    
    # AsterixDB-specific exceptions
    TypeMismatchError, ResultProcessingError, NetworkError, HTTPError,
    TimeoutError, AuthenticationError, ResourceError, PoolExhaustedError,
    ConnectionValidationError, SyntaxError, IdentifierError, QueryBuildError,
    AsyncQueryError, AsyncTimeoutError, HandleError, PoolError, PoolShutdownError,
    DataFrameError,
    
    # Error mapping utilities
    ErrorMapper, AsyncErrorMapper,
    
    # Legacy aliases for backward compatibility
    ConnectionError, QueryError, ValidationError, TypeMappingError
)
from .observability import (
    ObservabilityConfig, ObservabilityManager, 
    MetricsConfig, TracingConfig, LoggingConfig,
    initialize_observability, get_observability_manager
)

__version__ = "0.1.0"
__all__ = [
    # Connection and core components
    'Connection',
    'connect',
    'Cursor',
    'AsterixConnectionPool',
    'PoolConfig', 
    'create_pool',
    
    # Base exceptions
    'AsterixError',
    'Warning',
    'Error',
    
    # PEP 249 standard exceptions
    'InterfaceError',
    'DatabaseError',
    'DataError',
    'OperationalError',
    'IntegrityError',
    'InternalError',
    'ProgrammingError',
    'NotSupportedError',
    
    # AsterixDB-specific exceptions
    'TypeMismatchError',
    'ResultProcessingError',
    'NetworkError',
    'HTTPError',
    'TimeoutError',
    'AuthenticationError',
    'ResourceError',
    'PoolExhaustedError',
    'ConnectionValidationError',
    'SyntaxError',
    'IdentifierError',
    'QueryBuildError',
    'AsyncQueryError',
    'AsyncTimeoutError',
    'HandleError',
    'PoolError',
    'PoolShutdownError',
    'DataFrameError',
    
    # Error mapping utilities
    'ErrorMapper',
    'AsyncErrorMapper',
    
    # Legacy aliases
    'ConnectionError',
    'QueryError',
    'ValidationError',
    'TypeMappingError',
    
    # Observability components
    'ObservabilityConfig',
    'ObservabilityManager',
    'MetricsConfig',
    'TracingConfig', 
    'LoggingConfig',
    'initialize_observability',
    'get_observability_manager',
]