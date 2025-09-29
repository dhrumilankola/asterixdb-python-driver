"""
AsterixDB Python Driver Exception Hierarchy

This module provides a comprehensive exception hierarchy for the AsterixDB Python driver
that is fully compliant with PEP 249 (Python Database API Specification v2.0) while
adding AsterixDB-specific error handling capabilities.

Exception Hierarchy:
    AsterixError (base)
    ├── Warning
    └── Error
        ├── InterfaceError
        └── DatabaseError
            ├── DataError
            │   ├── TypeMismatchError
            │   └── ResultProcessingError
            ├── OperationalError
            │   ├── NetworkError
            │   ├── HTTPError
            │   ├── TimeoutError
            │   ├── AuthenticationError
            │   ├── ResourceError
            │   ├── PoolExhaustedError
            │   └── ConnectionValidationError
            ├── IntegrityError
            ├── InternalError
            ├── ProgrammingError
            │   ├── SyntaxError
            │   ├── IdentifierError
            │   └── QueryBuildError
            └── NotSupportedError
"""

import time
import json
from typing import Dict, Any, Optional, Union


class AsterixError(Exception):
    """
    Base exception class for all AsterixDB client errors.
    
    This is the root of the exception hierarchy and provides common functionality
    for all AsterixDB-related exceptions including error context, timing, and
    debugging information.
    
    Attributes:
        message (str): Human-readable error message
        error_code (str, optional): AsterixDB-specific error code (e.g., 'ASX1001')
        context (dict): Additional context information
        timestamp (float): Unix timestamp when the error occurred
        errno (int, optional): Numeric error code for compatibility
        sqlstate (str, optional): SQL state code for compatibility
    """
    
    def __init__(self, message: str, error_code: Optional[str] = None, 
                 context: Optional[Dict[str, Any]] = None, errno: Optional[int] = None,
                 sqlstate: Optional[str] = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.context = context or {}
        self.timestamp = time.time()
        self.errno = errno
        self.sqlstate = sqlstate
        
    def __str__(self) -> str:
        """Enhanced string representation with error code and context."""
        base_msg = self.message
        if self.error_code:
            base_msg = f"[{self.error_code}] {base_msg}"
        return base_msg
    
    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return (f"{self.__class__.__name__}(message={self.message!r}, "
                f"error_code={self.error_code!r}, context={self.context!r})")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for serialization."""
        return {
            'type': self.__class__.__name__,
            'message': self.message,
            'error_code': self.error_code,
            'context': self.context,
            'timestamp': self.timestamp,
            'errno': self.errno,
            'sqlstate': self.sqlstate
        }


# PEP 249 Standard Exceptions with enhanced functionality

class Warning(AsterixError):
    """
    Exception raised for important warnings like data truncations while inserting, etc.
    
    This exception is not a subclass of Error but rather a separate category as per PEP 249.
    It indicates non-fatal issues that the application should be aware of.
    """
    pass


class Error(AsterixError):
    """
    Base class of all other error exceptions (only subclasses of this are considered errors).
    
    As per PEP 249, this is the base class for all database-related errors that are
    considered actual errors rather than warnings.
    """
    pass


# Database Interface Errors

class InterfaceError(Error):
    """
    Exception raised for errors that are related to the database interface rather than
    the database itself.
    
    This includes errors like:
    - Using a closed connection or cursor
    - Invalid cursor operations
    - Driver interface misuse
    
    These are typically programming errors in how the driver is being used.
    """
    pass


class DatabaseError(Error):
    """
    Exception raised for errors that are related to the database itself.
    
    This is the base class for all database-related errors including connection issues,
    query failures, data problems, etc. All database-specific errors inherit from this.
    """
    pass


# Database Error Subcategories (PEP 249 Standard)

class DataError(DatabaseError):
    """
    Exception raised for errors that are due to problems with the processed data.
    
    This includes:
    - Invalid data values
    - Type conversion errors
    - Data format problems
    - Result processing failures
    """
    pass


class OperationalError(DatabaseError):
    """
    Exception raised for errors that are related to the database's operation and not
    necessarily under the control of the programmer.
    
    This includes:
    - Connection lost during query execution
    - Database server errors
    - Network problems
    - Resource exhaustion
    - Timeout errors
    """
    pass


class IntegrityError(DatabaseError):
    """
    Exception raised when the relational integrity of the database is affected.
    
    This includes:
    - Constraint violations
    - Foreign key violations
    - Unique constraint violations
    
    Note: AsterixDB is NoSQL, so this may be less common but included for PEP 249 compliance.
    """
    pass


class InternalError(DatabaseError):
    """
    Exception raised when the database encounters an internal error.
    
    This includes:
    - Internal database system errors
    - Unexpected database state
    - Database corruption issues
    """
    pass


class ProgrammingError(DatabaseError):
    """
    Exception raised for programming errors.
    
    This includes:
    - SQL syntax errors
    - Wrong number of parameters
    - Invalid identifiers
    - Query building errors
    """
    pass


class NotSupportedError(DatabaseError):
    """
    Exception raised in case a method or database API was used which is not supported
    by the database.
    
    This includes:
    - Unsupported SQL++ features
    - Unsupported operation modes
    - Features not available in current AsterixDB version
    """
    pass


# AsterixDB-Specific Exception Hierarchy

# Data Errors (problems with data values, types, conversion)

class TypeMismatchError(DataError):
    """
    Exception raised when there's a type mismatch between Python and AsterixDB types.
    
    This includes:
    - Invalid type conversions
    - Unsupported Python types for AsterixDB
    - AsterixDB function parameter type mismatches
    """
    pass


class ResultProcessingError(DataError):
    """
    Exception raised when processing query results fails.
    
    This includes:
    - JSON parsing errors in responses
    - Malformed result data
    - Data conversion failures
    """
    pass


# Operational Errors (network, timeouts, resources)

class NetworkError(OperationalError):
    """
    Exception raised for network-related connection issues.
    
    This includes:
    - DNS resolution failures
    - Network connectivity issues
    - Socket errors
    - Connection refused
    """
    pass


class HTTPError(OperationalError):
    """
    Exception raised for HTTP-level errors.
    
    This includes:
    - HTTP 4xx client errors
    - HTTP 5xx server errors
    - Invalid HTTP responses
    - HTTP timeout errors
    """
    
    def __init__(self, message: str, status_code: Optional[int] = None, 
                 response_text: Optional[str] = None, **kwargs):
        super().__init__(message, **kwargs)
        self.status_code = status_code
        self.response_text = response_text
        
        # Add HTTP details to context
        if status_code:
            self.context['http_status_code'] = status_code
        if response_text:
            self.context['http_response'] = response_text[:1000]  # Limit size


class TimeoutError(OperationalError):
    """
    Exception raised when operations exceed their timeout limits.
    
    This includes:
    - Query execution timeouts
    - Connection establishment timeouts
    - Async query polling timeouts
    - Pool acquisition timeouts
    """
    
    def __init__(self, message: str, timeout_duration: Optional[float] = None,
                 operation_type: Optional[str] = None, **kwargs):
        super().__init__(message, **kwargs)
        self.timeout_duration = timeout_duration
        self.operation_type = operation_type
        
        # Add timeout details to context
        if timeout_duration:
            self.context['timeout_duration'] = timeout_duration
        if operation_type:
            self.context['operation_type'] = operation_type


class AuthenticationError(OperationalError):
    """
    Exception raised for authentication and authorization failures.
    
    This includes:
    - Invalid credentials
    - Access denied errors
    - Permission errors
    - Authentication token issues
    """
    pass


class ResourceError(OperationalError):
    """
    Exception raised when system resources are exhausted.
    
    This includes:
    - Disk space errors
    - Memory exhaustion
    - File handle limits
    - Database resource limits
    """
    
    def __init__(self, message: str, resource_type: Optional[str] = None, 
                 resource_limit: Optional[Union[int, str]] = None, **kwargs):
        super().__init__(message, **kwargs)
        self.resource_type = resource_type
        self.resource_limit = resource_limit
        
        # Add resource details to context
        if resource_type:
            self.context['resource_type'] = resource_type
        if resource_limit:
            self.context['resource_limit'] = resource_limit


class PoolExhaustedError(OperationalError):
    """
    Exception raised when connection pool capacity is exceeded.
    
    This includes:
    - No available connections in pool
    - Pool size limits reached
    - Pool acquisition timeouts
    """
    
    def __init__(self, message: str, pool_size: Optional[int] = None,
                 active_connections: Optional[int] = None, **kwargs):
        super().__init__(message, **kwargs)
        self.pool_size = pool_size
        self.active_connections = active_connections
        
        # Add pool details to context
        if pool_size:
            self.context['pool_size'] = pool_size
        if active_connections:
            self.context['active_connections'] = active_connections


class ConnectionValidationError(OperationalError):
    """
    Exception raised when connection validation fails.
    
    This includes:
    - Health check failures
    - Connection staleness
    - Invalid connection state
    """
    
    def __init__(self, message: str, validation_failures: Optional[int] = None, **kwargs):
        super().__init__(message, **kwargs)
        self.validation_failures = validation_failures
        
        if validation_failures:
            self.context['validation_failures'] = validation_failures


# Programming Errors (syntax, identifiers, query building)

class SyntaxError(ProgrammingError):
    """
    Exception raised for SQL++ syntax errors.
    
    This includes:
    - Invalid SQL++ syntax
    - Malformed queries
    - Reserved keyword misuse
    """
    
    def __init__(self, message: str, line_number: Optional[int] = None,
                 column_number: Optional[int] = None, query: Optional[str] = None, **kwargs):
        super().__init__(message, **kwargs)
        self.line_number = line_number
        self.column_number = column_number
        self.query = query
        
        # Add syntax error details to context
        if line_number:
            self.context['line_number'] = line_number
        if column_number:
            self.context['column_number'] = column_number
        if query:
            self.context['query'] = query[:500]  # Limit size


class IdentifierError(ProgrammingError):
    """
    Exception raised for identifier resolution errors.
    
    This includes:
    - Undefined identifiers
    - Ambiguous identifier references
    - Invalid identifier names
    """
    
    def __init__(self, message: str, identifier: Optional[str] = None, **kwargs):
        super().__init__(message, **kwargs)
        self.identifier = identifier
        
        if identifier:
            self.context['identifier'] = identifier


class QueryBuildError(ProgrammingError):
    """
    Exception raised when query building fails.
    
    This includes:
    - Invalid query construction
    - Missing required clauses
    - Incompatible query operations
    """
    pass


# Async Query Specific Errors

class AsyncQueryError(DatabaseError):
    """
    Exception raised for asynchronous query execution failures.
    
    This includes:
    - Async query submission failures
    - Query handle issues
    - Async status check errors
    """
    
    def __init__(self, message: str, handle: Optional[str] = None, 
                 query_status: Optional[str] = None, **kwargs):
        super().__init__(message, **kwargs)
        self.handle = handle
        self.query_status = query_status
        
        # Add async details to context
        if handle:
            self.context['async_handle'] = handle
        if query_status:
            self.context['query_status'] = query_status


class AsyncTimeoutError(TimeoutError):
    """
    Exception raised when asynchronous query polling times out.
    
    This includes:
    - Async query execution timeouts
    - Polling timeout exceeded
    - Query still running after timeout
    """
    pass


class HandleError(DatabaseError):
    """
    Exception raised for async query handle-related errors.
    
    This includes:
    - Invalid query handles
    - Missing query handles
    - Handle resolution failures
    """
    
    def __init__(self, message: str, handle: Optional[str] = None, **kwargs):
        super().__init__(message, **kwargs)
        self.handle = handle
        
        if handle:
            self.context['handle'] = handle


# Connection Pool Specific Errors

class PoolError(DatabaseError):
    """
    Base exception for connection pool related errors.
    
    This is the base class for all pool-specific errors.
    """
    pass


class PoolShutdownError(InterfaceError):
    """
    Exception raised when attempting operations on a shutdown pool.
    
    This includes:
    - Getting connections from shutdown pool
    - Operations on closed pool
    """
    pass


# DataFrame Specific Errors

class DataFrameError(DatabaseError):
    """
    Base exception for DataFrame operation errors.
    
    This includes:
    - DataFrame query execution errors
    - Result processing errors
    - DataFrame state errors
    """
    pass


# Error Mapping and Detection Utilities

class ErrorMapper:
    """
    Utility class for mapping various error conditions to appropriate AsterixDB exceptions.
    
    This class provides static methods to convert HTTP responses, AsterixDB error codes,
    and other error conditions into the appropriate exception types.
    """
    
    # AsterixDB Error Code to Exception mapping
    ASTERIX_ERROR_MAP = {
        'ASX1001': SyntaxError,           # Syntax error
        'ASX1073': IdentifierError,       # Cannot resolve identifier
        'ASX0002': TypeMismatchError,     # Type mismatch
        'ASX1002': SyntaxError,           # Syntax error variations
        'ASX1074': IdentifierError,       # Ambiguous identifier
        'ASX0001': InternalError,         # Internal system error
        'ASX0003': ResourceError,         # Resource exhaustion
        'ASX0004': NotSupportedError,     # Feature not supported
    }
    
    # HTTP Status Code to Exception mapping
    HTTP_ERROR_MAP = {
        400: SyntaxError,                 # Bad Request - usually syntax errors
        401: AuthenticationError,         # Unauthorized
        403: AuthenticationError,         # Forbidden
        404: IdentifierError,             # Not Found - dataset/dataverse not found
        408: TimeoutError,                # Request Timeout
        429: ResourceError,               # Too Many Requests - rate limiting
        500: InternalError,               # Internal Server Error
        502: NetworkError,                # Bad Gateway
        503: ResourceError,               # Service Unavailable
        504: TimeoutError,                # Gateway Timeout
    }
    
    @staticmethod
    def from_http_response(response, request_context: Optional[Dict[str, Any]] = None) -> AsterixError:
        """
        Map HTTP response to appropriate exception.
        
        Args:
            response: HTTP response object
            request_context: Additional context about the request
            
        Returns:
            Appropriate AsterixError subclass instance
        """
        status_code = response.status_code
        response_text = ""
        
        try:
            response_text = response.text
            # Try to parse JSON response for AsterixDB-specific errors
            if response_text:
                response_data = json.loads(response_text)
                if 'errors' in response_data:
                    return ErrorMapper.from_asterix_error_response(response_data, status_code)
        except (json.JSONDecodeError, ValueError):
            # Not JSON or malformed JSON
            pass
        
        # Map by HTTP status code
        exception_class = ErrorMapper.HTTP_ERROR_MAP.get(status_code, HTTPError)
        
        # Create context
        context = {
            'http_status_code': status_code,
            'http_response': response_text[:1000] if response_text else None,
            'url': getattr(response, 'url', None),
        }
        
        if request_context:
            context.update(request_context)
        
        # Create appropriate exception
        if exception_class == HTTPError:
            return HTTPError(
                f"HTTP {status_code} error: {response_text[:200]}",
                status_code=status_code,
                response_text=response_text,
                context=context
            )
        else:
            return exception_class(
                f"HTTP {status_code}: {response_text[:200]}",
                context=context
            )
    
    @staticmethod
    def from_asterix_error_response(response_data: Dict[str, Any], 
                                  status_code: Optional[int] = None) -> AsterixError:
        """
        Map AsterixDB error response to appropriate exception.
        
        Args:
            response_data: Parsed JSON response from AsterixDB
            status_code: HTTP status code if available
            
        Returns:
            Appropriate AsterixError subclass instance
        """
        errors = response_data.get('errors', [])
        if not errors:
            return DatabaseError("Unknown database error", context={'response': response_data})
        
        # Get the first error (most specific)
        error = errors[0] if isinstance(errors, list) else errors
        error_msg = str(error)
        
        # Extract error code if present
        error_code = None
        if isinstance(error, dict):
            error_code = error.get('code')
            error_msg = error.get('msg', str(error))
        
        # Try to extract error code from message (format: "ASX1001: message")
        if not error_code and isinstance(error_msg, str):
            import re
            match = re.match(r'ASX(\d+):', error_msg)
            if match:
                error_code = f"ASX{match.group(1)}"
        
        # Map to appropriate exception
        exception_class = DatabaseError
        if error_code:
            exception_class = ErrorMapper.ASTERIX_ERROR_MAP.get(error_code, DatabaseError)
        
        # Extract additional context
        context = {
            'asterix_error_code': error_code,
            'asterix_errors': errors,
            'response_data': response_data
        }
        
        if status_code:
            context['http_status_code'] = status_code
        
        # Create exception with enhanced context
        return exception_class(
            error_msg,
            error_code=error_code,
            context=context
        )
    
    @staticmethod
    def from_network_error(error: Exception, context: Optional[Dict[str, Any]] = None) -> NetworkError:
        """
        Map network-related exceptions to NetworkError.
        
        Args:
            error: Original network exception
            context: Additional context
            
        Returns:
            NetworkError instance
        """
        try:
            import requests
            
            error_context = {'original_error_type': type(error).__name__}
            if context:
                error_context.update(context)
            
            if isinstance(error, requests.exceptions.ConnectionError):
                return NetworkError(
                    f"Connection failed: {str(error)}",
                    context=error_context
                )
            elif isinstance(error, requests.exceptions.Timeout):
                return TimeoutError(
                    f"Request timed out: {str(error)}",
                    context=error_context
                )
            elif isinstance(error, requests.exceptions.HTTPError):
                return HTTPError(
                    f"HTTP error: {str(error)}",
                    context=error_context
                )
            else:
                return NetworkError(
                    f"Network error: {str(error)}",
                    context=error_context
                )
        except ImportError:
            # Fallback if requests is not available
            error_context = {'original_error_type': type(error).__name__}
            if context:
                error_context.update(context)
            
            return NetworkError(
                f"Network error: {str(error)}",
                context=error_context
            )
    
    @staticmethod
    def from_json_error(error: Exception, response_text: str) -> ResultProcessingError:
        """
        Map JSON parsing errors to ResultProcessingError.
        
        Args:
            error: JSON parsing exception
            response_text: Raw response text that failed to parse
            
        Returns:
            ResultProcessingError instance
        """
        return ResultProcessingError(
            f"Failed to parse JSON response: {str(error)}",
            context={
                'original_error_type': type(error).__name__,
                'response_text': response_text[:500],
                'response_length': len(response_text)
            }
        )
    
    @staticmethod
    def from_validation_error(field_name: str, field_value: Any, 
                            validation_type: str) -> DataError:
        """
        Create validation error with detailed context.
        
        Args:
            field_name: Name of the field that failed validation
            field_value: Value that failed validation
            validation_type: Type of validation that failed
            
        Returns:
            DataError instance
        """
        return DataError(
            f"Validation failed for field '{field_name}': {validation_type}",
            context={
                'field_name': field_name,
                'field_value': str(field_value)[:100],
                'validation_type': validation_type
            }
        )


class AsyncErrorMapper:
    """
    Specialized error mapper for async query operations.
    """
    
    @staticmethod
    def from_async_status(status_data: Dict[str, Any], handle: str) -> AsterixError:
        """
        Map async query status to appropriate exception.
        
        Args:
            status_data: Status response from async query
            handle: Query handle
            
        Returns:
            Appropriate exception for the status
        """
        status = status_data.get('status', 'unknown')
        
        context = {
            'async_handle': handle,
            'query_status': status,
            'status_data': status_data
        }
        
        if status in ['failed', 'fatal']:
            errors = status_data.get('errors', [])
            if errors:
                # Use main error mapper for AsterixDB errors
                return ErrorMapper.from_asterix_error_response(status_data)
            else:
                return AsyncQueryError(
                    f"Async query failed with status: {status}",
                    handle=handle,
                    query_status=status,
                    context=context
                )
        elif status == 'timeout':
            return AsyncTimeoutError(
                f"Async query timed out",
                context=context
            )
        else:
            return AsyncQueryError(
                f"Unexpected async query status: {status}",
                handle=handle,
                query_status=status,
                context=context
            )


# Legacy exception aliases for backward compatibility
# These map to the old exception names used in the codebase

ConnectionError = NetworkError  # Legacy alias
QueryError = AsyncQueryError    # Legacy alias  
ValidationError = DataError     # Legacy alias for validation issues
TypeMappingError = TypeMismatchError  # Legacy alias