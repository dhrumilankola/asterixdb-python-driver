# AsterixDB Python Driver Exception Handling

This document provides comprehensive documentation for the exception handling system in the AsterixDB Python driver, which is fully compliant with PEP 249 (Python Database API Specification v2.0) while providing enhanced error handling capabilities specific to AsterixDB.

## Table of Contents

1. [Exception Hierarchy](#exception-hierarchy)
2. [PEP 249 Standard Exceptions](#pep-249-standard-exceptions)
3. [AsterixDB-Specific Exceptions](#asterixdb-specific-exceptions)
4. [Error Mapping and Detection](#error-mapping-and-detection)
5. [Usage Examples](#usage-examples)
6. [Best Practices](#best-practices)
7. [Migration Guide](#migration-guide)

## Exception Hierarchy

The AsterixDB Python driver provides a comprehensive exception hierarchy that extends the PEP 249 standard:

```
AsterixError (base)
├── Warning
└── Error
    ├── InterfaceError
    │   └── PoolShutdownError
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
        ├── NotSupportedError
        ├── AsyncQueryError
        │   └── AsyncTimeoutError
        ├── HandleError
        ├── PoolError
        └── DataFrameError
```

## PEP 249 Standard Exceptions

All exceptions are fully compliant with PEP 249 and include the standard exception types:

### Base Exceptions

#### `AsterixError`
The root exception class for all AsterixDB-related errors. Provides enhanced functionality including:
- Error codes (AsterixDB-specific codes like "ASX1001")
- Context information (detailed error context)
- Timestamps
- Serialization support

#### `Warning`
Exception raised for important warnings like data truncations.

#### `Error`
Base class for all error exceptions (not warnings).

### Standard Database Exceptions

#### `InterfaceError`
Errors related to the database interface rather than the database itself:
- Using closed connections or cursors
- Invalid cursor operations
- Driver interface misuse

#### `DatabaseError`
Base class for all database-related errors.

#### `DataError`
Errors due to problems with processed data:
- Invalid data values
- Type conversion errors
- Data format problems

#### `OperationalError`
Errors related to database operation:
- Connection issues
- Network problems
- Resource exhaustion
- Timeout errors

#### `IntegrityError`
Database integrity violations (less common in NoSQL context).

#### `InternalError`
Internal database errors.

#### `ProgrammingError`
Programming errors:
- SQL syntax errors
- Wrong number of parameters
- Invalid identifiers

#### `NotSupportedError`
Unsupported operations or features.

## AsterixDB-Specific Exceptions

### Data Processing Errors

#### `TypeMismatchError`
Type mismatch between Python and AsterixDB types.

```python
try:
    cursor.execute("SELECT get_day('invalid_date')")
except TypeMismatchError as e:
    print(f"Type error: {e}")
    print(f"Error code: {e.error_code}")  # e.g., "ASX0002"
```

#### `ResultProcessingError`
Query result processing failures.

```python
try:
    result_data = response.json()
except ResultProcessingError as e:
    print(f"JSON parsing failed: {e}")
    print(f"Response length: {e.context['response_length']}")
```

### Network and Connection Errors

#### `NetworkError`
Network-related connection issues.

```python
try:
    connection = connect(host="unreachable-host")
except NetworkError as e:
    print(f"Network error: {e}")
    print(f"Original error type: {e.context['original_error_type']}")
```

#### `HTTPError`
HTTP-level errors with detailed context.

```python
try:
    cursor.execute("INVALID QUERY")
except HTTPError as e:
    print(f"HTTP {e.status_code}: {e}")
    print(f"Response: {e.response_text[:200]}")
```

#### `TimeoutError`
Operation timeout errors with duration tracking.

```python
try:
    cursor.execute("SELECT * FROM huge_dataset", timeout=5)
except TimeoutError as e:
    print(f"Timeout after {e.timeout_duration}s")
    print(f"Operation: {e.operation_type}")
```

### Query and Programming Errors

#### `SyntaxError`
SQL++ syntax errors with location information.

```python
try:
    cursor.execute("SELECT * FROM WHERE invalid_syntax")
except SyntaxError as e:
    print(f"Syntax error at line {e.line_number}, column {e.column_number}")
    print(f"Query: {e.query}")
```

#### `IdentifierError`
Identifier resolution errors.

```python
try:
    cursor.execute("SELECT undefined_field FROM dataset")
except IdentifierError as e:
    print(f"Undefined identifier: {e.identifier}")
```

### Async Query Errors

#### `AsyncQueryError`
Asynchronous query execution failures.

```python
try:
    cursor.execute("SELECT * FROM dataset", mode="async")
    result = cursor.get_async_result()
except AsyncQueryError as e:
    print(f"Async query failed: {e}")
    print(f"Handle: {e.handle}")
    print(f"Status: {e.query_status}")
```

#### `AsyncTimeoutError`
Async query timeout with detailed context.

```python
try:
    result = cursor.get_async_result(timeout=30)
except AsyncTimeoutError as e:
    print(f"Async timeout after {e.timeout_duration}s")
    print(f"Total attempts: {e.context['total_attempts']}")
```

#### `HandleError`
Invalid or missing async query handles.

```python
try:
    cursor.get_async_result()  # No async query executed
except HandleError as e:
    print(f"Invalid handle: {e.handle}")
```

### Connection Pool Errors

#### `PoolExhaustedError`
Connection pool capacity exceeded.

```python
try:
    with pool.get_connection() as conn:
        # Pool is full
        pass
except PoolExhaustedError as e:
    print(f"Pool exhausted: {e.pool_size} connections, {e.active_connections} active")
```

#### `PoolShutdownError`
Operations on shutdown pools.

```python
try:
    with shutdown_pool.get_connection() as conn:
        pass
except PoolShutdownError as e:
    print(f"Pool is shut down: {e}")
```

#### `ConnectionValidationError`
Connection validation failures.

```python
# Automatic validation during connection borrowing
try:
    with pool.get_connection() as conn:
        pass
except ConnectionValidationError as e:
    print(f"Connection validation failed: {e.validation_failures} failures")
```

## Error Mapping and Detection

The driver provides intelligent error mapping through the `ErrorMapper` and `AsyncErrorMapper` classes:

### HTTP Response Mapping

```python
from pyasterix.exceptions import ErrorMapper

# Automatically maps HTTP responses to appropriate exceptions
try:
    response = requests.post(url, data=payload)
    response.raise_for_status()
except requests.exceptions.HTTPError as e:
    # Convert to appropriate AsterixDB exception
    asterix_error = ErrorMapper.from_http_response(e.response)
    raise asterix_error
```

### AsterixDB Error Code Mapping

AsterixDB error codes are automatically mapped to specific exception types:

- `ASX1001`, `ASX1002`: `SyntaxError`
- `ASX1073`, `ASX1074`: `IdentifierError`
- `ASX0002`: `TypeMismatchError`
- `ASX0001`: `InternalError`
- `ASX0003`: `ResourceError`

### Network Error Mapping

```python
try:
    # Network operation
    pass
except requests.exceptions.ConnectionError as e:
    # Automatically converted to NetworkError with context
    raise ErrorMapper.from_network_error(e, context={'operation': 'query'})
```

## Usage Examples

### Basic Exception Handling

```python
import pyasterix
from pyasterix import (
    NetworkError, SyntaxError, TimeoutError, 
    DatabaseError, DataError
)

try:
    connection = pyasterix.connect(host="localhost", port=19002)
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM dataset WHERE condition = ?", ["value"])
    results = cursor.fetchall()
    
except NetworkError as e:
    print(f"Connection failed: {e}")
    # Implement retry logic
    
except SyntaxError as e:
    print(f"Query syntax error: {e}")
    # Fix query syntax
    
except TimeoutError as e:
    print(f"Query timeout: {e}")
    # Increase timeout or optimize query
    
except DataError as e:
    print(f"Data processing error: {e}")
    # Handle data validation issues
    
except DatabaseError as e:
    print(f"Database error: {e}")
    # Generic database error handling
    
finally:
    cursor.close()
    connection.close()
```

### Async Query Exception Handling

```python
from pyasterix import AsyncTimeoutError, AsyncQueryError, HandleError

try:
    cursor.execute("SELECT * FROM large_dataset", mode="async")
    result = cursor.get_async_result(timeout=60)
    
except AsyncTimeoutError as e:
    print(f"Async query timed out after {e.timeout_duration}s")
    # Could implement longer timeout or query optimization
    
except AsyncQueryError as e:
    print(f"Async query failed: {e}")
    print(f"Status: {e.query_status}")
    
except HandleError as e:
    print(f"Invalid query handle: {e}")
    # Programming error - ensure async query was submitted
```

### Connection Pool Exception Handling

```python
from pyasterix import create_pool, PoolExhaustedError, ConnectionValidationError

pool = create_pool(host="localhost", port=19002, max_pool_size=5)

try:
    with pool.get_connection(timeout=10) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM dataset")
        result = cursor.fetchone()
        
except PoolExhaustedError as e:
    print(f"No connections available in pool of size {e.pool_size}")
    # Increase pool size or reduce connection hold time
    
except ConnectionValidationError as e:
    print(f"Connection validation failed: {e}")
    # Pool will automatically recreate the connection
```

### Error Context Usage

```python
try:
    # Some operation
    pass
except Exception as e:
    if hasattr(e, 'context'):
        print("Detailed error context:")
        for key, value in e.context.items():
            print(f"  {key}: {value}")
    
    # Serialize for logging
    if hasattr(e, 'to_dict'):
        import json
        error_dict = e.to_dict()
        print(json.dumps(error_dict, indent=2))
```

## Best Practices

### 1. Catch Specific Exceptions

```python
# Good: Catch specific exception types
try:
    cursor.execute(query)
except SyntaxError:
    # Handle syntax errors specifically
    pass
except NetworkError:
    # Handle network issues specifically
    pass

# Avoid: Catching generic exceptions
try:
    cursor.execute(query)
except Exception:
    # Too broad - loses error context
    pass
```

### 2. Use Exception Context

```python
try:
    cursor.execute(query)
except TimeoutError as e:
    # Use specific timeout information
    print(f"Query timed out after {e.timeout_duration}s")
    print(f"Operation type: {e.operation_type}")
    
    # Access full context
    print(f"Context: {e.context}")
```

### 3. Implement Retry Logic

```python
def execute_with_retry(cursor, query, max_retries=3):
    for attempt in range(max_retries):
        try:
            cursor.execute(query)
            return cursor.fetchall()
        except (NetworkError, TimeoutError) as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)  # Exponential backoff
        except (SyntaxError, IdentifierError):
            # Don't retry programming errors
            raise
```

### 4. Log Errors Appropriately

```python
import logging
import json

logger = logging.getLogger(__name__)

try:
    cursor.execute(query)
except Exception as e:
    # Log with full context
    if hasattr(e, 'to_dict'):
        logger.error(f"Query failed: {json.dumps(e.to_dict())}")
    else:
        logger.error(f"Query failed: {e}")
    raise
```

### 5. Handle Pool Errors Gracefully

```python
def safe_pool_operation(pool, operation):
    try:
        with pool.get_connection() as conn:
            return operation(conn)
    except PoolExhaustedError:
        # Wait and retry
        time.sleep(1)
        with pool.get_connection(timeout=30) as conn:
            return operation(conn)
    except ConnectionValidationError:
        # Pool will handle recreation
        with pool.get_connection() as conn:
            return operation(conn)
```

## Migration Guide

### From Legacy Exceptions

The driver maintains backward compatibility through aliases:

```python
# Old code (still works)
try:
    cursor.execute(query)
except ConnectionError:  # Maps to NetworkError
    pass
except QueryError:       # Maps to AsyncQueryError
    pass
except ValidationError:  # Maps to DataError
    pass
except TypeMappingError: # Maps to TypeMismatchError
    pass

# New code (recommended)
try:
    cursor.execute(query)
except NetworkError:
    pass
except AsyncQueryError:
    pass
except DataError:
    pass
except TypeMismatchError:
    pass
```

### Enhanced Error Information

New exceptions provide much more context:

```python
# Old: Limited error information
try:
    cursor.execute(query)
except Exception as e:
    print(f"Error: {e}")

# New: Rich error context
try:
    cursor.execute(query)
except HTTPError as e:
    print(f"HTTP {e.status_code}: {e}")
    print(f"Response: {e.response_text}")
    print(f"Context: {e.context}")
except SyntaxError as e:
    print(f"Syntax error at line {e.line_number}")
    print(f"Query: {e.query}")
```

### Error Mapping Usage

```python
# Manual error conversion (if needed)
from pyasterix.exceptions import ErrorMapper

try:
    response = requests.post(url, data=payload)
    response.raise_for_status()
except requests.exceptions.HTTPError as e:
    # Convert to AsterixDB exception
    asterix_error = ErrorMapper.from_http_response(e.response)
    raise asterix_error
```

## Troubleshooting Common Issues

### 1. Connection Problems

```python
try:
    connection = connect(host="localhost", port=19002)
except NetworkError as e:
    print("Troubleshooting steps:")
    print("1. Check if AsterixDB is running")
    print("2. Verify host and port are correct")
    print("3. Check network connectivity")
    print(f"Error details: {e.context}")
```

### 2. Query Syntax Issues

```python
try:
    cursor.execute("SELECT * FROM dataset WHERE invalid")
except SyntaxError as e:
    print(f"Fix syntax error at line {e.line_number}, column {e.column_number}")
    print(f"Problematic query: {e.query}")
    print("Check SQL++ documentation for correct syntax")
```

### 3. Async Query Problems

```python
try:
    result = cursor.get_async_result(timeout=30)
except AsyncTimeoutError as e:
    print("Async query troubleshooting:")
    print(f"- Query ran for {e.timeout_duration}s")
    print(f"- Handle: {e.context.get('handle')}")
    print("- Consider optimizing query or increasing timeout")
    print("- Check dataset size and complexity")
```

### 4. Pool Issues

```python
try:
    with pool.get_connection() as conn:
        # Operations
        pass
except PoolExhaustedError as e:
    print("Pool troubleshooting:")
    print(f"- Pool size: {e.pool_size}")
    print(f"- Active connections: {e.active_connections}")
    print("- Increase pool size")
    print("- Reduce connection hold time")
    print("- Check for connection leaks")
```

This exception handling system provides robust error management while maintaining compatibility with Python database standards and offering enhanced debugging capabilities specific to AsterixDB operations.
