"""
AsterixDB Exception Handling Guide

This example demonstrates best practices for handling exceptions in the AsterixDB Python driver.
It shows how to catch specific exception types and handle them appropriately.
"""

import time
import json
import os
import sys

root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
sys.path.insert(0, root_path)

from src.pyasterix import (
    connect, 
    # Exception types for specific handling
    NetworkError, HTTPError, TimeoutError, SyntaxError, IdentifierError,
    AsyncTimeoutError, PoolExhaustedError, ConnectionValidationError,
    ResultProcessingError, TypeMismatchError, DataFrameError,
    # Base exceptions for general handling
    DatabaseError, OperationalError, ProgrammingError, DataError
)
from src.pyasterix.exceptions import ErrorMapper


def demonstrate_basic_exception_handling():
    """Demonstrate basic exception handling patterns."""
    print("=== Basic Exception Handling ===")
    
    try:
        # Attempt to connect to AsterixDB
        connection = connect(host="localhost", port=19002, timeout=5)
        cursor = connection.cursor()
        
        # Execute a potentially problematic query
        cursor.execute("SELECT * FROM non_existent_dataset")
        results = cursor.fetchall()
        
    except NetworkError as e:
        print(f"Network connection failed: {e}")
        print(f"Error details: {e.context}")
        # Retry logic could go here
        
    except HTTPError as e:
        print(f"HTTP error occurred: {e}")
        print(f"Status code: {e.status_code}")
        print(f"Response: {e.response_text[:200] if e.response_text else 'No response'}")
        
    except SyntaxError as e:
        print(f"SQL++ syntax error: {e}")
        if e.line_number:
            print(f"Error at line {e.line_number}, column {e.column_number}")
        print(f"Query: {e.query[:100] if e.query else 'N/A'}")
        
    except IdentifierError as e:
        print(f"Identifier not found: {e}")
        print(f"Problematic identifier: {e.identifier}")
        
    except TimeoutError as e:
        print(f"Operation timed out: {e}")
        print(f"Timeout duration: {e.timeout_duration}s")
        print(f"Operation type: {e.operation_type}")
        
    except DatabaseError as e:
        print(f"General database error: {e}")
        print(f"Error code: {e.error_code}")
        print(f"Context: {e.context}")
        
    finally:
        # Always clean up resources
        try:
            cursor.close()
            connection.close()
        except:
            pass


def demonstrate_async_query_handling():
    """Demonstrate handling async query exceptions."""
    print("\n=== Async Query Exception Handling ===")
    
    try:
        connection = connect(host="localhost", port=19002)
        cursor = connection.cursor()
        
        # Submit async query
        cursor.execute(
            "SELECT * FROM large_dataset WHERE complex_condition = true",
            mode="async"
        )
        
        # Wait for result with timeout
        result = cursor.get_async_result(timeout=30)
        print(f"Async query completed with {len(result)} results")
        
    except AsyncTimeoutError as e:
        print(f"Async query timed out: {e}")
        print(f"Timeout after: {e.timeout_duration}s")
        print(f"Handle: {e.context.get('handle', 'N/A')}")
        print(f"Attempts made: {e.context.get('attempts', 'N/A')}")
        
        # Could implement retry logic here
        print("Consider retrying with longer timeout or checking query complexity")
        
    except AsyncQueryError as e:
        print(f"Async query failed: {e}")
        print(f"Query handle: {e.handle}")
        print(f"Status: {e.query_status}")
        
        # Log details for debugging
        print(f"Full context: {json.dumps(e.context, indent=2)}")
        
    except HandleError as e:
        print(f"Invalid or missing query handle: {e}")
        print(f"Handle: {e.handle}")
        # This usually indicates a programming error
        
    finally:
        try:
            cursor.close()
            connection.close()
        except:
            pass


def demonstrate_connection_pool_handling():
    """Demonstrate handling connection pool exceptions."""
    print("\n=== Connection Pool Exception Handling ===")
    
    from src.pyasterix import create_pool, PoolConfig
    
    try:
        # Create a small pool for demonstration
        pool_config = PoolConfig(
            max_pool_size=2,
            min_pool_size=1,
            pool_wait_timeout=5
        )
        
        pool = create_pool(
            host="localhost",
            port=19002,
            pool_config=pool_config
        )
        
        # Try to get more connections than available
        connections = []
        for i in range(5):  # More than max_pool_size
            with pool.get_connection() as conn:
                connections.append(conn)
                # Simulate work
                time.sleep(1)
                
    except PoolExhaustedError as e:
        print(f"Connection pool exhausted: {e}")
        print(f"Pool size: {e.pool_size}")
        print(f"Active connections: {e.active_connections}")
        print("Consider increasing pool size or reducing connection hold time")
        
    except ConnectionValidationError as e:
        print(f"Connection validation failed: {e}")
        print(f"Validation failures: {e.validation_failures}")
        print("Pool will recreate the connection automatically")
        
    except PoolShutdownError as e:
        print(f"Attempted to use shutdown pool: {e}")
        print("Create a new pool instance")
        
    finally:
        try:
            pool.shutdown()
        except:
            pass


def demonstrate_data_processing_errors():
    """Demonstrate handling data processing exceptions."""
    print("\n=== Data Processing Exception Handling ===")
    
    try:
        connection = connect(host="localhost", port=19002)
        cursor = connection.cursor()
        
        # Execute query that might return problematic data
        cursor.execute("SELECT complex_nested_data FROM dataset")
        
        # Process results
        for row in cursor:
            # This might fail due to type mismatches or malformed data
            processed_data = process_complex_data(row)
            
    except ResultProcessingError as e:
        print(f"Failed to process query results: {e}")
        print(f"Response length: {e.context.get('response_length', 'N/A')}")
        print("Check for malformed JSON or unexpected data structure")
        
    except TypeMismatchError as e:
        print(f"Type conversion failed: {e}")
        print(f"Context: {e.context}")
        print("Verify data types match expected schema")
        
    except DataError as e:
        print(f"Data validation error: {e}")
        if 'field_name' in e.context:
            print(f"Problematic field: {e.context['field_name']}")
            print(f"Field value: {e.context['field_value']}")
        
    finally:
        try:
            cursor.close()
            connection.close()
        except:
            pass


def demonstrate_dataframe_error_handling():
    """Demonstrate DataFrame-specific exception handling."""
    print("\n=== DataFrame Exception Handling ===")
    
    try:
        from src.pyasterix.dataframe import AsterixDataFrame
        
        connection = connect(host="localhost", port=19002)
        
        # Create DataFrame
        df = AsterixDataFrame(connection, "test_dataset")
        
        # Chain operations that might fail
        result = (df
                 .filter(df.invalid_field == "test")  # Invalid field
                 .group_by("category")
                 .agg({"price": "sum", "invalid_field": "avg"})  # Invalid aggregation
                 .execute())
        
    except DataFrameError as e:
        print(f"DataFrame operation failed: {e}")
        print("Check field names and query structure")
        
    except QueryBuildError as e:
        print(f"Query building failed: {e}")
        print("Verify DataFrame operations are compatible")
        
    finally:
        try:
            connection.close()
        except:
            pass


def demonstrate_error_context_usage():
    """Demonstrate how to use error context for debugging."""
    print("\n=== Using Error Context for Debugging ===")
    
    try:
        connection = connect(host="localhost", port=19002, timeout=1)  # Very short timeout
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM large_dataset")
        
    except Exception as e:
        print(f"Exception type: {type(e).__name__}")
        print(f"Error message: {e}")
        
        # Check if it's an AsterixDB exception with context
        if hasattr(e, 'context'):
            print("\nDetailed context:")
            for key, value in e.context.items():
                print(f"  {key}: {value}")
        
        # Check for specific attributes
        if hasattr(e, 'error_code'):
            print(f"\nAsterixDB error code: {e.error_code}")
        
        if hasattr(e, 'status_code'):
            print(f"HTTP status code: {e.status_code}")
        
        if hasattr(e, 'timeout_duration'):
            print(f"Timeout duration: {e.timeout_duration}s")
        
        # Serialize for logging
        if hasattr(e, 'to_dict'):
            error_dict = e.to_dict()
            print(f"\nSerialized error: {json.dumps(error_dict, indent=2)}")


def demonstrate_error_recovery_patterns():
    """Demonstrate error recovery and retry patterns."""
    print("\n=== Error Recovery Patterns ===")
    
    def execute_with_retry(cursor, query, max_retries=3, backoff_factor=2):
        """Execute query with exponential backoff retry."""
        for attempt in range(max_retries):
            try:
                cursor.execute(query)
                return cursor.fetchall()
                
            except NetworkError as e:
                if attempt == max_retries - 1:
                    raise
                wait_time = backoff_factor ** attempt
                print(f"Network error on attempt {attempt + 1}, retrying in {wait_time}s...")
                time.sleep(wait_time)
                
            except TimeoutError as e:
                if attempt == max_retries - 1:
                    raise
                # Increase timeout for retry
                new_timeout = e.timeout_duration * 1.5 if e.timeout_duration else 30
                print(f"Timeout on attempt {attempt + 1}, increasing timeout to {new_timeout}s...")
                
            except SyntaxError as e:
                # Don't retry syntax errors
                print("Syntax error detected - not retrying")
                raise
    
    try:
        connection = connect(host="localhost", port=19002)
        cursor = connection.cursor()
        
        # Try query with retry logic
        results = execute_with_retry(cursor, "SELECT COUNT(*) FROM dataset")
        print(f"Query succeeded with {len(results)} results")
        
    except Exception as e:
        print(f"Query failed after all retries: {e}")
    
    finally:
        try:
            cursor.close()
            connection.close()
        except:
            pass


def process_complex_data(data):
    """Dummy function to simulate data processing that might fail."""
    # This would contain actual data processing logic
    if not isinstance(data, dict):
        raise TypeMismatchError("Expected dictionary data structure")
    return data


def demonstrate_custom_error_handling():
    """Demonstrate creating custom error handling logic."""
    print("\n=== Custom Error Handling Logic ===")
    
    class AsterixRetryHandler:
        """Custom retry handler for AsterixDB operations."""
        
        def __init__(self, max_retries=3, timeout_multiplier=1.5):
            self.max_retries = max_retries
            self.timeout_multiplier = timeout_multiplier
        
        def should_retry(self, exception, attempt):
            """Determine if an exception should trigger a retry."""
            if attempt >= self.max_retries:
                return False
            
            # Retry on network and timeout errors
            if isinstance(exception, (NetworkError, TimeoutError)):
                return True
            
            # Retry on specific HTTP errors
            if isinstance(exception, HTTPError):
                return exception.status_code in [502, 503, 504]
            
            # Don't retry on programming errors
            if isinstance(exception, (SyntaxError, IdentifierError)):
                return False
            
            return False
        
        def handle_exception(self, exception, attempt):
            """Handle exception and prepare for retry if applicable."""
            print(f"Attempt {attempt + 1} failed: {exception}")
            
            if self.should_retry(exception, attempt):
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                return True
            else:
                print("Not retrying this exception")
                return False
    
    # Use the custom handler
    retry_handler = AsterixRetryHandler()
    
    for attempt in range(retry_handler.max_retries):
        try:
            connection = connect(host="localhost", port=19002, timeout=1)
            cursor = connection.cursor()
            cursor.execute("SELECT * FROM test_dataset")
            results = cursor.fetchall()
            print(f"Success on attempt {attempt + 1}")
            break
            
        except Exception as e:
            if not retry_handler.handle_exception(e, attempt):
                print(f"Final failure: {e}")
                break
        finally:
            try:
                cursor.close()
                connection.close()
            except:
                pass


if __name__ == "__main__":
    """Run all demonstration functions."""
    print("AsterixDB Exception Handling Guide")
    print("=" * 50)
    
    demonstrations = [
        demonstrate_basic_exception_handling,
        demonstrate_async_query_handling,
        demonstrate_connection_pool_handling,
        demonstrate_data_processing_errors,
        demonstrate_dataframe_error_handling,
        demonstrate_error_context_usage,
        demonstrate_error_recovery_patterns,
        demonstrate_custom_error_handling
    ]
    
    for demo_func in demonstrations:
        try:
            demo_func()
        except Exception as e:
            print(f"Demo function {demo_func.__name__} failed: {e}")
        print()  # Add spacing between demonstrations
    
    print("Exception handling guide completed!")
