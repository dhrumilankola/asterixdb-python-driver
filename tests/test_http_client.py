import pytest
import responses
import json
from pyasterix._http_client import (
    AsterixDBHttpClient,
    QueryExecutionError,
    ConnectionTimeoutError,
    InvalidJSONResponseError,
)

BASE_URL = "http://localhost:19002"
QUERY_ENDPOINT = f"{BASE_URL}/query/service"

@pytest.fixture
def client():
    """Create a test client instance."""
    return AsterixDBHttpClient(BASE_URL)

@responses.activate
def test_simple_query_success(client):
    """Test successful execution of a simple query."""
    # Mock successful response
    responses.add(
        responses.POST,
        QUERY_ENDPOINT,
        json={
            "requestID": "123",
            "clientContextID": "xyz",
            "signature": "*",
            "results": [{"$1": 2}],
            "status": "success",
            "metrics": {
                "elapsedTime": "10.263371ms",
                "executionTime": "9.889389ms",
                "resultCount": 1,
                "resultSize": 15
            }
        },
        status=200
    )

    result = client.execute_query("SELECT VALUE 1 + 1;")
    assert result["status"] == "success"
    assert result["results"][0]["$1"] == 2

@responses.activate
def test_query_with_syntax_error(client):
    """Test handling of SQL++ syntax error."""
    error_response = {
        "requestID": "123",
        "status": "error",
        "errors": ["Syntax error: Encountered \"SLECT\" at line 1"]
    }
    
    responses.add(
        responses.POST,
        QUERY_ENDPOINT,
        json=error_response,
        status=200
    )

    with pytest.raises(QueryExecutionError) as exc_info:
        client.execute_query("SLECT VALUE 1 + 1;")  # Intentional typo
    assert "Syntax error" in str(exc_info.value)

@responses.activate
def test_invalid_json_response(client):
    """Test handling of invalid JSON response."""
    responses.add(
        responses.POST,
        QUERY_ENDPOINT,
        body="Invalid JSON{",
        status=200
    )

    with pytest.raises(InvalidJSONResponseError):
        client.execute_query("SELECT VALUE 1 + 1;")

@responses.activate
def test_connection_timeout(client):
    """Test handling of connection timeout."""
    responses.add(
        responses.POST,
        QUERY_ENDPOINT,
        body=responses.ConnectionError()
    )

    with pytest.raises(QueryExecutionError):
        client.execute_query("SELECT VALUE 1 + 1;")

@responses.activate
def test_deferred_query_execution(client):
    """Test successful execution of deferred query."""
    # Mock initial response with handle
    initial_response = {
        "requestID": "123",
        "handle": f"{BASE_URL}/query/service/result/123",
        "status": "success"
    }
    
    # Mock result response
    result_response = {
        "results": [{"name": "Test User"}],
        "status": "success"
    }

    responses.add(
        responses.POST,
        QUERY_ENDPOINT,
        json=initial_response,
        status=200
    )
    
    responses.add(
        responses.GET,
        f"{BASE_URL}/query/service/result/123",
        json=result_response,
        status=200
    )

    result = client.execute_query(
        "SELECT * FROM TestDataset;",
        mode="deferred"
    )
    assert result["status"] == "success"
    assert len(result["results"]) == 1

@responses.activate
def test_async_query_execution(client):
    """Test successful execution of async query."""
    # Mock initial response
    responses.add(
        responses.POST,
        QUERY_ENDPOINT,
        json={
            "requestID": "123",
            "handle": f"{BASE_URL}/query/service/status/123",
            "status": "running"
        },
        status=200
    )
    
    # Mock status check responses
    responses.add(
        responses.GET,
        f"{BASE_URL}/query/service/status/123",
        json={
            "status": "SUCCESS",
            "handle": f"{BASE_URL}/query/service/result/123"
        },
        status=200
    )
    
    # Mock final result
    responses.add(
        responses.GET,
        f"{BASE_URL}/query/service/result/123",
        json={
            "results": [{"data": "test"}],
            "status": "success"
        },
        status=200
    )

    result = client.execute_query(
        "SELECT * FROM LargeDataset;",
        mode="async"
    )
    assert result["status"] == "success"
    assert len(result["results"]) == 1

@responses.activate
def test_async_query_failure(client):
    """Test handling of async query failure."""
    # Mock initial response
    responses.add(
        responses.POST,
        QUERY_ENDPOINT,
        json={
            "requestID": "123",
            "handle": f"{BASE_URL}/query/service/status/123",
            "status": "running"
        },
        status=200
    )
    
    # Mock status check showing failure
    responses.add(
        responses.GET,
        f"{BASE_URL}/query/service/status/123",
        json={
            "status": "FAILED",
            "errors": ["Query execution failed: Out of memory"]
        },
        status=200
    )

    with pytest.raises(QueryExecutionError) as exc_info:
        client.execute_query(
            "SELECT * FROM VeryLargeDataset;",
            mode="async"
        )
    assert "Out of memory" in str(exc_info.value)

@responses.activate
def test_malformed_query(client):
    """Test handling of malformed query with missing semicolon."""
    error_response = {
        "requestID": "123",
        "status": "error",
        "errors": ["Syntax error: Expected semicolon at end of statement"]
    }
    
    responses.add(
        responses.POST,
        QUERY_ENDPOINT,
        json=error_response,
        status=200
    )

    with pytest.raises(QueryExecutionError) as exc_info:
        client.execute_query("SELECT VALUE 1 + 1")  # Missing semicolon
    assert "Syntax error" in str(exc_info.value)

@responses.activate
def test_dataverse_not_found(client):
    """Test handling of non-existent dataverse."""
    error_response = {
        "requestID": "123",
        "status": "error",
        "errors": ["Cannot find dataverse NonExistentDataverse"]
    }
    
    responses.add(
        responses.POST,
        QUERY_ENDPOINT,
        json=error_response,
        status=200
    )

    with pytest.raises(QueryExecutionError) as exc_info:
        client.execute_query(
            "USE NonExistentDataverse;",
            dataverse="NonExistentDataverse"
        )
    assert "Cannot find dataverse" in str(exc_info.value)

@responses.activate
def test_invalid_parameter_type(client):
    """Test handling of invalid parameter type."""
    # We don't need to add a mock response because the validation
    # should happen before the request is made
    with pytest.raises(TypeError) as exc_info:
        client.execute_query(123)  # Query should be string
    assert "Query statement must be a string" in str(exc_info.value)

def test_invalid_mode(client):
    """Test handling of invalid query mode."""
    with pytest.raises(ValueError) as exc_info:
        client.execute_query("SELECT VALUE 1;", mode="invalid_mode")
    assert "Invalid mode" in str(exc_info.value)
    assert "Must be one of: immediate, deferred, async" in str(exc_info.value)

@responses.activate
def test_context_manager():
    """Test client as context manager."""
    with AsterixDBHttpClient() as test_client:
        # Make a request to verify session is working
        responses.add(
            responses.POST,
            "http://localhost:19002/query/service",
            json={"status": "success", "results": [1]},
            status=200
        )
        result = test_client.execute_query("SELECT VALUE 1;")
        assert result["status"] == "success"
    
    # Try to make request after context exit to verify session is closed
    with pytest.raises(RuntimeError) as exc_info:
        test_client.execute_query("SELECT VALUE 1;")
    assert "Invalid or closed session" in str(exc_info.value)