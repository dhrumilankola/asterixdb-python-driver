import requests
from urllib.parse import urljoin
from typing import Optional, Dict, Any
from .exceptions import NotSupportedError, InterfaceError
from .cursor import Cursor
from .observability import ObservabilityConfig, ObservabilityManager, initialize_observability
import logging


def connect(
    host: str = "localhost",
    port: int = 19002,
    timeout: int = 30,
    max_retries: int = 3,
    retry_delay: float = 0.1,
    observability_config: Optional[ObservabilityConfig] = None,
    trace_context: Optional[Dict[str, str]] = None
):
    """
    Create a connection to AsterixDB.
    
    Args:
        host: AsterixDB hostname
        port: AsterixDB port
        timeout: Request timeout in seconds
        max_retries: Maximum number of retry attempts
        retry_delay: Initial delay between retries (in seconds)
        observability_config: Configuration for observability features
        trace_context: Optional trace context from upstream service
        
    Returns:
        Connection instance
    """
    base_url = f"http://{host}:{port}"
    return Connection(
        base_url=base_url,
        timeout=timeout,
        max_retries=max_retries,
        retry_delay=retry_delay,
        observability_config=observability_config,
        trace_context=trace_context
    )

# Configure logging
logger = logging.getLogger(__name__)

class Connection:
    """
    Represents a connection to the AsterixDB database.
    Manages the HTTP session and provides access to Cursor objects for query execution.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:19002",
        timeout: int = 30,
        max_retries: int = 3,
        retry_delay: float = 0.1,
        observability_config: Optional[ObservabilityConfig] = None,
        trace_context: Optional[Dict[str, str]] = None
    ):
        """
        Initialize a Connection instance.

        Args:
            base_url: Base URL of the AsterixDB instance.
            timeout: Request timeout in seconds.
            max_retries: Maximum number of retry attempts.
            retry_delay: Initial delay between retries (in seconds).
            observability_config: Configuration for observability features.
            trace_context: Optional trace context from upstream service for distributed tracing.
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._closed = False

        # HTTP session without default headers - we'll set them per request
        self.session = requests.Session()
        
        # Initialize observability
        self.observability = initialize_observability(observability_config)
        
        # Set trace context if provided
        self.trace_context = trace_context
        if self.observability and trace_context:
            self.observability.set_trace_context(trace_context)
        
        # Get structured logger
        if self.observability:
            self.logger = self.observability.get_logger("pyasterix.connection")
        else:
            self.logger = logging.getLogger("pyasterix.connection")
        
        # Track connection creation
        if self.observability:
            self.observability.set_active_connections(1, service="asterixdb-client")
        
        self.logger.info("Connection initialized successfully", extra={
            "base_url": base_url,
            "timeout": timeout,
            "max_retries": max_retries,
            "retry_delay": retry_delay,
            "observability_enabled": self.observability is not None,
            "trace_context_provided": trace_context is not None
        })

    def get_trace_context(self) -> Optional[Dict[str, str]]:
        """
        Get current trace context for propagation to other services.
        
        Returns:
            Dictionary containing trace context headers or None if tracing disabled
        """
        if self.observability:
            return self.observability.get_current_trace_context()
        return None
    
    def get_span_context(self) -> Optional[Dict[str, str]]:
        """
        Get current span context information.
        
        Returns:
            Dictionary with trace_id, span_id, and correlation_id if available
        """
        if self.observability:
            return self.observability.get_current_span_context()
        return None

    def cursor(self) -> Cursor:
        """
        Create a new Cursor object for executing queries.

        Returns:
            Cursor: A new Cursor instance.
        
        Raises:
            InterfaceError: If the connection is closed.
        """
        if self._closed:
            raise InterfaceError("Cannot create a cursor on a closed connection.")
        return Cursor(self, observability=self.observability)

    def commit(self):
        """
        Commit the current transaction.
        Not supported by AsterixDB.
        """
        raise NotSupportedError("AsterixDB does not support transactions.")

    def rollback(self):
        """
        Rollback the current transaction.
        Not supported by AsterixDB.
        """
        raise NotSupportedError("AsterixDB does not support transactions.")

    def close(self):
        """
        Close the connection and the HTTP session.
        """
        if not self._closed:
            # Record connection closure in metrics
            if self.observability:
                self.observability.set_active_connections(0, service="asterixdb-client")
            
            self.session.close()
            self._closed = True
            
            self.logger.info("Connection closed successfully", extra={
                "base_url": self.base_url,
                "connection_id": id(self),
                "session_closed": True
            })
        else:
            self.logger.warning("Attempted to close already closed connection", extra={
                "base_url": self.base_url,
                "connection_id": id(self),
                "was_already_closed": True
            })

    def __enter__(self):
        """
        Support the context manager protocol for using connections with `with` statements.
        """
        if self._closed:
            raise InterfaceError("Cannot use a closed connection.")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Close the connection when exiting a `with` block.
        """
        self.close()
