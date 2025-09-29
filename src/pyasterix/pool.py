"""
Advanced Connection Pool for AsterixDB with intelligent management and async support.

This module provides enterprise-grade connection pooling that integrates seamlessly
with the existing PyAsterix architecture while adding production-ready features:
- Connection lifecycle management with validation
- Enhanced async query handling 
- Comprehensive observability integration
- Intelligent connection reuse and cleanup
"""

import threading
import time
import queue
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Union
from dataclasses import dataclass, field
from contextlib import contextmanager
from urllib.parse import urljoin

from .connection import Connection
from .exceptions import (
    DatabaseError, NetworkError, InterfaceError, PoolExhaustedError,
    PoolShutdownError, ConnectionValidationError, TimeoutError,
    ErrorMapper, AsyncErrorMapper
)
from .observability import ObservabilityManager, ObservabilityConfig


@dataclass
class PoolConfig:
    """Configuration for connection pool behavior."""
    # Pool sizing
    max_pool_size: int = 10
    min_pool_size: int = 2
    
    # Connection lifecycle
    max_lifetime: timedelta = timedelta(minutes=30)
    idle_timeout: timedelta = timedelta(minutes=5)
    
    # Timeout configuration (enhanced from single timeout)
    connection_timeout: int = 10  # TCP connection establishment
    query_timeout: int = 60       # Query execution  
    pool_wait_timeout: int = 30   # Wait for available connection
    health_check_timeout: int = 5  # Connection validation
    
    # Health and validation
    validate_on_borrow: bool = True
    validate_on_return: bool = False
    health_check_query: str = "SELECT VALUE 1"
    
    # Async query optimization
    async_poll_interval: float = 0.5  # Seconds between status checks
    async_max_polls: int = 120        # Max polls before timeout (60s default)
    
    # Cleanup and maintenance
    cleanup_interval: int = 60        # Seconds between cleanup runs
    enable_background_cleanup: bool = True


class PooledConnection:
    """Wrapper for pooled connections with lifecycle tracking."""
    
    def __init__(self, connection: Connection, pool: 'AsterixConnectionPool'):
        self.connection = connection
        self.pool = pool
        self.created_at = datetime.now()
        self.last_used = datetime.now()
        self.is_valid = True
        self.in_use = False
        self.validation_failures = 0
        self._lock = threading.Lock()
    
    @property
    def age(self) -> timedelta:
        """Get the age of this connection."""
        return datetime.now() - self.created_at
    
    @property
    def idle_time(self) -> timedelta:
        """Get how long this connection has been idle."""
        return datetime.now() - self.last_used
    
    def mark_used(self):
        """Mark connection as recently used."""
        with self._lock:
            self.last_used = datetime.now()
            self.in_use = True
    
    def mark_returned(self):
        """Mark connection as returned to pool."""
        with self._lock:
            self.last_used = datetime.now()
            self.in_use = False
    
    def mark_invalid(self):
        """Mark connection as invalid."""
        with self._lock:
            self.is_valid = False
    
    def should_expire(self, config: PoolConfig) -> bool:
        """Check if connection should be expired."""
        return (self.age > config.max_lifetime or 
                self.idle_time > config.idle_timeout or
                not self.is_valid)
    
    def validate(self, config: PoolConfig) -> bool:
        """Validate connection health."""
        if not self.is_valid:
            return False
        
        try:
            # Use lightweight health check
            cursor = self.connection.cursor()
            cursor.execute(
                config.health_check_query,
                mode="immediate",
                readonly=True
            )
            result = cursor.fetchone()
            cursor.close()
            
            # Reset failure count on success
            with self._lock:
                self.validation_failures = 0
            
            return result is not None
            
        except Exception as e:
            with self._lock:
                self.validation_failures += 1
                if self.validation_failures >= 3:
                    self.is_valid = False
            
            # Log validation failure
            if self.connection.observability:
                self.connection.observability.record_connection_error(
                    error_type="validation_failed",
                    validation_failures=self.validation_failures
                )
            
            return False
    
    def close(self):
        """Close the underlying connection."""
        try:
            self.connection.close()
        except Exception:
            pass  # Ignore errors during cleanup
        finally:
            self.is_valid = False


class AsterixConnectionPool:
    """
    Enterprise-grade connection pool for AsterixDB with intelligent management.
    
    Features:
    - Automatic connection lifecycle management
    - Health checking and validation
    - Enhanced async query support with connection reuse
    - Comprehensive observability integration
    - Background cleanup and maintenance
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 19002,
        config: Optional[PoolConfig] = None,
        observability_config: Optional[ObservabilityConfig] = None,
        **connection_kwargs
    ):
        """
        Initialize connection pool.
        
        Args:
            host: AsterixDB hostname
            port: AsterixDB port  
            config: Pool configuration
            observability_config: Observability configuration
            **connection_kwargs: Additional arguments for Connection instances
        """
        self.host = host
        self.port = port
        self.config = config or PoolConfig()
        self.observability_config = observability_config
        self.connection_kwargs = connection_kwargs
        
        # Pool state
        self._available = queue.Queue(maxsize=self.config.max_pool_size)
        self._all_connections: Dict[int, PooledConnection] = {}
        self._lock = threading.RLock()
        self._shutdown = False
        
        # Observability
        self.observability = None
        if observability_config:
            from .observability import initialize_observability
            self.observability = initialize_observability(observability_config)
        
        # Logging
        if self.observability:
            self.logger = self.observability.get_logger("pyasterix.pool")
        else:
            self.logger = logging.getLogger("pyasterix.pool")
        
        # Background cleanup thread
        self._cleanup_thread = None
        if self.config.enable_background_cleanup:
            self._start_cleanup_thread()
        
        # Initialize minimum connections
        self._initialize_min_connections()
        
        self.logger.info("Connection pool initialized", extra={
            "max_pool_size": self.config.max_pool_size,
            "min_pool_size": self.config.min_pool_size,
            "host": host,
            "port": port,
            "validate_on_borrow": self.config.validate_on_borrow
        })
    
    def _initialize_min_connections(self):
        """Initialize minimum number of connections."""
        for i in range(self.config.min_pool_size):
            try:
                conn = self._create_connection()
                pooled_conn = PooledConnection(conn, self)
                
                with self._lock:
                    self._all_connections[id(pooled_conn)] = pooled_conn
                    self._available.put(pooled_conn, block=False)
                
            except Exception as e:
                self.logger.warning(f"Failed to create initial connection {i+1}", extra={
                    "error": str(e),
                    "connection_index": i
                })
    
    def _create_connection(self) -> Connection:
        """Create a new connection instance."""
        # Enhanced timeout configuration
        connection_kwargs = {
            **self.connection_kwargs,
            'timeout': self.config.query_timeout,  # Use query timeout as default
            'observability_config': self.observability_config
        }
        
        base_url = f"http://{self.host}:{self.port}"
        connection = Connection(base_url=base_url, **connection_kwargs)
        
        # Update pool metrics
        if self.observability:
            self.observability.set_active_connections(
                len(self._all_connections) + 1,
                service="asterixdb-pool"
            )
        
        return connection
    
    def _start_cleanup_thread(self):
        """Start background cleanup thread."""
        def cleanup_worker():
            while not self._shutdown:
                try:
                    self._cleanup_expired_connections()
                    time.sleep(self.config.cleanup_interval)
                except Exception as e:
                    self.logger.error("Error in cleanup thread", extra={
                        "error": str(e),
                        "error_type": type(e).__name__
                    })
                    time.sleep(5)  # Brief pause on error
        
        self._cleanup_thread = threading.Thread(
            target=cleanup_worker,
            daemon=True,
            name="asterix-pool-cleanup"
        )
        self._cleanup_thread.start()
    
    def _cleanup_expired_connections(self):
        """Clean up expired and invalid connections."""
        expired_connections = []
        
        with self._lock:
            # Find expired connections
            for conn_id, pooled_conn in list(self._all_connections.items()):
                if pooled_conn.should_expire(self.config) and not pooled_conn.in_use:
                    expired_connections.append((conn_id, pooled_conn))
        
        # Close expired connections
        for conn_id, pooled_conn in expired_connections:
            try:
                pooled_conn.close()
                
                with self._lock:
                    self._all_connections.pop(conn_id, None)
                    # Remove from available queue (best effort)
                    temp_queue = queue.Queue()
                    while not self._available.empty():
                        try:
                            conn = self._available.get_nowait()
                            if id(conn) != conn_id:
                                temp_queue.put(conn)
                        except queue.Empty:
                            break
                    
                    # Put back non-expired connections
                    while not temp_queue.empty():
                        self._available.put(temp_queue.get())
                
                self.logger.debug("Expired connection cleaned up", extra={
                    "connection_id": conn_id,
                    "age_seconds": pooled_conn.age.total_seconds(),
                    "idle_seconds": pooled_conn.idle_time.total_seconds()
                })
                
            except Exception as e:
                self.logger.warning("Error cleaning up connection", extra={
                    "connection_id": conn_id,
                    "error": str(e)
                })
        
        # Update metrics
        if self.observability and expired_connections:
            self.observability.set_active_connections(
                len(self._all_connections),
                service="asterixdb-pool"
            )
    
    @contextmanager
    def get_connection(self, timeout: Optional[float] = None):
        """
        Get a connection from the pool.
        
        Args:
            timeout: Timeout for acquiring connection (uses pool_wait_timeout if None)
            
        Yields:
            Connection: A validated database connection
            
        Raises:
            ConnectionError: If unable to acquire connection within timeout
        """
        if self._shutdown:
            raise PoolShutdownError("Connection pool is shut down")
        
        timeout = timeout or self.config.pool_wait_timeout
        start_time = time.time()
        pooled_conn = None
        
        try:
            # Try to get connection from pool
            while time.time() - start_time < timeout:
                try:
                    pooled_conn = self._available.get(timeout=min(1.0, timeout))
                    break
                except queue.Empty:
                    # Try to create new connection if below max size
                    with self._lock:
                        if len(self._all_connections) < self.config.max_pool_size:
                            try:
                                conn = self._create_connection()
                                pooled_conn = PooledConnection(conn, self)
                                self._all_connections[id(pooled_conn)] = pooled_conn
                                break
                            except Exception as e:
                                self.logger.warning("Failed to create new connection", extra={
                                    "error": str(e),
                                    "pool_size": len(self._all_connections)
                                })
            
            if pooled_conn is None:
                raise PoolExhaustedError(
                    f"Unable to acquire connection within {timeout}s",
                    pool_size=len(self._all_connections),
                    context={'timeout': timeout, 'max_pool_size': self.config.max_pool_size}
                )
            
            # Validate connection if required
            if self.config.validate_on_borrow:
                if not pooled_conn.validate(self.config):
                    # Connection is invalid, try to get another one
                    pooled_conn.close()
                    with self._lock:
                        self._all_connections.pop(id(pooled_conn), None)
                    
                    # Recursive call with reduced timeout
                    remaining_timeout = timeout - (time.time() - start_time)
                    if remaining_timeout > 0:
                        with self.get_connection(remaining_timeout) as conn:
                            yield conn
                            return
                    else:
                        raise ConnectionValidationError(
                            "Connection validation failed and timeout exceeded",
                            context={'timeout': timeout, 'remaining_timeout': 0}
                        )
            
            # Mark as in use
            pooled_conn.mark_used()
            
            # Track pool metrics
            if self.observability:
                self.observability.increment_query_count(
                    pool_status="connection_borrowed",
                    pool_size=len(self._all_connections),
                    available_connections=self._available.qsize()
                )
            
            self.logger.debug("Connection borrowed from pool", extra={
                "connection_id": id(pooled_conn),
                "pool_size": len(self._all_connections),
                "available_connections": self._available.qsize(),
                "connection_age_seconds": pooled_conn.age.total_seconds()
            })
            
            yield pooled_conn.connection
            
        finally:
            # Return connection to pool
            if pooled_conn and pooled_conn.is_valid:
                try:
                    # Validate on return if configured
                    if self.config.validate_on_return:
                        if not pooled_conn.validate(self.config):
                            pooled_conn.close()
                            with self._lock:
                                self._all_connections.pop(id(pooled_conn), None)
                            return
                    
                    pooled_conn.mark_returned()
                    self._available.put(pooled_conn, block=False)
                    
                    self.logger.debug("Connection returned to pool", extra={
                        "connection_id": id(pooled_conn),
                        "pool_size": len(self._all_connections),
                        "available_connections": self._available.qsize()
                    })
                    
                except queue.Full:
                    # Pool is full, close this connection
                    pooled_conn.close()
                    with self._lock:
                        self._all_connections.pop(id(pooled_conn), None)
                    
                except Exception as e:
                    self.logger.warning("Error returning connection to pool", extra={
                        "connection_id": id(pooled_conn),
                        "error": str(e)
                    })
                    pooled_conn.close()
                    with self._lock:
                        self._all_connections.pop(id(pooled_conn), None)
            
            elif pooled_conn:
                # Connection is invalid, clean it up
                pooled_conn.close()
                with self._lock:
                    self._all_connections.pop(id(pooled_conn), None)
    
    def execute_query(
        self,
        query: str,
        params: Optional[Any] = None,
        mode: str = "immediate",
        pretty: bool = False,
        readonly: bool = False,
        connection_timeout: Optional[float] = None
    ) -> Any:
        """
        Execute query using pooled connection with enhanced async support.
        
        Args:
            query: SQL++ query to execute
            params: Query parameters
            mode: Execution mode (immediate, deferred, async)
            pretty: Format output
            readonly: Read-only mode
            connection_timeout: Timeout for acquiring connection
            
        Returns:
            Query results or async handle info
        """
        with self.get_connection(connection_timeout) as connection:
            cursor = connection.cursor()
            
            try:
                # Enhanced async handling with pool-aware timeout
                if mode == "async":
                    cursor.execute(query, params, mode, pretty, readonly)
                    
                    # For async queries, handle polling with pool-optimized intervals
                    if hasattr(cursor, 'results') and isinstance(cursor.results, dict):
                        if "handle" in cursor.results:
                            return self._handle_async_query_pooled(
                                cursor.results,
                                connection
                            )
                    
                    return cursor.fetchall()
                else:
                    # Immediate and deferred modes
                    cursor.execute(query, params, mode, pretty, readonly)
                    return cursor.fetchall()
                    
            finally:
                cursor.close()
    
    def _handle_async_query_pooled(
        self,
        initial_response: Dict[str, Any],
        connection: Connection
    ) -> Any:
        """
        Handle async query with pool-optimized polling.
        
        This enhances the existing async implementation by using pool-aware
        timeouts and optimized polling intervals.
        """
        handle = initial_response.get("handle")
        if not handle:
            raise DatabaseError("Async query did not return a handle.")
        
        # Use pool config for polling optimization
        poll_interval = self.config.async_poll_interval
        max_polls = self.config.async_max_polls
        attempts = 0
        
        # Create span for pool-aware async handling
        span = None
        if self.observability:
            span = self.observability.create_database_span(
                operation="query.async.pool",
                handle=handle,
                max_polls=max_polls,
                poll_interval=poll_interval
            )
        
        try:
            with span if span else self._noop_context():
                status_url = urljoin(connection.base_url, handle)
                
                while attempts < max_polls:
                    time.sleep(poll_interval)
                    
                    try:
                        status_response = connection.session.get(
                            status_url,
                            timeout=self.config.health_check_timeout
                        )
                        status_data = status_response.json()
                        
                        if status_data.get("status") == "success":
                            # Get result using the result handle
                            result_handle = status_data.get("handle")
                            if result_handle:
                                result_url = urljoin(connection.base_url, result_handle)
                                result_response = connection.session.get(
                                    result_url,
                                    timeout=self.config.query_timeout
                                )
                                return result_response.json()
                            else:
                                return status_data.get("results", [])
                        
                        elif status_data.get("status") in ["failed", "fatal", "timeout"]:
                            raise AsyncErrorMapper.from_async_status(status_data, handle)
                        
                        # Still running, continue polling
                        attempts += 1
                        
                    except Exception as e:
                        if self.observability:
                            self.observability.record_connection_error(
                                error_type="async_poll_failed",
                                attempt=attempts
                            )
                        raise ErrorMapper.from_network_error(e, {'operation': 'async_poll', 'attempt': attempts})
                
                # Exceeded max polls
                raise TimeoutError(
                    f"Async query timeout after {max_polls} polls",
                    timeout_duration=max_polls * poll_interval,
                    operation_type="async_query_polling",
                    context={'handle': handle, 'max_polls': max_polls, 'poll_interval': poll_interval}
                )
                
        except Exception as e:
            if span and self.observability:
                self.observability.record_span_exception(span, e)
            raise
    
    def _noop_context(self):
        """No-operation context manager for when tracing is disabled."""
        class NoOpContext:
            def __enter__(self):
                return self
            def __exit__(self, exc_type, exc_val, exc_tb):
                pass
        return NoOpContext()
    
    def get_pool_stats(self) -> Dict[str, Any]:
        """Get current pool statistics."""
        with self._lock:
            total_connections = len(self._all_connections)
            available_connections = self._available.qsize()
            active_connections = total_connections - available_connections
            
            # Calculate connection ages
            ages = [conn.age.total_seconds() for conn in self._all_connections.values()]
            idle_times = [conn.idle_time.total_seconds() for conn in self._all_connections.values()]
            
            stats = {
                "total_connections": total_connections,
                "available_connections": available_connections,
                "active_connections": active_connections,
                "max_pool_size": self.config.max_pool_size,
                "min_pool_size": self.config.min_pool_size,
                "avg_connection_age": sum(ages) / len(ages) if ages else 0,
                "max_connection_age": max(ages) if ages else 0,
                "avg_idle_time": sum(idle_times) / len(idle_times) if idle_times else 0,
                "max_idle_time": max(idle_times) if idle_times else 0,
                "pool_utilization": active_connections / self.config.max_pool_size if self.config.max_pool_size > 0 else 0
            }
            
            return stats
    
    def health_check(self, deep: bool = False) -> Dict[str, Any]:
        """
        Perform health check on the pool.
        
        Args:
            deep: If True, validates all connections; if False, just checks pool state
            
        Returns:
            Health status information
        """
        try:
            stats = self.get_pool_stats()
            
            health_status = {
                "healthy": True,
                "timestamp": datetime.now().isoformat(),
                "pool_stats": stats,
                "issues": []
            }
            
            # Check pool state
            if stats["total_connections"] == 0:
                health_status["healthy"] = False
                health_status["issues"].append("No connections in pool")
            
            if stats["available_connections"] == 0 and stats["total_connections"] < self.config.max_pool_size:
                health_status["issues"].append("No available connections but pool not at max size")
            
            # Deep health check - validate all connections
            if deep:
                with self._lock:
                    valid_connections = 0
                    for pooled_conn in self._all_connections.values():
                        if not pooled_conn.in_use and pooled_conn.validate(self.config):
                            valid_connections += 1
                    
                    health_status["valid_connections"] = valid_connections
                    health_status["invalid_connections"] = stats["available_connections"] - valid_connections
                    
                    if valid_connections == 0:
                        health_status["healthy"] = False
                        health_status["issues"].append("No valid connections available")
            
            return health_status
            
        except Exception as e:
            return {
                "healthy": False,
                "timestamp": datetime.now().isoformat(),
                "error": str(e),
                "error_type": type(e).__name__
            }
    
    def shutdown(self, timeout: float = 30.0):
        """
        Gracefully shutdown the connection pool.
        
        Args:
            timeout: Maximum time to wait for shutdown
        """
        self._shutdown = True
        
        # Stop cleanup thread
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=5.0)
        
        # Close all connections
        start_time = time.time()
        with self._lock:
            for pooled_conn in list(self._all_connections.values()):
                try:
                    pooled_conn.close()
                except Exception:
                    pass
                
                if time.time() - start_time > timeout:
                    break
            
            self._all_connections.clear()
        
        # Clear available queue
        while not self._available.empty():
            try:
                self._available.get_nowait()
            except queue.Empty:
                break
        
        self.logger.info("Connection pool shutdown completed", extra={
            "shutdown_duration": time.time() - start_time
        })
    
    def __enter__(self):
        """Support context manager protocol."""
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        """Clean shutdown on context exit."""
        self.shutdown()


# Enhanced connect function with pool support
def create_pool(
    host: str = "localhost",
    port: int = 19002,
    pool_config: Optional[PoolConfig] = None,
    observability_config: Optional[ObservabilityConfig] = None,
    **connection_kwargs
) -> AsterixConnectionPool:
    """
    Create a connection pool for AsterixDB.
    
    Args:
        host: AsterixDB hostname
        port: AsterixDB port
        pool_config: Pool configuration
        observability_config: Observability configuration
        **connection_kwargs: Additional connection parameters
        
    Returns:
        Configured connection pool
    """
    return AsterixConnectionPool(
        host=host,
        port=port,
        config=pool_config,
        observability_config=observability_config,
        **connection_kwargs
    )
