class AsterixError(Exception):
    """Base exception class for AsterixDB client errors."""
    pass

class Warning(Exception):
    """Exception raised for important warnings."""
    pass

class Error(Exception):
    """Base class of all other error exceptions."""
    pass

# DB API standard exceptions
class InterfaceError(Error):
    """Database interface error."""
    pass

class DatabaseError(Error):
    """Database operation error."""
    pass

class DataError(DatabaseError):
    """Error in processed data."""
    pass

class OperationalError(DatabaseError):
    """Error related to database's operation."""
    pass

class IntegrityError(DatabaseError):
    """Data integrity error."""
    pass

class InternalError(DatabaseError):
    """Internal database error."""
    pass

class ProgrammingError(DatabaseError):
    """SQL programming error."""
    pass

class NotSupportedError(DatabaseError):
    """Feature not supported error."""
    pass

# Custom AsterixDB specific exceptions
class ConnectionError(DatabaseError):
    """Raised when connection fails."""
    pass

class QueryError(DatabaseError):
    """Raised when query execution fails."""
    pass

class ValidationError(Error):
    """Raised when input validation fails."""
    pass

class TypeMappingError(Error):
    """Raised when type mapping between Python and AsterixDB fails."""
    pass

class QueryBuildError(ProgrammingError):
    """Raised when query building fails."""
    pass