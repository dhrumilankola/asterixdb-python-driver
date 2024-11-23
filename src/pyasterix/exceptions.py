# exceptions.py
class Error(Exception):
    """Base class for all database exceptions."""
    pass

class DatabaseError(Error):
    """Exception raised for errors related to the database."""
    pass

class InterfaceError(Error):
    """Exception raised for errors related to the database interface."""
    pass

class NotSupportedError(DatabaseError):
    """Exception raised for unsupported operations."""
    pass
