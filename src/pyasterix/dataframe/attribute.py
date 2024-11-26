from typing import Union, List, Any, Dict, Optional
from datetime import datetime, date
from dataclasses import dataclass

@dataclass
class AsterixPredicate:
    """Represents a condition/predicate in AsterixDB query."""
    attribute: 'AsterixAttribute'
    operator: str
    value: Any
    is_compound: bool = False
    left_pred: Optional['AsterixPredicate'] = None
    right_pred: Optional['AsterixPredicate'] = None
    
    def __and__(self, other: 'AsterixPredicate') -> 'AsterixPredicate':
        """Support for AND operations between predicates."""
        return AsterixPredicate(
            attribute=self.attribute,
            operator="AND",
            value=None,
            is_compound=True,
            left_pred=self,
            right_pred=other
        )
        
    def __or__(self, other: 'AsterixPredicate') -> 'AsterixPredicate':
        """Support for OR operations between predicates."""
        return AsterixPredicate(
            attribute=self.attribute,
            operator="OR",
            value=None,
            is_compound=True,
            left_pred=self,
            right_pred=other
        )
        
    def to_sql(self, alias: str) -> str:
        """Convert predicate to SQL string."""
        if self.is_compound:
            # Handle compound predicates (AND/OR)
            left = self.left_pred.to_sql(alias)
            right = self.right_pred.to_sql(alias)
            return f"({left} {self.operator} {right})"
        elif self.operator == "BETWEEN":
            # Special case for BETWEEN: value is a tuple (value1, value2)
            value1, value2 = self.value
            return f"{alias}.{self.attribute.name} BETWEEN {value1} AND {value2}"
        elif self.operator in ("IS NULL", "IS NOT NULL"):
            return f"{alias}.{self.attribute.name} {self.operator}"
        else:
            # Handle basic predicates
            if isinstance(self.value, (str, datetime, date)):
                value = f"'{self.value}'"
            elif isinstance(self.value, (list, tuple)):
                value = f"({', '.join(repr(v) for v in self.value)})"
            else:
                value = str(self.value)
            return f"{alias}.{self.attribute.name} {self.operator} {value}"
        
    


class AsterixAttribute:
    """Represents a column in an AsterixDB dataset."""
    
    def __init__(self, name: str, parent: 'AsterixDataFrame'):
        self.name = name
        self.parent = parent
        
    def __eq__(self, other: Any) -> AsterixPredicate:
        return AsterixPredicate(self, "=", other)
        
    def __gt__(self, other: Any) -> AsterixPredicate:
        return AsterixPredicate(self, ">", other)
        
    def __lt__(self, other: Any) -> AsterixPredicate:
        return AsterixPredicate(self, "<", other)
        
    def __ge__(self, other: Any) -> AsterixPredicate:
        return AsterixPredicate(self, ">=", other)
        
    def __le__(self, other: Any) -> AsterixPredicate:
        return AsterixPredicate(self, "<=", other)
        
    def __ne__(self, other: Any) -> AsterixPredicate:
        return AsterixPredicate(self, "!=", other)
        
    def like(self, pattern: str) -> AsterixPredicate:
        """Create a LIKE predicate."""
        return AsterixPredicate(self, "LIKE", pattern)
        
    def in_(self, values: list) -> AsterixPredicate:
        """Create an IN predicate."""
        return AsterixPredicate(self, "IN", values)
        
    def is_null(self) -> AsterixPredicate:
        """Create an IS NULL predicate."""
        return AsterixPredicate(self, "IS NULL", None)
        
    def is_not_null(self) -> AsterixPredicate:
        """Create an IS NOT NULL predicate."""
        return AsterixPredicate(self, "IS NOT NULL", None)

    def between(self, value1: Any, value2: Any) -> AsterixPredicate:
        """Create a BETWEEN predicate."""
        return AsterixPredicate(self, "BETWEEN", (value1, value2))
