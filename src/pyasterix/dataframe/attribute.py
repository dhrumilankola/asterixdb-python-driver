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
    _dataset: Optional[str] = None
    
    def __init__(self, attribute, operator, value, is_compound=False, left_pred=None, right_pred=None):
        self.attribute = attribute
        self.operator = operator
        self.value = value
        self.is_compound = is_compound
        self.left_pred = left_pred
        self.right_pred = right_pred
        
        # Get parent and dataset information from attribute if possible
        self.parent = self.attribute.parent if self.attribute else None
        self.dataset = self.parent.dataset if self.parent else None
        self._alias = None
        self._alias = None

    def __post_init__(self):
        # Propagate parent from attribute to predicate
        self.parent = self.attribute.parent if self.attribute else None
        if self.parent:
            self._dataset = self.parent.dataset

    def __and__(self, other):
        """Support for AND operations between predicates."""
        return AsterixPredicate(
            attribute=None,
            operator="AND",
            value=None,
            is_compound=True,
            left_pred=self,
            right_pred=other
        )

    def __or__(self, other):
        """Support for OR operations between predicates."""
        return AsterixPredicate(
            attribute=None,
            operator="OR",
            value=None,
            is_compound=True,
            left_pred=self,
            right_pred=other
        )

    def __invert__(self):
        """Support for NOT operations."""
        return AsterixPredicate(
            attribute=None,
            operator="NOT",
            value=self,
            is_compound=True
        )

    def update_alias(self, new_alias):
        """Update the alias for this predicate and propagate to compound predicates."""
        self._alias = new_alias
        
        # Update alias for compound predicates
        if self.is_compound:
            if self.left_pred:
                self.left_pred.update_alias(new_alias)
            if self.right_pred:
                self.right_pred.update_alias(new_alias)

    def get_alias(self):
        """Get the effective alias for the predicate."""
        # First check if explicit alias was set
        if self._alias:
            return self._alias
            
        # If no explicit alias, try to determine from parent/context
        if self.parent and hasattr(self.parent, 'query_builder'):
            # Check if this dataset is involved in any joins
            for join in self.parent.query_builder.joins:
                if self.dataset == join['right_table']:
                    return join['alias_right']
                elif self.dataset == self.parent.query_builder.from_dataset:
                    return join['alias_left']
            
            # Default to query builder's alias if no join matches
            return self.parent.query_builder.alias
            
        # Default fallback alias
        return "t"

    def to_sql(self):
        """Convert predicate to SQL string."""
        if self.is_compound:
            if self.operator == "NOT":
                # Handle NOT operation
                if isinstance(self.value, AsterixPredicate):
                    inner_condition = self.value.to_sql()
                    return f"NOT ({inner_condition})"
                raise ValueError(f"Invalid value for NOT operation: {self.value}")
            
            # Handle AND/OR operations
            left = self.left_pred.to_sql()
            right = self.right_pred.to_sql()
            return f"({left}) {self.operator} ({right})"
        
        # Get the correct alias to use
        alias = self.get_alias()
        
        # Handle simple predicates
        field_ref = f"{alias}.{self.attribute.name}" if self.attribute else ""
        
        # Special handling for different operators and value types
        if self.operator in ("IS NULL", "IS NOT NULL"):
            return f"{field_ref} {self.operator}"
        else:
            formatted_value = self._format_value(self.value)
            return f"{field_ref} {self.operator} {formatted_value}"
        
    def _format_value(self, value):
        """Format a value appropriately for SQL++."""
        if value is None:
            return "NULL"
        elif isinstance(value, str):
            # Check if it's a datetime function call
            if value.startswith("datetime(") or value.startswith("date("):
                return value  # Don't quote - it's a function call
            return f"'{value}'"  # Regular string - quote it
        elif isinstance(value, datetime):
            # Format datetime objects correctly for AsterixDB
            return f"datetime('{value.isoformat()}')"
        elif isinstance(value, date):
            return f"date('{value.isoformat()}')"
        elif isinstance(value, (list, tuple)):
            values_str = ", ".join(self._format_value(v) for v in value)
            if self.operator == "IN":
                return f"({values_str})"
            elif self.operator == "BETWEEN":
                if len(value) == 2:
                    return f"{self._format_value(value[0])} AND {self._format_value(value[1])}"
                else:
                    raise ValueError("BETWEEN operator requires exactly two values")
            return f"({values_str})"
        else:
            return str(value)
    





class AsterixAttribute:
    """Represents a column in an AsterixDB dataset."""
    
    def __init__(self, name, parent):
        self.name = name
        self.parent = parent

    def _get_effective_alias(self):
        """Get the effective alias for this attribute based on context."""
        if not self.parent:
            return "t"  # Default fallback
            
        # Check if this attribute's parent has a query builder
        if hasattr(self.parent, 'query_builder'):
            qb = self.parent.query_builder
            
            # If this dataset is in a join, find the right alias
            for join in qb.joins:
                if self.parent.dataset == join['right_table']:
                    return join['alias_right']
                elif self.parent.dataset == qb.from_dataset:
                    return join['alias_left']
            return qb.alias
        return "t"

    def __eq__(self, other):
        return AsterixPredicate(self, "=", other)
        
    def __gt__(self, other):
        return AsterixPredicate(self, ">", other)
        
    def __lt__(self, other):
        return AsterixPredicate(self, "<", other)
        
    def __ge__(self, other):
        return AsterixPredicate(self, ">=", other)
        
    def __le__(self, other):
        return AsterixPredicate(self, "<=", other)
        
    def __ne__(self, other):
        return AsterixPredicate(self, "!=", other)
        
    def like(self, pattern):
        """Create a LIKE predicate."""
        return AsterixPredicate(self, "LIKE", pattern)
        
    def in_(self, values):
        """Create an IN predicate."""
        return AsterixPredicate(self, "IN", values)
        
    def is_null(self):
        """Create an IS NULL predicate."""
        return AsterixPredicate(self, "IS NULL", None)
        
    def is_not_null(self):
        """Create an IS NOT NULL predicate."""
        return AsterixPredicate(self, "IS NOT NULL", None)

    def between(self, value1, value2):
        """Create a BETWEEN predicate."""
        return AsterixPredicate(self, "BETWEEN", (value1, value2))
        
    def contains(self, value):
        """Create a CONTAINS predicate."""
        return AsterixPredicate(self, "CONTAINS", value)
    
    def split(self, delimiter: str) -> 'AsterixAttribute':
        """Split string field by delimiter."""
        return AsterixAttribute(f"split({self.name}, '{delimiter}')", self.parent)
