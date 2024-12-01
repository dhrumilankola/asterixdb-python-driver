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
    
    def __post_init__(self):
        # Propagate parent from attribute to predicate
        self.parent = self.attribute.parent if self.attribute else None

    def __and__(self, other: 'AsterixPredicate') -> 'AsterixPredicate':
        """Support for AND operations between predicates."""
        return AsterixPredicate(
            attribute=None,  # Compound predicates don't directly reference an attribute
            operator="AND",
            value=None,
            is_compound=True,
            left_pred=self,
            right_pred=other
        )
        
    def __or__(self, other: 'AsterixPredicate') -> 'AsterixPredicate':
        """Support for OR operations between predicates."""
        return AsterixPredicate(
            attribute=None,  # Compound predicates don't directly reference an attribute
            operator="OR",
            value=None,
            is_compound=True,
            left_pred=self,
            right_pred=other
        )

    def update_alias(self, new_alias: str):
        """Update the alias used in the predicate."""
        if hasattr(self, 'parent') and self.parent:
            self.parent.query_builder.alias = new_alias
        if self.is_compound:
            if self.left_pred:
                self.left_pred.update_alias(new_alias)
            if self.right_pred:
                self.right_pred.update_alias(new_alias)
    
    def to_sql(self, alias: str) -> str:
        """Convert predicate to SQL string."""
        if self.is_compound:
            # Handle compound predicates (AND/OR)
            left = self.left_pred.to_sql(alias)
            right = self.right_pred.to_sql(alias)
            return f"({left} {self.operator} {right})"
        elif self.operator == "CONTAINS":
            # Use the correct alias based on which table the field belongs to
            correct_alias = alias
            if hasattr(self, 'parent') and self.parent:
                for join in self.parent.query_builder.joins:
                    if self.parent.dataset == join['right_table']:
                        correct_alias = join['alias_right']
                        break
            field_ref = f"{correct_alias}.{self.attribute.name}"
            return f"CONTAINS({field_ref}, '{self.value}')"
        else:
            # For other operators, also ensure we use the correct alias
            correct_alias = alias
            if hasattr(self, 'parent') and self.parent:
                for join in self.parent.query_builder.joins:
                    if self.parent.dataset == join['right_table']:
                        correct_alias = join['alias_right']
                        break
            field_ref = f"{correct_alias}.{self.attribute.name}"
            
            if isinstance(self.value, (str, datetime, date)):
                value = f"'{self.value}'"
            elif isinstance(self.value, (list, tuple)):
                value = f"({', '.join(repr(v) for v in self.value)})"
            else:
                value = str(self.value)
                
            return f"{field_ref} {self.operator} {value}"

        
class AsterixAttribute:
    """Represents a column in an AsterixDB dataset."""
    
    def __init__(self, name: str, parent: 'AsterixDataFrame'):
        self.name = name
        self.parent = parent
        
    def __eq__(self, other: Any) -> AsterixPredicate:
        return AsterixPredicate(
            attribute=self,
            operator="=",
            value=other,
            table_alias=self.table_alias  # Pass table alias
        )
        
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
    
    def contains(self, value: str) -> AsterixPredicate:
        """Create a CONTAINS predicate."""
        return AsterixPredicate(
            attribute=self,
            operator="CONTAINS",
            value=value,
            table_alias=self.table_alias  # Pass table alias
        )
    
    def split(self, delimiter: str) -> 'AsterixAttribute':
        """Split string field by delimiter."""
        return AsterixAttribute(f"split({self.name}, '{delimiter}')", self.parent)
