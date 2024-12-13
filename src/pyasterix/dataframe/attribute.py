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
    
    def __post_init__(self):
        # Propagate parent from attribute to predicate
        self.parent = self.attribute.parent if self.attribute else None
        if self.parent:
            self._dataset = self.parent.dataset

    def __and__(self, other: 'AsterixPredicate') -> 'AsterixPredicate':
        """Support for AND operations between predicates."""
        # Create compound predicate
        compound = AsterixPredicate(
            attribute=None,
            operator="AND",
            value=None,
            is_compound=True,
            left_pred=self,
            right_pred=other
        )
        
        # If either predicate has a parent, propagate it
        if hasattr(self, 'parent') and self.parent:
            compound.parent = self.parent
        elif hasattr(other, 'parent') and other.parent:
            compound.parent = other.parent
            
        return compound

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

    def get_correct_alias(self) -> str:
        """Get the correct alias based on the dataset"""
        if self._dataset and 'Businesses' in self._dataset:
            return 'b'
        elif self._dataset and 'Reviews' in self._dataset:
            return 'r'
        if hasattr(self, 'parent') and self.parent:
            if hasattr(self.parent, 'query_builder'):
                for join in self.parent.query_builder.joins:
                    if self.parent.dataset == join['right_table']:
                        return join['alias_right']
                    elif self.parent.dataset == self.parent.query_builder.from_dataset:
                        return join['alias_left']
        return 't'  # Default alias
   
    def to_sql(self, alias: str) -> str:
        """Convert predicate to SQL string."""
        if self.is_compound:
            left = self.left_pred.to_sql(alias)
            right = self.right_pred.to_sql(alias)
            return f"{left} {self.operator} {right}"

        correct_alias = self.get_correct_alias()
        field_ref = f"{correct_alias}.{self.attribute.name}"
        
        if self.operator == "CONTAINS":
            return f"CONTAINS({field_ref}, '{self.value}')"
        elif self.operator in {"SUM", "AVG", "COUNT", "MIN", "MAX"}:
            return f"{self.operator}({field_ref})"
        else:
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

    def _get_table_alias(self) -> str:
        """
        Dynamically get the table alias from the parent DataFrame's query builder.
        This ensures the correct alias is used in joined queries.
        """
        if hasattr(self.parent, 'query_builder'):
            return self.parent.query_builder._get_current_alias()
        return "t"  # Default alias

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
    
    def contains(self, value: str) -> 'AsterixPredicate':
        """Create a CONTAINS predicate."""
        return AsterixPredicate(
            attribute=self,
            operator="CONTAINS",
            value=value
        )
    
    def split(self, delimiter: str) -> 'AsterixAttribute':
        """Split string field by delimiter."""
        return AsterixAttribute(f"split({self.name}, '{delimiter}')", self.parent)
