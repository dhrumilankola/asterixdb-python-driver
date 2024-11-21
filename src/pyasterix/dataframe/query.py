from typing import List, Optional, Any, Dict, Union
from typing import List, Optional
from datetime import datetime, date
import json
from .attribute import AsterixPredicate

class AsterixQueryBuilder:
    """Builds SQL++ queries for AsterixDB."""
    
    def __init__(self):
        self.select_cols: List[str] = []
        self.where_clauses: List[AsterixPredicate] = []
        self.from_dataset: Optional[str] = None
        self.limit_val: Optional[int] = None
        self.offset_val: Optional[int] = None
        self.alias = "t"  # Default table alias
        self.current_dataverse: Optional[str] = None
        
    def reset_where(self):
        """Reset where clauses."""
        self.where_clauses = []
        
    def from_table(self, dataset: str) -> 'AsterixQueryBuilder':
        """Set the dataset and extract dataverse if provided."""
        if '.' in dataset:
            self.current_dataverse, self.from_dataset = dataset.split('.')
        else:
            self.from_dataset = dataset
        return self
        
    def select(self, columns: List[str]) -> 'AsterixQueryBuilder':
        """Set the columns to select."""
        self.select_cols = columns
        return self
        
    def where(self, predicate: AsterixPredicate) -> 'AsterixQueryBuilder':
        """Add a WHERE clause."""
        # Reset existing where clauses before adding new one
        self.reset_where()
        self.where_clauses.append(predicate)
        return self

    def limit(self, n: int) -> 'AsterixQueryBuilder':
        """Set the LIMIT clause."""
        self.limit_val = n
        return self

    def offset(self, n: int) -> 'AsterixQueryBuilder':
        """Set the OFFSET clause."""
        self.offset_val = n
        return self

    def _build_where_clause(self) -> str:
        """Build the WHERE clause from predicates."""
        if not self.where_clauses:
            return ""
        return " AND ".join(pred.to_sql(self.alias) for pred in self.where_clauses)

    def build(self) -> str:
        """Build the complete SQL++ query string."""
        if not self.from_dataset:
            raise ValueError("No dataset specified")

        query_parts = []
        
        # Add USE statement if dataverse is specified
        if self.current_dataverse:
            query_parts.append(f"USE {self.current_dataverse};")

        # Build SELECT clause
        select_clause = "SELECT VALUE t" if not self.select_cols else \
                       f"SELECT {', '.join(f'{self.alias}.{col}' for col in self.select_cols)}"
        query_parts.append(select_clause)

        # Build FROM clause
        from_clause = f"FROM {self.from_dataset} {self.alias}"
        query_parts.append(from_clause)

        # Build WHERE clause
        where_clause = self._build_where_clause()
        if where_clause:
            query_parts.append(f"WHERE {where_clause}")

        # Build LIMIT and OFFSET
        if self.limit_val is not None:
            query_parts.append(f"LIMIT {self.limit_val}")
        if self.offset_val is not None:
            query_parts.append(f"OFFSET {self.offset_val}")

        # Join all parts with spaces and add final semicolon
        query = " ".join(query_parts)
        if not query.endswith(';'):
            query += ';'

        print(f"\nGenerated Query: {query}")  # Debug print
        return query