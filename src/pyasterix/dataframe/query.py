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
        self.group_by_columns: List[str] = []
        self.aggregates: Dict[str, str] = {}
        self.order_by_columns: List[Dict[str, Union[str, bool]]] = []
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

    def groupby(self, column: str) -> 'AsterixQueryBuilder':
        """Add a GROUP BY clause."""
        self.group_by_columns.append(column)
        return self

    def _build_group_by_clause(self) -> str:
        """Build the GROUP BY clause."""
        if not self.group_by_columns:
            return ""
        return f"GROUP BY {', '.join(f'{self.alias}.{col}' for col in self.group_by_columns)}"

    def aggregate(self, aggregates: Dict[str, str]) -> 'AsterixQueryBuilder':
        """Add aggregate functions to the query."""
        self.aggregates.update(aggregates)
        return self

    def _build_select_clause(self) -> str:
        """Build the SELECT clause, including aggregates."""
        if self.aggregates:
            # Add aggregate functions
            aggregate_parts = [f"{func}({self.alias}.{col}) AS {func.lower()}_{col}" for col, func in self.aggregates.items()]
            select_clause = "SELECT " + ", ".join(aggregate_parts)
        elif self.select_cols:
            # Add regular columns
            select_clause = "SELECT " + ", ".join(f"{self.alias}.{col}" for col in self.select_cols)
        else:
            # Default SELECT clause
            select_clause = "SELECT VALUE t"
        return select_clause

    def order_by(self, columns: Union[str, List[str]], desc: bool = False) -> 'AsterixQueryBuilder':
        """Add an ORDER BY clause."""
        if isinstance(columns, str):
            self.order_by_columns.append({"column": columns, "desc": desc})
        elif isinstance(columns, list):
            for col in columns:
                self.order_by_columns.append({"column": col, "desc": desc})
        else:
            raise TypeError("columns must be a string or list of strings")
        return self

    def _build_order_by_clause(self) -> str:
        """Build the ORDER BY clause."""
        if not self.order_by_columns:
            return ""
        order_parts = []
        for col in self.order_by_columns:
            column = col["column"]
            # Use alias for aggregates if defined
            if column.upper() in self.aggregates.values():
                column = f"{column.lower()}_{list(self.aggregates.keys())[list(self.aggregates.values()).index(column.upper())]}"
            order_parts.append(f"{column} {'DESC' if col['desc'] else 'ASC'}")
        return "ORDER BY " + ", ".join(order_parts)


    def build(self) -> str:
        """Build the complete SQL++ query string."""
        if not self.from_dataset:
            raise ValueError("No dataset specified")

        query_parts = []
        
        # Add USE statement if dataverse is specified
        if self.current_dataverse:
            query_parts.append(f"USE {self.current_dataverse};")

        # Build SELECT clause
        query_parts.append(self._build_select_clause())

        # Build FROM clause
        from_clause = f"FROM {self.from_dataset} {self.alias}"
        query_parts.append(from_clause)

        # Build WHERE clause
        where_clause = self._build_where_clause()
        if where_clause:
            query_parts.append(f"WHERE {where_clause}")

        # Build GROUP BY clause
        group_by_clause = self._build_group_by_clause()
        if group_by_clause:
            query_parts.append(group_by_clause)

        # Build ORDER BY clause
        order_by_clause = self._build_order_by_clause()
        if order_by_clause:
            query_parts.append(order_by_clause)

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