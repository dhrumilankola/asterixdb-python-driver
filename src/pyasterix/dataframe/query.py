from typing import List, Optional, Any, Dict, Union
from datetime import datetime, date
from .attribute import AsterixPredicate

class AsterixQueryBuilder:
    """Builds SQL++ queries for AsterixDB."""

    def __init__(self):
        self.select_cols: List[str] = []
        self.where_clauses: List[AsterixPredicate] = []
        self.group_by_columns: List[str] = []
        self.aggregates: Dict[str, str] = {}
        self.order_by_columns: List[Dict[str, Union[str, bool]]] = []
        self.unnest_clauses: List[str] = []
        self.joins: List[Dict[str, Any]] = []  # To store JOIN clauses
        self.from_dataset: Optional[str] = None
        self.limit_val: Optional[int] = None
        self.offset_val: Optional[int] = None
        self.alias = "t"  # Default table alias
        self.current_dataverse: Optional[str] = None
        self.current_alias = None

    def reset(self):
        """Reset all query parts."""
        self.select_cols = []
        self.where_clauses = []
        self.group_by_columns = []
        self.aggregates = {}
        self.order_by_columns = []
        self.unnest_clauses = []
        self.joins = []
        self.limit_val = None
        self.offset_val = None

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
        # Ensure the correct alias is applied
        self._ensure_correct_alias(predicate)

        # Append the new predicate to the WHERE clauses
        self.where_clauses.append(predicate)
        return self


    def _ensure_correct_alias(self, predicate: AsterixPredicate) -> None:
        """Ensure predicate has correct alias based on its dataset."""
        if hasattr(predicate, 'parent') and predicate.parent:
            dataset = predicate.parent.dataset
            if 'Businesses' in dataset:
                predicate.update_alias('b')
            elif 'Reviews' in dataset:
                predicate.update_alias('r')

    def limit(self, n: int) -> 'AsterixQueryBuilder':
        """Set the LIMIT clause."""
        self.limit_val = n
        return self

    def offset(self, n: int) -> 'AsterixQueryBuilder':
        """Set the OFFSET clause."""
        self.offset_val = n
        return self

    def groupby(self, columns: Union[str, List[str]]) -> 'AsterixQueryBuilder':
        """Add GROUP BY clause to query."""
        if isinstance(columns, str):
            self.group_by_columns = [columns]
        else:
            self.group_by_columns = columns
        return self

    def aggregate(self, aggregates: Dict[str, str]) -> 'AsterixQueryBuilder':
        """Add aggregate functions to the query."""
        valid_aggs = {"AVG", "SUM", "COUNT", "MIN", "MAX", "ARRAY_AGG"}
        for col, func in aggregates.items():
            if func.upper() not in valid_aggs:
                raise ValidationError(f"Invalid aggregate function: {func}")
            self._validate_field_name(col)
            self.aggregates[col] = func.upper()

        return self

    def order_by(self, columns: Union[str, List[str], Dict[str, bool]], desc: bool = False) -> 'AsterixQueryBuilder':
        """Add ORDER BY clause to query."""
        if isinstance(columns, str):
            self.order_by_columns.append({"column": columns, "desc": desc})
        elif isinstance(columns, list):
            for col in columns:
                self.order_by_columns.append({"column": col, "desc": desc})
        elif isinstance(columns, dict):
            for col, is_desc in columns.items():
                self.order_by_columns.append({"column": col, "desc": is_desc})
        return self

    def build(self) -> str:
        """Build complete SQL++ query."""
        parts = []

        # Add USE statement if dataverse specified
        if self.current_dataverse:
            parts.append(f"USE {self.current_dataverse};")

        # Build SELECT clause
        select_clause = self._build_select_clause()
        if select_clause:
            parts.append(select_clause)

        # Build FROM clause
        parts.append(f"FROM {self.from_dataset} {self.alias}")

        # Add JOIN clause if present
        join_clause = self._build_join_clause()
        if join_clause:
            parts.append(join_clause)

        # Add WHERE clause if present
        where_clause = self._build_where_clause()
        if where_clause:
            parts.append(f"WHERE {where_clause}")

        # Add GROUP BY clause if present
        group_by_clause = self._build_group_by_clause()
        if group_by_clause:
            parts.append(group_by_clause)

        # Add ORDER BY clause if present
        order_by_clause = self._build_order_by_clause()
        if order_by_clause:
            parts.append(order_by_clause)

        # Add LIMIT and OFFSET clauses if present
        if self.limit_val is not None:
            parts.append(f"LIMIT {self.limit_val}")
        if self.offset_val is not None:
            parts.append(f"OFFSET {self.offset_val}")

        return " ".join(parts) + ";"

    def _build_select_clause(self) -> str:
        """Build the SELECT clause."""
        select_parts = []

        for col in self.select_cols:
            if " AS " in col:
                select_parts.append(col)
            else:
                prefix = col.split(".")[0] if "." in col else self.alias
                select_parts.append(f"{prefix}.{col}")

        for col, func in self.aggregates.items():
            select_parts.append(f"{func}({self.alias}.{col}) AS {col}_{func.lower()}")

        return f"SELECT {', '.join(select_parts)}" if select_parts else f"SELECT VALUE {self.alias}"

    def _build_where_clause(self) -> str:
        """Build the WHERE clause."""
        clauses = [pred.to_sql(self.alias) for pred in self.where_clauses]
        print(f"Debug: WHERE Clauses: {clauses}")  # Debug output
        return " AND ".join(clauses) if clauses else ""


    def _build_group_by_clause(self) -> str:
        """Build the GROUP BY clause."""
        group_cols = [f"{self.alias}.{col}" for col in self.group_by_columns]
        return f"GROUP BY {', '.join(group_cols)}" if group_cols else ""

    def _build_order_by_clause(self) -> str:
        """Build the ORDER BY clause."""
        order_parts = []
        for col_info in self.order_by_columns:
            col = col_info["column"]
            is_desc = col_info["desc"]
            order_parts.append(f"{self.alias}.{col} {'DESC' if is_desc else 'ASC'}")
        return f"ORDER BY {', '.join(order_parts)}" if order_parts else ""

    def _build_join_clause(self) -> str:
        """Build the JOIN clause."""
        return " ".join(
            f"JOIN {join['right_table']} {join['alias_right']} ON {join['alias_left']}.{join['on']} = {join['alias_right']}.{join['on']}"
            for join in self.joins
        )
        
    def add_unnest(self, field: str, alias: str, function: Optional[str] = None, table_alias: Optional[str] = None) -> None:
            """Add UNNEST clause to query."""
            table_alias = table_alias or self.alias
            if function:
                self.unnest_clauses.append(f"UNNEST {function} AS {alias}")
            else:
                self.unnest_clauses.append(f"UNNEST {table_alias}.{field} AS {alias}")
                
    def _build_unnest_clause(self) -> str:
        """Build the UNNEST clause."""
        return " ".join(self.unnest_clauses)

    def reset(self):
        """Reset all query parts."""
        self.select_cols = []
        self.where_clauses = []
        self.group_by_columns = []
        self.aggregates = {}
        self.order_by_columns = []
        self.unnest_clauses = []
        self.joins = []
        self.from_dataset = None
        self.limit_val = None
        self.offset_val = None