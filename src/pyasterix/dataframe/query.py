from typing import List, Optional, Any, Dict, Union
from datetime import datetime, date
from .attribute import AsterixPredicate

class AsterixQueryBuilder:
    """Builds SQL++ queries for AsterixDB."""

    def __init__(self):
        self.select_cols = []
        self.where_clauses = []
        self.group_by_columns = []
        self.aggregates = {}
        self.from_subqueries = []  
        self.having_clauses = []  
        self.order_by_columns = []
        self.joins = []
        self.from_dataset = None
        self.limit_val = None
        self.offset_val = None
        self.alias = "t"  # Default table alias
        self.current_dataverse = None
        self.column_aliases = set()
        self.unnest_clauses = []

    def set_alias(self, alias):
        """Set the primary alias for the main dataset."""
        if not alias or not isinstance(alias, str):
            raise ValueError("Alias must be a non-empty string")
        self.alias = alias
        return self
    
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

    def from_table(self, dataset):
        """Set the dataset and extract dataverse if provided."""
        if dataset:
            if '.' in dataset:
                parts = dataset.split('.')
                if len(parts) == 2:
                    self.current_dataverse, self.from_dataset = parts
                else:
                    raise ValueError(f"Invalid dataset format: {dataset}")
            else:
                self.from_dataset = dataset
        else:
            raise ValueError("Dataset must be provided for the FROM clause.")
        return self


    def select(self, columns):
        """Set the columns to select."""
        self.select_cols = columns
        
        # Track aliases from SELECT clause
        for col in columns:
            if " AS " in col:
                # Extract the alias part
                parts = col.split(" AS ", 1)
                if len(parts) == 2:
                    alias = parts[1].strip()
                    self.column_aliases.add(alias)
        
        return self

    def where(self, predicate):
        """Add a WHERE clause."""
        self.where_clauses.append(predicate)
        return self

    def aggregate(self, agg_dict):
        """
        Add aggregation functions to the query.
        
        Args:
            agg_dict: Dictionary where keys are result column names and 
                    values are dictionaries with 'column' and 'function' keys
                    or simple strings representing function names
        """
        valid_aggs = {"AVG", "SUM", "COUNT", "MIN", "MAX", "ARRAY_AGG"}
        
        for result_col, agg_info in agg_dict.items():
            if isinstance(agg_info, dict):
                # Handle dictionary format from base.py
                func = agg_info.get('function', 'COUNT')
                if func.upper() not in valid_aggs:
                    raise ValueError(f"Invalid aggregate function: {func}")
                # Store as is
                self.aggregates[result_col] = agg_info
            elif isinstance(agg_info, str):
                # Handle string format (for direct calls)
                if agg_info.upper() not in valid_aggs:
                    raise ValueError(f"Invalid aggregate function: {agg_info}")
                # Convert to dictionary format
                self.aggregates[result_col] = {
                    'function': agg_info.upper(),
                    'column': result_col if result_col != '*' else '*'
                }
            else:
                raise ValueError(f"Invalid aggregate specification: {agg_info}")
            self.column_aliases.add(result_col)

        return self
    
    def add_subquery(self, subquery, alias):
        """
        Add a subquery to the FROM clause.
        
        Args:
            subquery: Either a SQL string or another AsterixQueryBuilder
            alias: Alias for the subquery result
        """
        if isinstance(subquery, AsterixQueryBuilder):
            # Convert query builder to a query string - strip off trailing semicolon
            query_str = subquery.build().rstrip(';')
            # If the query has USE dataverse, remove it for subquery
            if query_str.startswith("USE "):
                query_str = query_str[query_str.index(';') + 1:].strip()
        else:
            # Assume it's already a string
            query_str = subquery
            
        self.from_subqueries.append({
            'query': query_str,
            'alias': alias
        })
        return self
    
    def having(self, predicate):
        """
        Add a HAVING clause for filtering grouped results.
        
        Args:
            predicate: AsterixPredicate for filtering aggregated results
        """
        self.having_clauses.append(predicate)
        return self

    def _ensure_correct_alias(self, predicate: AsterixPredicate) -> None:
        """Ensure predicate has correct alias based on its dataset."""
        if hasattr(predicate, 'parent') and predicate.parent:
            dataset = predicate.parent.dataset
            if 'Businesses' in dataset:
                predicate.update_alias('b')
            elif 'Reviews' in dataset:
                predicate.update_alias('r')

    def limit(self, n):
        """Set the LIMIT clause."""
        self.limit_val = n
        return self

    def offset(self, n):
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

    def order_by(self, columns, desc=False):
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

    def _apply_table_alias_to_expression(self, expr):
        """Apply table alias to unqualified column references in expressions."""
        import re
        
        # Pattern to match function calls like SUM(column_name)
        # This handles: FUNC(column), FUNC(column1, column2), etc.
        func_pattern = r'(\w+)\s*\(\s*([^)]+)\s*\)'
        
        def replace_func(match):
            func_name = match.group(1)
            args = match.group(2).strip()
            
            # Split arguments by comma (for multi-argument functions)
            arg_parts = [arg.strip() for arg in args.split(',')]
            qualified_args = []
            
            for arg in arg_parts:
                # Skip if it's already qualified (contains .) or is a literal (* or number)
                if '.' in arg or arg == '*' or arg.isdigit() or arg.startswith('"') or arg.startswith("'"):
                    qualified_args.append(arg)
                else:
                    # Apply table alias
                    qualified_args.append(f"{self.alias}.{arg}")
            
            return f"{func_name}({', '.join(qualified_args)})"
        
        # Apply the replacement
        result = re.sub(func_pattern, replace_func, expr)
        return result

    def build(self):
        """Build complete SQL++ query."""
        parts = []
        
        # Add USE statement if dataverse specified
        if self.current_dataverse:
            parts.append(f"USE {self.current_dataverse};")
        
        # Start building the query
        query = []
        
        # Build SELECT clause
        select_clause = self._build_select_clause()
        query.append(select_clause)
        
        # Build FROM clause with JOINs and subqueries
        from_clause = self._build_from_clause()
        query.append(from_clause)
        
        # Build WHERE clause
        where_clause = self._build_where_clause()
        if where_clause:
            query.append(f"WHERE {where_clause}")
        
        # Build GROUP BY clause
        group_by_clause = self._build_group_by_clause()
        if group_by_clause:
            query.append(group_by_clause)
        
        # Build HAVING clause (only when GROUP BY is used)
        if group_by_clause:
            having_clause = self._build_having_clause()
            if having_clause:
                query.append(having_clause)
        
        # Build ORDER BY clause
        order_by_clause = self._build_order_by_clause()
        if order_by_clause:
            query.append(order_by_clause)
        
        # Add LIMIT and OFFSET
        if self.limit_val is not None:
            query.append(f"LIMIT {self.limit_val}")
        if self.offset_val is not None:
            query.append(f"OFFSET {self.offset_val}")
        
        # Add the query to parts
        parts.append(" ".join(query) + ";")
        
        return " ".join(parts)

    def _build_select_clause(self):
        """Build the SELECT clause with aggregates."""
        # If no columns or aggregates, select all
        if not self.select_cols and not self.aggregates:
            return f"SELECT VALUE {self.alias}"
        
        # Process selected columns
        select_parts = []
        for col in self.select_cols:
            # Handle column with explicit alias (AS)
            if " AS " in col:
                # For expressions, ensure table alias is applied to field references
                parts = col.split(" AS ", 1)
                expr = parts[0].strip()
                alias = parts[1].strip()
                
                # Check if it's a simple column reference or an expression
                if "." in expr or " " in expr or "(" in expr or ")" in expr or "+" in expr or "-" in expr or "*" in expr or "/" in expr or "%" in expr:
                    # It's an expression - apply table alias to unqualified column references
                    qualified_expr = self._apply_table_alias_to_expression(expr)
                    select_parts.append(f"{qualified_expr} AS {alias}")
                else:
                    # Simple column - qualify with table alias
                    select_parts.append(f"{self.alias}.{expr} AS {alias}")
            # Handle already qualified column reference
            elif "." in col:
                select_parts.append(col)
            # Handle simple column name
            else:
                select_parts.append(f"{self.alias}.{col}")
        
        # Add aggregates
        for result_col, agg_info in self.aggregates.items():
            func_name = agg_info['function']
            column = agg_info['column']
            
            # Format column reference based on whether it's * or a specific column
            if column == '*':
                select_parts.append(f"{func_name}(*) AS {result_col}")
            elif "." in column:
                select_parts.append(f"{func_name}({column}) AS {result_col}")
            else:
                select_parts.append(f"{func_name}({self.alias}.{column}) AS {result_col}")
        
        # Return final SELECT clause
        return f"SELECT {', '.join(select_parts)}" if select_parts else f"SELECT VALUE {self.alias}"
        
        # Add aggregates
        for result_col, agg_info in self.aggregates.items():
            func_name = agg_info['function']
            column = agg_info['column']
            
            # Format column reference based on whether it's * or a specific column
            if column == '*':
                select_parts.append(f"{func_name}(*) AS {result_col}")
            elif "." in column:
                select_parts.append(f"{func_name}({column}) AS {result_col}")
            else:
                select_parts.append(f"{func_name}({self.alias}.{column}) AS {result_col}")
        
        # Return final SELECT clause
        return f"SELECT {', '.join(select_parts)}" if select_parts else f"SELECT VALUE {self.alias}"

    def _build_from_clause(self):
        """Build the FROM clause with subqueries and JOINs."""
        # Handle regular table source
        if self.from_dataset:
            clause = f"FROM {self.from_dataset} {self.alias}"
        # Handle subquery source
        elif self.from_subqueries:
            subq = self.from_subqueries[0]  # Use first subquery as main FROM
            clause = f"FROM ({subq['query']}) {subq['alias']}"
            self.alias = subq['alias']  # Update default alias
        else:
            raise ValueError("No data source specified for query")
        
        # Add additional subqueries as joins if there are multiple
        if len(self.from_subqueries) > 1:
            for i, subq in enumerate(self.from_subqueries[1:], 1):
                join_alias = subq['alias']
                clause += f" JOIN ({subq['query']}) {join_alias} ON {self.alias}.id = {join_alias}.id"
        
        # Add regular joins
        for join in self.joins:
            alias_left = join.get('alias_left', self.alias)
            clause += f" {join['join_type']} {join['right_table']} {join['alias_right']} " \
                     f"ON {alias_left}.{join['left_on']} = {join['alias_right']}.{join['right_on']}"
        
        # Add UNNEST clauses if any
        if self.unnest_clauses:
            clause += " " + self._build_unnest_clause()
            
        return clause

    def _build_having_clause(self):
        """Build the HAVING clause by combining all predicates."""
        if not self.having_clauses:
            return ""
            
        # Convert all predicates to SQL strings and join with AND
        having_conditions = []
        for pred in self.having_clauses:
            sql = pred.to_sql()
            if sql:  # Only add non-empty conditions
                having_conditions.append(sql)
                
        return f"HAVING {' AND '.join(having_conditions)}" if having_conditions else ""
    
    def _build_where_clause(self):
        """Build the WHERE clause by combining all predicates."""
        if not self.where_clauses:
            return ""
            
        # Convert all predicates to SQL strings and join with AND
        where_conditions = []
        for pred in self.where_clauses:
            sql = pred.to_sql()
            if sql:  # Only add non-empty conditions
                where_conditions.append(sql)
                
        return " AND ".join(where_conditions)


    def _build_group_by_clause(self):
        """Build the GROUP BY clause."""
        if not self.group_by_columns:
            return ""
            
        group_cols = []
        for col in self.group_by_columns:
            # GROUP BY should never use SELECT aliases per SQL++ semantics
            # Always use the original column references
            if "." in col:
                # Already qualified column
                group_cols.append(col)
            else:
                # Regular column name, prefix with table alias
                group_cols.append(f"{self.alias}.{col}")
                    
        return f"GROUP BY {', '.join(group_cols)}" if group_cols else ""

    def _build_order_by_clause(self):
            """Build the ORDER BY clause."""
            if not self.order_by_columns:
                return ""
                
            order_parts = []
            for col_info in self.order_by_columns:
                col = col_info["column"]
                is_desc = col_info["desc"]
                
                # Check if this column is an alias or has qualified name
                if col in self.column_aliases or "." in col:
                    order_parts.append(f"{col} {'DESC' if is_desc else 'ASC'}")
                else:
                    order_parts.append(f"{self.alias}.{col} {'DESC' if is_desc else 'ASC'}")
                    
            return f"ORDER BY {', '.join(order_parts)}" if order_parts else ""

    def _build_join_clause(self) -> str:
        """Build the JOIN clause."""
        return " ".join(
            f"JOIN {join['right_table']} {join['alias_right']} ON {join['alias_left']}.{join['on']} = {join['alias_right']}.{join['on']}"
            for join in self.joins
        )
        
    def add_join(self, right_table, on=None, how="INNER", left_on=None, right_on=None, 
                alias_left=None, alias_right=None):
        """
        Add a join to the query.
        
        Args:
            right_table: The table to join with
            on: The column to join on (if same name in both tables)
            how: Join type ("INNER", "LEFT", "RIGHT", "OUTER")
            left_on: Join column from left table
            right_on: Join column from right table
            alias_left: Alias for left table (defaults to primary alias)
            alias_right: Alias for right table (defaults to "r" + join index)
        """
        if not right_table:
            raise ValueError("Right table must be provided for JOIN")
            
        # Handle default aliases
        alias_left = alias_left or self.alias
        alias_right = alias_right or f"r{len(self.joins)}"
        
        # Determine join columns
        if on:
            left_on = on
            right_on = on
        elif not (left_on and right_on):
            raise ValueError("Must provide either 'on' or both 'left_on' and 'right_on'")
        
        # Normalize join type
        valid_joins = {"INNER": "JOIN", "LEFT": "LEFT OUTER JOIN", 
                       "RIGHT": "RIGHT OUTER JOIN", "OUTER": "FULL OUTER JOIN"}
        join_type = valid_joins.get(how.upper(), "JOIN")
        
        # Add the join configuration
        self.joins.append({
            "right_table": right_table,
            "join_type": join_type,
            "left_on": left_on,
            "right_on": right_on,
            "alias_left": alias_left,
            "alias_right": alias_right
        })
        
        return self
        
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