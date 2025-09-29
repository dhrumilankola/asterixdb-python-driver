from typing import Union, List, Any, Dict, Tuple, Optional
import pandas as pd
from ..connection import Connection
from ..exceptions import DataError, DataFrameError, QueryBuildError, ErrorMapper
from .attribute import AsterixAttribute, AsterixPredicate
from .query import AsterixQueryBuilder


class AsterixDataFrame:
    """DataFrame-like interface for AsterixDB datasets."""

    def __init__(self, connection, dataset):
        """
        Initialize AsterixDataFrame.
        
        Args:
            connection: AsterixDB connection instance
            dataset: Name of the dataset to query
        """
        if not isinstance(connection, Connection):
            raise DataError("connection must be an instance of Connection")
            
        self.connection = connection
        self.cursor = connection.cursor()
        self.dataset = dataset
        self.query_builder = AsterixQueryBuilder()
        self.query_builder.from_table(dataset)
        
        # Result tracking
        self._executed = False
        self.result_set = None
        self._query = None
        
        # For handling mock results (prior to execution)
        self.mock_result = []

    def __getitem__(self, key: Union[str, List[str], AsterixPredicate]) -> 'AsterixDataFrame':
        if isinstance(key, str):
            # Single column access
            return AsterixAttribute(name=key, parent=self)
        elif isinstance(key, list):
            # Multiple columns selection
            return self.select(key)
        elif isinstance(key, AsterixPredicate):
            # Filter rows
            return self.filter(key)
        else:
            raise TypeError(f"Invalid key type: {type(key)}")
            
    def select(self, columns: List[str]) -> 'AsterixDataFrame':
        """Select specific columns."""
        # Reset aggregates when doing a new select
        self.query_builder.aggregates = {}
        
        # Set the new columns
        self.query_builder.select(columns)
        self.mock_result = [{col: f"<{col}>" for col in columns}]
        return self
    
    def filter(self, predicate):
        """Add a filter condition to the query."""
        if not isinstance(predicate, AsterixPredicate):
            raise TypeError(f"Predicate must be an AsterixPredicate object, got {type(predicate)}")
        
        try:
            # Handle compound predicates recursively
            if predicate.is_compound:
                if predicate.left_pred:
                    self.filter(predicate.left_pred)
                if predicate.right_pred:
                    self.filter(predicate.right_pred)
                return self

            # Set correct alias based on dataset
            if predicate.attribute and predicate.attribute.parent:
                parent_dataset = predicate.attribute.parent.dataset
                
                # If this dataset is involved in a join, find and set the correct alias
                joins_updated = False
                for join in self.query_builder.joins:
                    if parent_dataset == join['right_table']:
                        predicate.update_alias(join['alias_right'])
                        joins_updated = True
                        break
                    elif parent_dataset == self.dataset:
                        predicate.update_alias(join['alias_left'])
                        joins_updated = True
                        break
                
                # If no join matched, use default alias
                if not joins_updated:
                    predicate.update_alias(self.query_builder.alias)

            self.query_builder.where(predicate)
            return self
        except Exception as e:
            raise QueryBuildError(f"Failed to add filter: {str(e)}")

    def limit(self, n: int) -> 'AsterixDataFrame':
        """Limit the number of results."""
        self.query_builder.limit(n)
        self.mock_result = self.mock_result[:n]
        return self

    def offset(self, n: int) -> 'AsterixDataFrame':
        """Skip the first n results."""
        self.query_builder.offset(n)
        self.mock_result = self.mock_result[n:]
        return self
    
    def group_by(self, columns):
        """
        Group DataFrame by columns.
        
        Args:
            columns: Column name or list of column names to group by
            
        Returns:
            AsterixDataFrame: Grouped DataFrame ready for aggregation
        """
        if isinstance(columns, str):
            columns = [columns]
            
        self.query_builder.groupby(columns)
        return self

    def having(self, predicate):
        """
        Filter groups based on an aggregated value.
        
        Args:
            predicate: AsterixPredicate for filtering aggregated results
            
        Returns:
            AsterixDataFrame: Filtered DataFrame
        """
        self.query_builder.having(predicate)
        return self

    def from_subquery(self, subquery, alias=None):
        """
        Create a DataFrame from a subquery.
        
        Args:
            subquery: SQL++ query string or another AsterixDataFrame
            alias: Alias for the subquery (default: 'sub')
            
        Returns:
            AsterixDataFrame: New DataFrame based on the subquery
        """
        alias = alias or f"sub{id(subquery) % 100}"  # Generate alias if not provided
        
        if isinstance(subquery, AsterixDataFrame):
            # Use query from another DataFrame
            sub_query = subquery.query_builder
        else:
            # Use raw query string
            sub_query = subquery
            
        # Create a new DataFrame with the same connection
        result = AsterixDataFrame(self.connection, None)
        result.query_builder.add_subquery(sub_query, alias)
        
        return result
    
    def agg(self, agg_dict):
        """
        Aggregate using one or more operations.
        
        Args:
            agg_dict: Dictionary of column names to aggregate functions
                    Example: {'column1': 'COUNT', 'column2': 'SUM'}
        
        Returns:
            AsterixDataFrame: New DataFrame with aggregated results
        """
        if not agg_dict:
            return self
            
        # Format aggregations into the expected format
        formatted_aggs = {}
        for col, func in agg_dict.items():
            if isinstance(func, str):
                func_name = func.upper()
                # Handle special case for COUNT(*) to create a valid alias
                if col == '*':
                    result_col = f"count_star"  # Use a valid alias instead of *_count
                else:
                    result_col = f"{col}_{func_name.lower()}"
                    
                formatted_aggs[result_col] = {
                    'column': col,
                    'function': func_name
                }
            elif isinstance(func, list):
                # Handle case where multiple aggregations apply to one column
                for f in func:
                    f_name = f.upper()
                    # Handle special case for COUNT(*) here too
                    if col == '*':
                        result_col = f"{f_name.lower()}_star"
                    else:
                        result_col = f"{col}_{f_name.lower()}"
                        
                    formatted_aggs[result_col] = {
                        'column': col,
                        'function': f_name
                    }
            
        # Add aggregations to query builder
        self.query_builder.aggregate(formatted_aggs)
        
        return self

    def count(self):
        """
        Count rows in each group or the entire DataFrame.
        
        Returns:
            AsterixDataFrame: DataFrame with count results
        """
        # Create a new DataFrame to avoid modifying the original
        result_df = AsterixDataFrame(self.connection, self.dataset)
        result_df.query_builder = self.query_builder
        
        # Clear any existing aggregates before adding COUNT
        result_df.query_builder.aggregates = {}
        
        # Add the count aggregation
        return result_df.agg({'*': 'COUNT'})
    
    def sum(self, columns=None):
        """
        Sum of values in specified columns.
        
        Args:
            columns: List of columns to sum or None for all numeric columns
            
        Returns:
            AsterixDataFrame: DataFrame with sum results
        """
        if not columns:
            # In a real implementation, we would determine numeric columns
            raise ValueError("Must specify columns to sum")
            
        if isinstance(columns, str):
            columns = [columns]
            
        agg_dict = {col: 'SUM' for col in columns}
        return self.agg(agg_dict)  

    def avg(self, columns=None):
        """
        Average of values in specified columns.
        
        Args:
            columns: List of columns to average or None for all numeric columns
            
        Returns:
            AsterixDataFrame: DataFrame with average results
        """
        if not columns:
            # In a real implementation, we would determine numeric columns
            raise ValueError("Must specify columns to average")
            
        if isinstance(columns, str):
            columns = [columns]
            
        agg_dict = {col: 'AVG' for col in columns}
        return self.agg(agg_dict)

    def order_by(self, columns: Union[str, List[str]], desc: bool = False) -> 'AsterixDataFrame':
        """Add ORDER BY clause to the query."""
        if isinstance(columns, str):
            columns = [columns]
        self.query_builder.order_by(columns, desc)
        return self

    def _is_valid_identifier(self, name: str) -> bool:
        """Check if a name is a valid AsterixDB identifier."""
        if not name or not isinstance(name, str):
            return False
        # Basic validation: starts with letter, contains only alphanumeric and underscore
        return name[0].isalpha() and all(c.isalnum() or c == '_' for c in name)

    def _validate_field_name(self, field: str) -> None:
        """Validate field name format."""
        if not field or not isinstance(field, str):
            raise DataError("Field name must be a non-empty string")
        
        # Split into parts (for nested fields)
        parts = field.split('.')
        if not all(self._is_valid_identifier(part) for part in parts):
            raise DataError(f"Invalid field name: {field}")

    def _validate_alias(self, alias: str) -> None:
        """Validate alias format."""
        if not self._is_valid_identifier(alias):
            raise DataError(f"Invalid alias: {alias}")

    def unnest(
        self,
        field: str,
        alias: str,
        function: Optional[str] = None
    ) -> 'AsterixDataFrame':
        """
        Unnest an array or apply a splitting function and unnest the results.
        
        Args:
            field: The field/array to unnest
            alias: Alias for the unnested values
            function: Optional function to apply before unnesting (e.g., split)
        """
        # Validate field name and alias
        self._validate_field_name(field)
        self._validate_alias(alias)
        
        # Get correct table alias
        table_alias = 'b' if 'Businesses' in self.dataset else 'r'
        
        # If function is provided, replace any instance of default alias 't'
        # with the correct table alias
        if function:
            function = function.replace('t.', f'{table_alias}.')
            
        # Add unnest clause to query builder
        self.query_builder.add_unnest(field, alias, function, table_alias)
        return self

    def where(self, condition: AsterixPredicate) -> 'AsterixDataFrame':
        """Keeps rows where the condition is True."""
        return self.filter(condition)

    def join(self, other, on=None, how="INNER", left_on=None, right_on=None, 
            alias_left=None, alias_right=None):
        """Join with another AsterixDataFrame."""
        if not isinstance(other, AsterixDataFrame):
            raise TypeError(f"Can only join with another AsterixDataFrame, got {type(other)}")
        
        # Set default aliases if not provided
        alias_left = alias_left or self.query_builder.alias
        alias_right = alias_right or f"r{len(self.query_builder.joins)}"
        
        # Update the alias in the query builder to match the alias_left
        self.query_builder.set_alias(alias_left)
        
        # Determine join columns
        if on:
            left_on = on
            right_on = on
        elif not (left_on and right_on):
            raise ValueError("Must provide either 'on' or both 'left_on' and 'right_on'")
        
        # Strip dataverse from right table if it contains a dataverse prefix
        right_table = other.dataset
        if '.' in right_table:
            # Extract just the dataset name without the dataverse
            right_table = right_table.split('.')[-1]
        
        # Add the join to the query builder
        self.query_builder.add_join(
            right_table=right_table,
            how=how,
            left_on=left_on,
            right_on=right_on,
            alias_left=alias_left,
            alias_right=alias_right
        )
        
        return self

    def mask(self, condition: AsterixPredicate) -> 'AsterixDataFrame':
        """Keeps rows where the condition is False."""
        if not isinstance(condition, AsterixPredicate):
            raise ValueError("Condition must be an instance of AsterixPredicate.")
        
        negated_condition = AsterixPredicate(
            attribute=None,
            operator="NOT",
            value=condition,
            is_compound=True
        )
        return self.filter(negated_condition)

    def isin(self, column: str, values: List[Any]) -> 'AsterixDataFrame':
        """Keeps rows where column value is in the given list."""
        predicate = AsterixAttribute(column, self).in_(values)
        return self.filter(predicate)

    def between(self, column: str, value1: Any, value2: Any) -> 'AsterixDataFrame':
        """Keeps rows where column value is between value1 and value2."""
        predicate = AsterixAttribute(column, self).between(value1, value2)
        return self.filter(predicate)

    def filter_items(self, items: List[str]) -> 'AsterixDataFrame':
        """Select specific columns (alternative to select)."""
        return self.select(items)

    def column_slice(self, start_col: str, end_col: str) -> 'AsterixDataFrame':
        """Select columns between two labels (inclusive)."""
        selected_cols = [col for col in self.mock_result[0] if start_col <= col <= end_col]
        return self.select(selected_cols)

    def execute(self):
        """Execute the built query and store the results."""
        # Build the query
        query = self.query_builder.build()
        self._query = query
        
        # Create high-level DataFrame span
        span = None
        if hasattr(self.connection, 'observability') and self.connection.observability:
            span = self.connection.observability.create_database_span(
                operation="dataframe.execute",
                query=query,
                dataset=self.dataset
            )
            
            # Add DataFrame-specific attributes
            if span and hasattr(span, 'set_attribute'):
                span.set_attribute("db.dataframe.dataset", self.dataset)
                span.set_attribute("db.dataframe.operation", "execute")
                span.set_attribute("db.query.builder", str(type(self.query_builder).__name__))
                
                # Add query complexity indicators
                if hasattr(self.query_builder, 'joins') and self.query_builder.joins:
                    span.set_attribute("db.query.joins", len(self.query_builder.joins))
                if hasattr(self.query_builder, 'aggregates') and self.query_builder.aggregates:
                    span.set_attribute("db.query.aggregates", len(self.query_builder.aggregates))
                if hasattr(self.query_builder, 'where_clauses') and self.query_builder.where_clauses:
                    span.set_attribute("db.query.where_clauses", len(self.query_builder.where_clauses))
        
        try:
            with span if span else self._noop_context():
                # Execute the query (this will create child spans in cursor)
                self.cursor.execute(query)
                
                # Store the raw results
                raw_results = self.cursor.fetchall()
                
                # Process the results to ensure consistent format
                processed_results = self._process_results(raw_results)
                
                # Update result set
                self.result_set = processed_results
                self._executed = True
                
                # Update span with results
                if span and hasattr(span, 'set_attribute'):
                    span.set_attribute("db.dataframe.result_count", len(processed_results))
                    span.set_attribute("db.dataframe.executed", True)
                    
                    # Calculate result complexity
                    if processed_results:
                        first_row = processed_results[0]
                        if isinstance(first_row, dict):
                            span.set_attribute("db.dataframe.result_columns", len(first_row.keys()))
                
                # Mark span as successful
                if span and self.connection.observability:
                    self.connection.observability.set_span_success(span)
                
                # Return self for method chaining
                return self
            
        except Exception as e:
            # Record error in span
            if span and self.connection.observability:
                self.connection.observability.record_span_exception(span, e)
            
            raise DataFrameError(f"Failed to execute query: {str(e)}\nQuery: {query}")
    
    def _noop_context(self):
        """No-operation context manager for when tracing is disabled."""
        class NoOpContext:
            def __enter__(self):
                return self
            def __exit__(self, exc_type, exc_val, exc_tb):
                pass
        return NoOpContext()

    def _ensure_executed(self):
        """Ensure the query has been executed."""
        if not self._executed:
            self.execute()

    def _process_results(self, raw_results):
        """Process raw results from AsterixDB to ensure consistent format."""
        if not raw_results:
            return []
            
        # Handle different types of results
        processed = []
        
        for item in raw_results:
            # Handle dictionaries directly
            if isinstance(item, dict):
                processed.append(item)
            # Handle scalar values
            elif not hasattr(item, '__iter__') or isinstance(item, (str, bytes)):
                processed.append({"value": item})
            # Handle lists/tuples
            elif isinstance(item, (list, tuple)):
                # Try to convert to dict if it looks like a key-value structure
                if len(item) % 2 == 0:
                    try:
                        processed.append(dict(zip(item[::2], item[1::2])))
                    except (TypeError, ValueError):
                        processed.append({"value": item})
                else:
                    processed.append({"value": item})
            else:
                # Default fallback
                processed.append({"value": item})
        
        return processed

    def fetchall(self):
        """Fetch all results as a list of dictionaries."""
        self._ensure_executed()
        return self.result_set
    
    def fetchone(self):
        """Fetch the first result."""
        self._ensure_executed()
        if not self.result_set:
            return None
        return self.result_set[0]

    def reset(self):
        """Reset all query parts."""
        # Create a fresh query builder instead of reusing the existing one
        self.query_builder = AsterixQueryBuilder()
        self.query_builder.from_table(self.dataset)
        
        # Reset result tracking
        self._executed = False
        self.result_set = None
        self._query = None
        self.mock_result = []
        
        return self

    def __iter__(self):
        """Allow iteration over results."""
        self._ensure_executed()
        return iter(self.result_set)

    def __len__(self):
        """Return the number of results."""
        self._ensure_executed()
        return len(self.result_set)

    def __repr__(self) -> str:
        """Return a string representation of the DataFrame."""
        if self.result_set is not None:
            return pd.DataFrame(self.result_set).__repr__()
        else:
            return pd.DataFrame(self.mock_result).__repr__()

    def __str__(self) -> str:
        """Return a user-friendly string representation of the DataFrame."""
        return self.__repr__()

    def head(self, n: int = 5) -> 'AsterixDataFrame':
        """Limit the number of results to the first n rows."""
        return self.limit(n)

    def tail(self, n: int = 5) -> 'AsterixDataFrame':
        """Return the last n rows by applying offset."""
        self.execute()  # Execute query to get result_set
        total_rows = len(self.result_set)
        return self.offset(total_rows - n)

    def to_pandas(self):
        """Convert the result set to a pandas DataFrame."""
        self._ensure_executed()
        
        import pandas as pd
        if not self.result_set:
            # Return empty DataFrame with appropriate structure
            return pd.DataFrame()
        
        return pd.DataFrame(self.result_set)

    def close(self):
        """Close the cursor."""
        if self.cursor:
            self.cursor.close()

    def __enter__(self):
        """Support context manager protocol."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up resources when exiting context."""
        self.close()
        
class AsterixGroupBy:
    """Handles group-by operations for AsterixDataFrame."""
    
    def __init__(self, dataframe: 'AsterixDataFrame', group_columns: List[str]):
        self.dataframe = dataframe
        self.group_columns = group_columns

    def agg(self, aggregates: Dict[str, str]) -> 'AsterixDataFrame':
        """Apply aggregation after grouping."""
        self.dataframe.query_builder.groupby(self.group_columns)
        self.dataframe.query_builder.aggregate(aggregates)
        return self.dataframe