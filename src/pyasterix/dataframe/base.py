from typing import Union, List, Any, Dict, Tuple, Optional
import pandas as pd
from src.pyasterix._http_client import AsterixDBHttpClient
from ..connection import Connection
from src.pyasterix.exceptions import *
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
            raise ValidationError("connection must be an instance of Connection")
            
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
    
    def groupby(self, column: Union[str, List[str]]) -> 'AsterixGroupBy':
        """Group by one or more columns."""
        if isinstance(column, str):
            column = [column]
        return AsterixGroupBy(self, column)

    
    def aggregate(
        self,
        aggregates: Dict[str, str],
        group_by: Optional[Union[str, List[str]]] = None
    ) -> 'AsterixDataFrame':
        """
        Add aggregate functions to the query.
        Args:
            aggregates: Dictionary mapping field names to aggregate functions
            group_by: Optional field or list of fields to group by
        Returns:
            Updated AsterixDataFrame with aggregation logic
        """
        # Validate aggregate functions
        valid_aggs = {"AVG", "SUM", "COUNT", "MIN", "MAX", "ARRAY_AGG"}
        for col, func in aggregates.items():
            if func.upper() not in valid_aggs:
                raise ValidationError(f"Invalid aggregate function: {func}")
            self.query_builder.aggregate({col: func.upper()})

        # Handle group by fields
        if group_by:
            if isinstance(group_by, str):
                self.query_builder.groupby([group_by])
            elif isinstance(group_by, list):
                self.query_builder.groupby(group_by)

        return self

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
            raise ValidationError("Field name must be a non-empty string")
        
        # Split into parts (for nested fields)
        parts = field.split('.')
        if not all(self._is_valid_identifier(part) for part in parts):
            raise ValidationError(f"Invalid field name: {field}")

    def _validate_alias(self, alias: str) -> None:
        """Validate alias format."""
        if not self._is_valid_identifier(alias):
            raise ValidationError(f"Invalid alias: {alias}")

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
        
        try:
            # Execute the query
            self.cursor.execute(query)
            
            # Store the raw results
            raw_results = self.cursor.fetchall()
            
            # Process the results to ensure consistent format
            processed_results = self._process_results(raw_results)
            
            # Update result set
            self.result_set = processed_results
            self._executed = True
            
            # Return self for method chaining
            return self
            
        except Exception as e:
            raise QueryError(f"Failed to execute query: {str(e)}\nQuery: {query}")

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
        self.query_builder.reset()

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