from typing import Union, List, Any, Dict, Tuple, Optional
import pandas as pd
from src.pyasterix._http_client import AsterixDBHttpClient
from ..connection import Connection
from src.pyasterix.exceptions import *
from .attribute import AsterixAttribute, AsterixPredicate
from .query import AsterixQueryBuilder


class AsterixDataFrame:
    """DataFrame-like interface for AsterixDB datasets."""

    def __init__(self, connection: Connection, dataset: str):
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
        self.result_set = None
        self.mock_result = []

    def __getitem__(self, key: Union[str, List[str], AsterixPredicate]) -> Union['AsterixDataFrame', AsterixAttribute]:
        if isinstance(key, str):
            # Column access: df['column']
            attr = AsterixAttribute(
                name=key,
                parent=self
            )
            # Explicitly set the dataset from parent
            attr.parent.dataset = self.dataset  # Add this line
            return attr
        elif isinstance(key, list):
            # Multiple column selection: df[['col1', 'col2']]
            return self.select(key)
        elif isinstance(key, AsterixPredicate):
            # Boolean indexing: df[df['column'] > 5]
            return self.filter(key)
        else:
            raise TypeError(f"Invalid key type: {type(key)}")

            
    def select(self, columns: List[str]) -> 'AsterixDataFrame':
        """Select specific columns."""
        self.query_builder.select(columns)
        self.mock_result = [{col: f"<{col}>" for col in columns}]
        return self
    
    def filter(self, predicate: AsterixPredicate) -> 'AsterixDataFrame':
        """Add a filter condition to the query."""
        # Handle compound predicates recursively
        if predicate.is_compound:
            if predicate.left_pred:
                self.filter(predicate.left_pred)
            if predicate.right_pred:
                self.filter(predicate.right_pred)
            return self

        # Set correct alias based on dataset
        if predicate.attribute:
            if predicate.attribute.parent.dataset == 'Businesses':
                predicate.update_alias('b')
            elif predicate.attribute.parent.dataset == 'Reviews':
                predicate.update_alias('r')
            else:
                # For other datasets, use join aliases if available
                if self.query_builder.joins:
                    for join in self.query_builder.joins:
                        if predicate.attribute.parent.dataset == join['right_table']:
                            predicate.update_alias(join['alias_right'])
                        elif predicate.attribute.parent.dataset == self.dataset:
                            predicate.update_alias(join['alias_left'])

        self.query_builder.where(predicate)
        return self

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
    
    def groupby(self, column: str) -> 'AsterixDataFrame':
        """Group by a column."""
        self.query_builder.groupby(column)
        return self
    
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


    
    def order_by(
        self, 
        columns: Union[str, List[str]], 
        desc: bool = False
    ) -> 'AsterixDataFrame':
        """
        Add ORDER BY clause to query.
        
        Args:
            columns: Column(s) to sort by. Can be single column name or list of columns.
            desc: True for descending order, False for ascending
            
        Returns:
            Updated AsterixDataFrame
        """
        # Validate column names
        if isinstance(columns, str):
            columns = [columns]
            
        for col in columns:
            if " AS " in col:
                # For columns with aliases, validate the base column
                base_col = col.split(" AS ")[0].strip()
                if "." not in base_col:  # Not already qualified
                    self._validate_field_name(base_col)
            else:
                self._validate_field_name(col)
                
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


    def join(
        self,
        other: 'AsterixDataFrame',
        on: str,
        alias_left: str,
        alias_right: str
    ) -> 'AsterixDataFrame':
        """
        Join another dataset.

        Args:
            other: The other AsterixDataFrame to join.
            on: The column to join on.
            alias_left: Alias for the left dataset.
            alias_right: Alias for the right dataset.
        """
        if not isinstance(other, AsterixDataFrame):
            raise ValueError("Can only join with another AsterixDataFrame")
        self.query_builder.add_join(
            right_table=other.dataset,
            on=on,
            alias_left=alias_left,
            alias_right=alias_right
        )
        return self

    def mask(self, condition: AsterixPredicate) -> 'AsterixDataFrame':
        """Keeps rows where the condition is False."""
        negated_condition = AsterixPredicate(
            attribute=condition.attribute,
            operator=f"NOT ({condition.operator})",
            value=condition.value
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

    def execute(self) -> 'AsterixDataFrame':
        """Execute the built query and return a new AsterixDataFrame."""
        query = self.query_builder.build()
        print(f"\nExecuting Query: {query}")  # Debug print
        
        try:
            self.cursor.execute(query)
            
            # Convert results to proper format
            results = self.cursor.fetchall()
            
            # Create new DataFrame with results
            new_df = AsterixDataFrame(self.connection, self.dataset)
            new_df.result_set = results
            
            # If results exist but are empty, ensure proper structure
            if not results:
                new_df.result_set = []
                
            return new_df
            
        except Exception as e:
            raise QueryError(f"Failed to execute query: {str(e)}")

    def __repr__(self) -> str:
        """Return a string representation of the DataFrame."""
        if self.result_set is not None:
            return pd.DataFrame(self.result_set).__repr__()
        else:
            return pd.DataFrame(self.mock_result).__repr__()

    def __str__(self) -> str:
        """Return a user-friendly string representation of the DataFrame."""
        return self.__repr__()

    def head(self, n: int = 5) -> List[Dict[str, Any]]:
        """Return the first n rows of the mock result."""
        if self.result_set is not None:
            return self.result_set[:n]
        return self.mock_result[:n]

    def tail(self, n: int = 5) -> List[Dict[str, Any]]:
        """Return the last n rows of the mock result."""
        if self.result_set is not None:
            return self.result_set[-n:]
        return self.mock_result[-n:]

    def to_pandas(self) -> pd.DataFrame:
        """Convert the result set to a Pandas DataFrame."""
        if self.result_set is None:
            raise RuntimeError("No results available. Execute the query first.")
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