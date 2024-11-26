from typing import Union, List, Any, Dict
import pandas as pd
from src.pyasterix._http_client import AsterixDBHttpClient
from .attribute import AsterixAttribute, AsterixPredicate
from .query import AsterixQueryBuilder


class AsterixDataFrame:
    """DataFrame-like interface for AsterixDB datasets."""

    def __init__(self, client: AsterixDBHttpClient, dataset: str):
        if not isinstance(client, AsterixDBHttpClient):
            raise TypeError("client must be an instance of AsterixDBHttpClient")
            
        self.client = client
        self.dataset = dataset
        self.query_builder = AsterixQueryBuilder()
        self.query_builder.from_table(dataset)
        self.result_set = None  # Stores results after query execution
        self.mock_result = []  # Mock results for inspection

    def __getitem__(self, key: Union[str, List[str], AsterixPredicate]) -> Union['AsterixDataFrame', AsterixAttribute]:
        if isinstance(key, str):
            # Column access: df['column']
            return AsterixAttribute(key, self)
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
        """Filter rows based on predicate."""
        self.query_builder.where(predicate)
        self.mock_result = [{**row} for row in self.mock_result]
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

    def where(self, condition: AsterixPredicate) -> 'AsterixDataFrame':
        """Keeps rows where the condition is True."""
        return self.filter(condition)

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
        """Execute the query and return a new AsterixDataFrame."""
        query = self.query_builder.build()
        print(f"\nExecuting Query: {query}")  # Debug print
        
        try:
            result = self.client.execute_query(statement=query)
            if isinstance(result, dict) and result.get('status') == 'success':
                new_df = AsterixDataFrame(self.client, self.dataset)
                new_df.result_set = result.get('results', [])
                return new_df
            else:
                raise RuntimeError(f"Query failed: {result.get('error', 'Unknown error')}")
        except Exception as e:
            raise RuntimeError(f"Failed to execute query: {str(e)}")

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
