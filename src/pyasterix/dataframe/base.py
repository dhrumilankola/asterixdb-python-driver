from typing import Union, List, Any, Dict, Optional
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
        return self
        
    def filter(self, predicate: AsterixPredicate) -> 'AsterixDataFrame':
        """Filter rows based on predicate."""
        # Reset any existing where clauses before adding new one
        self.query_builder.where(predicate)
        return self

    def limit(self, n: int) -> 'AsterixDataFrame':
        """Limit the number of results."""
        self.query_builder.limit(n)
        return self

    def offset(self, n: int) -> 'AsterixDataFrame':
        """Skip the first n results."""
        self.query_builder.offset(n)
        return self

    def execute(self) -> List[Dict[str, Any]]:
        """Execute the query and return results."""
        query = self.query_builder.build()
        print(f"\nExecuting Query: {query}")  # Debug print
        
        try:
            # Execute query using AsterixDBHttpClient
            result = self.client.execute_query(statement=query)
            
            if isinstance(result, dict):
                if result.get('status') != 'success':
                    raise RuntimeError(f"Query failed: {result.get('error', 'Unknown error')}")
                return result.get('results', [])
            else:
                raise RuntimeError(f"Unexpected response format: {result}")
            
        except Exception as e:
            raise RuntimeError(f"Failed to execute query: {str(e)}")