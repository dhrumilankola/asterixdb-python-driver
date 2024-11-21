from typing import Dict, List, Any, Union, Set, Optional, Tuple
from datetime import datetime, date, timezone
from decimal import Decimal
import json
from jinja2 import Environment, PackageLoader, select_autoescape
import logging
from pathlib import Path
from src.pyasterix._http_client import (
    AsterixDBHttpClient,
    AsterixDBError,
    QueryExecutionError
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AsterixClientError(Exception):
    """Base exception class for AsterixDB client errors."""
    pass

class ValidationError(AsterixClientError):
    """Raised when input validation fails."""
    pass

class TypeMappingError(AsterixClientError):
    """Raised when type mapping between Python and AsterixDB fails."""
    pass

class QueryBuildError(AsterixClientError):
    """Raised when query building fails."""
    pass

class AsterixClient:
    """
    High-level client for AsterixDB operations.
    Provides methods for managing dataverses, types, datasets, and data operations.
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 19002,
        timeout: int = 30
    ):
        """
        Initialize AsterixDB client.
        
        Args:
            host: AsterixDB host address
            port: AsterixDB HTTP port
            timeout: Request timeout in seconds
        """
        self.base_url = f"http://{host}:{port}"
        self._http_client = AsterixDBHttpClient(
            base_url=self.base_url,
            timeout=timeout
        )
        self._current_dataverse: Optional[str] = None
        
        # Initialize Jinja2 environment with autoescape disabled
        self.env = Environment(
            loader=PackageLoader('src.pyasterix', 'templates'),
            autoescape=False,  # Disable auto-escaping
            trim_blocks=True,
            lstrip_blocks=True
        )
        
        logger.info(f"Initialized AsterixDB client connecting to {self.base_url}")

    def _format_query(self, query: str) -> str:
        """
        Format query string for AsterixDB SQL++ execution.
        Ensures query is properly formatted.
        """
        # Remove any existing quotes and extra whitespace
        query = query.strip().strip('"\'')
        
        # Remove any newlines and extra spaces
        query = ' '.join(query.split())
        
        # Ensure statement ends with semicolon
        if not query.endswith(';'):
            query += ';'
        
        return query
    
    def get_schema(self, schema_type="dataset", name=None):
        if schema_type == "dataset":
            # Construct the query for retrieving the schema of a dataset
            query = f"SELECT * FROM `Metadata`.`Dataset` WHERE DatasetName = '{name}' LIMIT 1;"
        elif schema_type == "datatype":
            # Construct the query for retrieving the schema of a datatype
            query = f"SELECT * FROM `Metadata`.`Datatype` WHERE DatatypeName = '{name}' LIMIT 1;"
        else:
            raise ValueError("Invalid schema type. Choose 'dataset' or 'datatype'.")

        try:
            # Directly pass the query string to execute_query instead of a payload
            response = self._http_client.execute_query(query)
            
            if response:
                # Process schema information as needed
                schema_info = response.get("results", [])
                return schema_info
            else:
                print("Failed to retrieve schema.")
                return None
        except TypeError as e:
            print("Error:", e)
            return None

    def create_dataverse(
        self,
        name: str,
        if_not_exists: bool = False
    ) -> None:
        """
        Create a new dataverse.
        
        Args:
            name: Name of the dataverse
            if_not_exists: If True, won't error if dataverse exists
        """
        if not self._is_valid_identifier(name):
            raise ValidationError(f"Invalid dataverse name: {name}")
            
        try:
            # Drop existing dataverse if if_not_exists is True
            if if_not_exists:
                drop_query = f"DROP DATAVERSE {name} IF EXISTS;"
                try:
                    self._http_client.execute_query(drop_query)
                except QueryExecutionError:
                    # Ignore errors from drop query
                    pass
            
            # Create new dataverse
            create_query = f"CREATE DATAVERSE {name};"
            self._http_client.execute_query(create_query)
            
            # Set as current dataverse
            self._current_dataverse = name
            
        except QueryExecutionError as e:
            logger.error(f"Failed to create dataverse {name}. Error: {str(e)}")
            logger.error(f"Drop query: {drop_query}")
            logger.error(f"Create query: {create_query}")
            raise QueryExecutionError(f"Failed to create dataverse {name}: {str(e)}")
        
    def drop_dataverse(
        self,
        name: str,
        if_exists: bool = True
    ) -> Dict[str, Any]:
        """
        Drop a dataverse.
        
        Args:
            name: Name of the dataverse
            if_exists: If True, check existence before dropping
            
        Returns:
            Query execution result
        """
        if not self._is_valid_identifier(name):
            raise ValidationError(f"Invalid dataverse name: {name}")
        
        try:
            if if_exists:
                # Check if dataverse exists first
                check_query = "SELECT VALUE dv FROM Metadata.`Dataverse` dv " + \
                            f"WHERE dv.DataverseName = '{name}';"
                
                check_result = self._http_client.execute_query(check_query)
                if not check_result.get('results', []):
                    # Dataverse doesn't exist, return success
                    return {'status': 'success', 'message': f'Dataverse {name} does not exist'}
            
            # Dataverse exists or if_exists is False, proceed with drop
            drop_query = f"DROP DATAVERSE {name};"
            
            # Print for debugging
            print("\nGenerated drop dataverse query:")
            print(drop_query)
            
            result = self._http_client.execute_query(drop_query)
            
            if result.get('status') == 'success' and name == self._current_dataverse:
                self._current_dataverse = None
                
            return result
            
        except QueryExecutionError as e:
            if not if_exists:
                raise QueryExecutionError(f"Failed to drop dataverse {name}: {str(e)}")
            return {'status': 'success', 'message': f'Dataverse {name} does not exist'}

    def use_dataverse(self, name: str) -> Dict[str, Any]:
        """
        Set the current dataverse context.
        
        Args:
            name: Name of the dataverse to use
            
        Returns:
            Query execution result
        """
        if not self._is_valid_identifier(name):
            raise ValidationError(f"Invalid dataverse name: {name}")
        
        query = f"USE {name};"
        
        try:
            formatted_query = self._format_query(query)
            result = self._http_client.execute_query(formatted_query)
            if result.get('status') == 'success':
                self._current_dataverse = name
            return result
        except QueryExecutionError as e:
            raise QueryExecutionError(f"Failed to use dataverse {name}: {str(e)}")

    def create_type(
        self,
        name: str,
        schema: Dict[str, Any],
        if_not_exists: bool = False,
        open_type: bool = True
    ) -> Dict[str, Any]:
        """Create a new type in the current dataverse."""
        if not self._current_dataverse:
            raise AsterixClientError("No dataverse selected")
        
        logger.debug(f"Creating type '{name}' in dataverse '{self._current_dataverse}'")
        logger.debug(f"Schema: {json.dumps(schema, indent=2)}")
        
        if not self._is_valid_identifier(name):
            raise ValidationError(f"Invalid type name: {name}")
            
        try:
            type_definition = self._build_type_definition(schema)
            logger.debug(f"Generated type definition: {type_definition}")
            
            # Build the CREATE TYPE statement directly
            query_parts = [
                "CREATE TYPE",
                "IF NOT EXISTS" if if_not_exists else "",
                name,
                "AS",
                "CLOSED" if not open_type else "",
                type_definition,
                ";"
            ]
            query = " ".join(filter(None, query_parts))
            
            logger.debug(f"Final CREATE TYPE query: {query}")
            
            result = self._http_client.execute_query(
                statement=query,
                dataverse=self._current_dataverse
            )
            
            logger.debug(f"Create type result: {json.dumps(result, indent=2)}")
            return result
            
        except TypeMappingError as e:
            logger.error(f"Type mapping error: {str(e)}")
            raise ValidationError(f"Invalid schema definition: {str(e)}")
        except QueryExecutionError as e:
            logger.error(f"Query execution error: {str(e)}")
            raise QueryExecutionError(f"Failed to create type {name}: {str(e)}")


    def drop_type(
        self,
        name: str,
        if_exists: bool = True
    ) -> Dict[str, Any]:
        """
        Drop a type from the current dataverse.
        
        Args:
            name: Name of the type
            if_exists: If True, check existence before dropping
            
        Returns:
            Query execution result
        """
        if not self._current_dataverse:
            raise AsterixClientError("No dataverse selected")
            
        if not self._is_valid_identifier(name):
            raise ValidationError(f"Invalid type name: {name}")
        
        try:
            if if_exists:
                # Check if type exists first
                check_query = f"USE {self._current_dataverse};\n" + \
                            f"SELECT VALUE dt FROM Metadata.`Datatype` dt " + \
                            f"WHERE dt.DataverseName = '{self._current_dataverse}' " + \
                            f"AND dt.DatatypeName = '{name}';"
                
                check_result = self._http_client.execute_query(check_query)
                if not check_result.get('results', []):
                    # Type doesn't exist, return success
                    return {'status': 'success', 'message': f'Type {name} does not exist'}
            
            # Type exists or if_exists is False, proceed with drop
            drop_query = f"USE {self._current_dataverse};\n" + \
                        f"DROP TYPE {name};"
            
            # Print for debugging
            print("\nGenerated drop type query:")
            print(drop_query)
            
            result = self._http_client.execute_query(drop_query)
            return result
            
        except QueryExecutionError as e:
            if not if_exists:
                raise QueryExecutionError(f"Failed to drop type {name}: {str(e)}")
            return {'status': 'success', 'message': f'Type {name} does not exist'}

    def create_dataset(
        self,
        name: str,
        type_name: str,
        primary_key: str,
        if_not_exists: bool = False
    ) -> Dict[str, Any]:
        """
        Create a new dataset in the current dataverse.
        
        Args:
            name: Name of the dataset
            type_name: Name of the type for this dataset
            primary_key: Field to use as primary key
            if_not_exists: If True, won't error if dataset exists
            
        Returns:
            Query execution result
        """
        if not self._current_dataverse:
            raise AsterixClientError("No dataverse selected")
            
        if not all(self._is_valid_identifier(x) for x in [name, type_name, primary_key]):
            raise ValidationError("Invalid dataset name, type name, or primary key")
        
        # Ensure the type name is fully qualified if it's not in the current dataverse
        if '.' not in type_name:
            type_name = f"{self._current_dataverse}.{type_name}"
        
        # Build the CREATE DATASET query
        query = (
            f"CREATE {'INTERNAL ' if not if_not_exists else ''}"
            f"DATASET {self._current_dataverse}.{name}({type_name}) "
            f"PRIMARY KEY {primary_key};"
        )
        
        try:
            formatted_query = self._format_query(query)
            result = self._http_client.execute_query(formatted_query)
            return result
        except QueryExecutionError as e:
            raise QueryExecutionError(f"Failed to create dataset {name}: {str(e)}")

    def drop_dataset(
        self,
        name: str,
        if_exists: bool = True
    ) -> Dict[str, Any]:
        """
        Drop a dataset from the current dataverse.
        
        Args:
            name: Name of the dataset
            if_exists: If True, check existence before dropping
            
        Returns:
            Query execution result
        """
        if not self._current_dataverse:
            raise AsterixClientError("No dataverse selected")
            
        if not self._is_valid_identifier(name):
            raise ValidationError(f"Invalid dataset name: {name}")
        
        try:
            if if_exists:
                # Check if dataset exists first
                check_query = f"USE {self._current_dataverse};\n" + \
                            f"SELECT VALUE ds FROM Metadata.`Dataset` ds " + \
                            f"WHERE ds.DataverseName = '{self._current_dataverse}' " + \
                            f"AND ds.DatasetName = '{name}';"
                
                check_result = self._http_client.execute_query(check_query)
                if not check_result.get('results', []):
                    # Dataset doesn't exist, return success
                    return {'status': 'success', 'message': f'Dataset {name} does not exist'}
            
            # Dataset exists or if_exists is False, proceed with drop
            drop_query = f"USE {self._current_dataverse};\n" + \
                        f"DROP DATASET {name};"
            
            # Print for debugging
            print("\nGenerated drop dataset query:")
            print(drop_query)
            
            result = self._http_client.execute_query(drop_query)
            return result
            
        except QueryExecutionError as e:
            if not if_exists:
                raise QueryExecutionError(f"Failed to drop dataset {name}: {str(e)}")
            return {'status': 'success', 'message': f'Dataset {name} does not exist'}

    def find(
        self,
        dataset: str,
        condition: Optional[Dict[str, Any]] = None,
        projection: Optional[List[str]] = None,
        order_by: Optional[Union[str, List[str], Dict[str, str]]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Find documents in a dataset based on given criteria.
        """
        if not self._current_dataverse:
            raise AsterixClientError("No dataverse selected")
            
        if not self._is_valid_identifier(dataset):
            raise ValidationError(f"Invalid dataset name: {dataset}")

        try:
            # Build SELECT clause
            if projection:
                if not all(self._is_valid_identifier(field.split('.')[0]) for field in projection):
                    raise ValidationError("Invalid field name in projection")
                select_clause = ", ".join(f"d.{field}" for field in projection)
            else:
                select_clause = "VALUE d"

            # Build WHERE clause with proper field handling
            where_clause = None
            if condition:
                where_clause = self._build_condition("d", condition)

            # Build ORDER BY clause
            order_clause = None
            if order_by:
                order_clause = self._build_order_by("d", order_by)

            # Prepare template context
            context = {
                "select_clause": select_clause,
                "dataset": f"{dataset} d",
                "where_clause": where_clause,
                "order_clause": order_clause,
                "limit": limit,
                "offset": offset
            }

            # Get the template and render it
            template = self.env.get_template("select.sql")
            query = template.render(**context)
            
            # Add USE dataverse statement
            full_query = f"USE {self._current_dataverse};\n{query}"
            
            # Print for debugging
            print("\nGenerated query:")
            print(full_query)
            
            # Execute the query
            result = self._http_client.execute_query(full_query)
            
            if result.get('status') == 'success':
                return result.get('results', [])
            else:
                raise QueryExecutionError(f"Query failed with status: {result.get('status')}")
                
        except Exception as e:
            logger.error(f"Find query failed: {str(e)}")
            raise QueryExecutionError(f"Failed to execute find query: {str(e)}")

    def find_one(
        self,
        dataset: str,
        condition: Optional[Dict[str, Any]] = None,
        projection: Optional[List[str]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Find a single document matching the criteria.
        
        Args:
            dataset: Name of the dataset to query
            condition: Query conditions
            projection: Fields to include in the result
            
        Returns:
            Matching document or None if not found
        """
        results = self.find(
            dataset=dataset,
            condition=condition,
            projection=projection,
            limit=1
        )
        
        return results[0] if results else None

    def count(
        self,
        dataset: str,
        condition: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        Count documents in a dataset matching the condition.
        
        Args:
            dataset: Name of the dataset
            condition: Optional filter condition
            
        Returns:
            Number of matching documents
        """
        if not self._current_dataverse:
            raise AsterixClientError("No dataverse selected")
            
        if not self._is_valid_identifier(dataset):
            raise ValidationError(f"Invalid dataset name: {dataset}")

        try:
            # Build the count query
            query_parts = ["USE", self._current_dataverse + ";"]
            query_parts.extend(["SELECT COUNT(*) as count", f"FROM {dataset} d"])
            
            # Add WHERE clause if condition exists
            if condition:
                where_clause = self._build_condition("d", condition)
                if where_clause:
                    query_parts.append(f"WHERE {where_clause}")
            
            # Join all parts
            query = " ".join(query_parts) + ";"
            
            # Debug print
            print("\nGenerated count query:")
            print(query)
            
            # Execute query
            result = self._http_client.execute_query(query)
            
            if result.get('status') == 'success' and isinstance(result.get('results'), list):
                count_result = result['results'][0]
                if isinstance(count_result, dict):
                    return int(count_result.get('count', 0))
                return 0
                
            raise QueryExecutionError("Invalid count query response format")
            
        except Exception as e:
            logger.error(f"Count query failed: {str(e)}")
            raise QueryExecutionError(f"Failed to execute count query: {str(e)}")

    def aggregate(
        self,
        dataset: str,
        pipeline: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Perform aggregation operations on dataset.
        
        Args:
            dataset: Name of the dataset
            pipeline: List of aggregation stages
            
        Returns:
            Aggregation results
        """
        if not self._current_dataverse:
            raise AsterixClientError("No dataverse selected")
            
        if not self._is_valid_identifier(dataset):
            raise ValidationError(f"Invalid dataset name: {dataset}")

        def build_group_expr(group_spec: Dict[str, Any]) -> Tuple[List[str], List[str]]:
            """Build GROUP BY and aggregate expressions"""
            group_by = []
            aggregates = []
            
            by_fields = group_spec.get("by", [])
            if isinstance(by_fields, str):
                by_fields = [by_fields]
                
            for field in by_fields:
                if not self._is_valid_identifier(field.split('.')[-1]):
                    raise ValidationError(f"Invalid group by field: {field}")
                group_by.append(f"d.{field}")
                
            for field, agg in group_spec.items():
                if field == "by":
                    continue
                    
                if isinstance(agg, dict):
                    op = next(iter(agg))
                    if op not in {"$sum", "$avg", "$min", "$max", "$count", "$array_agg"}:
                        raise ValidationError(f"Unknown aggregation operator: {op}")
                    
                    op_name = op[1:].upper()  # Remove $ and uppercase
                    field_expr = agg[op]
                    
                    if field_expr == "*":
                        if op_name == "COUNT":
                            # Use AS for column alias
                            aggregates.append(f"COUNT(*) AS {field}")
                        else:
                            raise ValidationError(f"Operator {op} cannot be used with '*'")
                    else:
                        # Use AS for column alias
                        aggregates.append(f"{op_name}(d.{field_expr}) AS {field}")
            
            return group_by, aggregates

        try:
            query_parts = [f"USE {self._current_dataverse};"]
            
            for stage in pipeline:
                if len(stage) != 1:
                    raise ValidationError("Each pipeline stage should have exactly one operator")
                    
                op = next(iter(stage))
                
                if op == "$group":
                    group_by, aggregates = build_group_expr(stage[op])
                    
                    # Construct SELECT and FROM clauses
                    select_clause = f"SELECT {', '.join(aggregates)}" if aggregates else "SELECT VALUE d"
                    query_parts.append(select_clause)
                    query_parts.append(f"FROM {dataset} d")
                    
                    # Add GROUP BY after FROM
                    if group_by:
                        query_parts.append(f"GROUP BY {', '.join(group_by)}")
                
                elif op == "$filter" or op == "$where":
                    where_clause = self._build_condition("d", stage[op])
                    if where_clause:
                        query_parts.append(f"WHERE {where_clause}")
                
                elif op == "$order_by":
                    order_clause = self._build_order_by("d", stage[op])
                    if order_clause:
                        query_parts.append(f"ORDER BY {order_clause}")
                
                elif op == "$limit":
                    limit = int(stage[op])
                    query_parts.append(f"LIMIT {limit}")
                
                elif op == "$offset":
                    offset = int(stage[op])
                    query_parts.append(f"OFFSET {offset}")
                
                else:
                    raise ValidationError(f"Unknown pipeline operator: {op}")

            # Join query parts and format
            query = " ".join(query_parts) + ";"
            
            # Debug print
            print("\nGenerated aggregation query:")
            print(query)
            
            # Execute query
            result = self._http_client.execute_query(query)
            
            if result.get('status') == 'success':
                return result.get('results', [])
            else:
                raise QueryExecutionError(f"Query failed with status: {result.get('status')}")
                
        except Exception as e:
            logger.error(f"Aggregation failed: {str(e)}")
            raise QueryExecutionError(f"Failed to execute aggregation query: {str(e)}")

    def _build_order_by(self, alias: str, order_spec: Union[str, List[str], Dict[str, str]]) -> Optional[str]:
        """
        Build ORDER BY clause from specification for SQL++.
        
        Args:
            alias: Table alias to use for field references
            order_spec: Ordering specification
            
        Returns:
            ORDER BY clause string or None if no ordering
            
        Raises:
            ValidationError: If ordering specification is invalid
        """
        if not order_spec:
            return None
            
        def format_field(field: str) -> str:
            """Format field reference with alias"""
            if not self._is_valid_identifier(field):
                raise ValidationError(f"Invalid order field: {field}")
            return f"{alias}.{field}" if alias else field

        if isinstance(order_spec, str):
            return format_field(order_spec)
            
        elif isinstance(order_spec, list):
            return ", ".join(format_field(field) for field in order_spec)
            
        elif isinstance(order_spec, dict):
            clauses = []
            for field, direction in order_spec.items():
                direction = direction.upper()
                if direction not in ("ASC", "DESC"):
                    raise ValidationError(f"Invalid sort direction: {direction}")
                clauses.append(f"{format_field(field)} {direction}")
            return ", ".join(clauses)
            
        raise ValidationError(f"Invalid order_by specification type: {type(order_spec)}")

    def _build_aggregation(
        self,
        dataset: str,
        pipeline: List[Dict[str, Any]]
    ) -> str:
        """Build SQL++ aggregation query from pipeline specification."""
        agg_operators = {
            "$sum": "SUM",
            "$avg": "AVG",
            "$min": "MIN",
            "$max": "MAX",
            "$count": "COUNT",
            "$array_agg": "ARRAY_AGG"
        }

        def build_group_expr(group_spec: Dict[str, Any]) -> Tuple[str, str]:
            group_by = []
            aggregates = []
            
            by_fields = group_spec.get("by", [])
            if isinstance(by_fields, str):
                by_fields = [by_fields]
                
            for field in by_fields:
                if not self._is_valid_identifier(field):
                    raise ValidationError(f"Invalid group by field: {field}")
                group_by.append(field)
            
            for field, agg in group_spec.items():
                if field == "by":
                    continue
                    
                if isinstance(agg, dict):
                    op = next(iter(agg))
                    if op not in agg_operators:
                        raise ValidationError(f"Unknown aggregation operator: {op}")
                    
                    field_expr = agg[op]
                    if field_expr == "*":
                        aggregates.append(f"{field} := {agg_operators[op]}(*)")
                    else:
                        aggregates.append(
                            f"{field} := {agg_operators[op]}({field_expr})"
                        )
            
            return (
                " GROUP BY " + ", ".join(group_by) if group_by else "",
                ", ".join(aggregates)
            )

        query_parts = [f"FROM {dataset}"]
        
        for stage in pipeline:
            op = next(iter(stage))
            
            if op == "$group":
                group_clause, agg_clause = build_group_expr(stage[op])
                if agg_clause:
                    query_parts.append(f"SELECT {agg_clause}{group_clause}")
            
            elif op == "$filter" or op == "$where":
                condition = self._build_condition(stage[op])
                query_parts.append(f"WHERE {condition}")
            
            elif op == "$order_by":
                order_clause = self._build_order_by(stage[op])
                query_parts.append(f"ORDER BY {order_clause}")
            
            elif op == "$limit":
                query_parts.append(f"LIMIT {int(stage[op])}")
            
            elif op == "$offset":
                query_parts.append(f"OFFSET {int(stage[op])}")
            
            else:
                raise ValidationError(f"Unknown pipeline operator: {op}")

        return " ".join(query_parts)

    def _format_record(self, record: Dict[str, Any]) -> str:
        """
        Format a Python dictionary as an AsterixDB record string for the insert query.
        """
        def format_value(value: Any) -> Any:
            # Handles formatting for different types of values
            if value is None:
                return "null"
            elif isinstance(value, bool):
                return str(value).lower()
            elif isinstance(value, (int, float)):
                return value
            elif isinstance(value, str):
                return json.dumps(value)  # Automatically handles escape sequences
            elif isinstance(value, datetime):
                return f'datetime("{value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")}")'
            elif isinstance(value, date):
                return f'date("{value.strftime("%Y-%m-%d")}")'
            elif isinstance(value, (list, tuple)):
                return f"[{', '.join(map(str, map(format_value, value)))}]"
            elif isinstance(value, dict):
                formatted_dict = ", ".join(f'"{k}": {format_value(v)}' for k, v in value.items())
                return f"{{{formatted_dict}}}"
            else:
                raise ValueError(f"Unsupported type for value: {type(value)}")

        formatted_record = format_value(record)
        print("Formatted Record:", formatted_record)
        return formatted_record



    # def insert(
    #     self,
    #     dataset: str,
    #     records: Union[Dict[str, Any], List[Dict[str, Any]]]
    # ) -> Dict[str, Any]:
    #     """
    #     Insert one or more records into a dataset.
        
    #     Args:
    #         dataset: Name of the dataset
    #         records: Single record dict or list of record dicts
            
    #     Returns:
    #         Query execution result
    #     """
    #     if not self._current_dataverse:
    #         raise AsterixClientError("No dataverse selected")
            
    #     if not self._is_valid_identifier(dataset):
    #         raise ValidationError(f"Invalid dataset name: {dataset}")
        
    #     # Normalize input to list
    #     if isinstance(records, dict):
    #         records = [records]
    #     elif not isinstance(records, list):
    #         raise ValidationError("Records must be a dict or list of dicts")
        
    #     try:
    #         # Format datetime objects and prepare records
    #         formatted_records = []
    #         for record in records:
    #             # Deep copy to avoid modifying original
    #             record_copy = record.copy()
    #             # Format datetime fields
    #             for key, value in record_copy.items():
    #                 if isinstance(value, datetime):
    #                     record_copy[key] = f'datetime("{value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")}")'
                
    #             # Convert to JSON-like string
    #             record_str = "{"
    #             record_items = []
    #             for key, value in record_copy.items():
    #                 if isinstance(value, str) and not value.startswith('datetime("'):
    #                     value = f'"{value}"'
    #                 record_items.append(f'"{key}": {value}')
    #             record_str += ", ".join(record_items)
    #             record_str += "}"
    #             formatted_records.append(record_str)
            
    #         # Use template to construct query
    #         template = self.env.get_template('insert.sql')
    #         query = template.render(
    #             dataset=dataset,
    #             records=formatted_records
    #         )
            
    #         # Log the query for debugging
    #         logger.debug(f"Executing insert query:\n{query}")
            
    #         # Execute the query
    #         result = self._http_client.execute_query(query)
    #         return result
            
    #     except Exception as e:
    #         logger.error(f"Insert failed: {str(e)}")
    #         raise QueryExecutionError(f"Failed to insert into dataset {dataset}: {str(e)}")

    def insert(
        self,
        dataset: str,
        records: Union[Dict[str, Any], List[Dict[str, Any]]]
    ) -> Dict[str, Any]:
        """
        Insert one or more records into a dataset.
        
        Args:
            dataset: Name of the dataset
            records: Single record dict or list of record dicts
            
        Returns:
            Query execution result
        """
        if not self._current_dataverse:
            raise AsterixClientError("No dataverse selected")
            
        if not self._is_valid_identifier(dataset):
            raise ValidationError(f"Invalid dataset name: {dataset}")

        # Normalize input to list
        if isinstance(records, dict):
            records = [records]
        elif not isinstance(records, list):
            raise ValidationError("Records must be a dict or list of dicts")
        
        try:
            # Format records using _format_record
            formatted_records = [self._format_record(record) for record in records]
            
            # Render the query using the template
            template = self.env.get_template('insert.sql')
            query = template.render(
                dataset=dataset,
                records=formatted_records,
                current_dataverse=self._current_dataverse
            )
            
            # Print the generated query for debugging
            print(f"Generated Insert Query:\n{query}")
            
            # Execute the query and capture the result
            result = self._http_client.execute_query(query)
            print("Insert Success:", result)
            return result
            
        except Exception as e:
            print(f"Insert failed with error: {str(e)}")
            raise QueryExecutionError(f"Failed to insert into dataset {dataset}: {str(e)}")



        
    def delete(
        self,
        dataset: str,
        condition: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Delete records from a dataset that match the condition.
        
        Args:
            dataset: Name of the dataset
            condition: Dictionary of field-value pairs for deletion condition
            
        Returns:
            Query execution result
        """
        if not self._current_dataverse:
            raise AsterixClientError("No dataverse selected")
            
        if not self._is_valid_identifier(dataset):
            raise ValidationError(f"Invalid dataset name: {dataset}")
        
        try:
            where_clause = self._build_condition(condition)
        except QueryBuildError as e:
            raise ValidationError(f"Invalid condition: {str(e)}")
        
        template = self.env.get_template('delete.sql')
        query = template.render(
            dataset=dataset,
            where_clause=where_clause
        )
        
        try:
            formatted_query = self._format_query(query)
            result = self._http_client.execute_query(formatted_query)
            return result    
        except QueryExecutionError as e:
            raise QueryExecutionError(f"Failed to delete from dataset {dataset}: {str(e)}")

    def update(
        self,
        dataset: str,
        condition: Dict[str, Any],
        updates: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Update records in a dataset that match the condition using UPSERT.
        
        Args:
            dataset: Name of the dataset
            condition: Dictionary of field-value pairs for update condition
            updates: Dictionary of field-value pairs to update
            
        Returns:
            Query execution result
        """
        if not self._current_dataverse:
            raise AsterixClientError("No dataverse selected")
            
        if not self._is_valid_identifier(dataset):
            raise ValidationError(f"Invalid dataset name: {dataset}")
        
        try:
            # Build WHERE clause
            where_parts = []
            for field, value in condition.items():
                if not self._is_valid_identifier(field.split('.')[0]):
                    raise ValidationError(f"Invalid field name: {field}")
                formatted_value = self._format_value(value)
                where_parts.append(f"orig.{field} = {formatted_value}")
            where_clause = " AND ".join(where_parts)
            
            # Format updates for template
            formatted_updates = {}
            for field, value in updates.items():
                if not self._is_valid_identifier(field.split('.')[0]):
                    raise ValidationError(f"Invalid field name: {field}")
                formatted_updates[field] = self._format_value(value)
            
            # Get template and render query
            template = self.env.get_template('upsert.sql')
            query = template.render(
                current_dataverse=self._current_dataverse,
                dataset=dataset,
                where_clause=where_clause,
                updates=formatted_updates
            )
            
            # Print for debugging
            print("\nGenerated upsert query:")
            print(query)
            
            # Execute query
            result = self._http_client.execute_query(query)
            return result
            
        except Exception as e:
            logger.error(f"Update failed: {str(e)}")
            raise QueryExecutionError(f"Failed to update dataset {dataset}: {str(e)}")

    def _is_valid_identifier(self, name: str) -> bool:
        """Check if a name is a valid AsterixDB identifier."""
        if not name or not isinstance(name, str):
            return False
        # Basic validation: starts with letter, contains only alphanumeric and underscore
        return name[0].isalpha() and all(c.isalnum() or c == '_' for c in name)

    def _build_type_definition(self, schema: Dict[str, Any]) -> str:
        """
        Convert Python type definition to AsterixDB type definition.
        """
        # Base type mappings
        type_mapping = {
            "int": "int32",
            "int8": "tinyint",
            "int16": "smallint", 
            "int32": "int",
            "int64": "bigint",
            "string": "string",
            "float": "float",
            "double": "double",
            "boolean": "boolean",
            "datetime": "datetime",
            "date": "date",
            "time": "time",
            "binary": "binary",
            "point": "point",
            "line": "line",
            "circle": "circle",
            "rectangle": "rectangle",
            "polygon": "polygon",
            "uuid": "uuid",
            # Add any other basic types here
        }

        def validate_identifier(name: str) -> str:
            """Validate and format identifier according to AsterixDB rules"""
            if not name or not name.isidentifier():
                raise ValidationError(
                    f"Invalid field name '{name}'. Must be valid identifier: "
                    "start with letter/underscore, contain only letters/numbers/underscores"
                )
            return name

        def get_type_definition(field_type: Union[str, List, Dict]) -> str:
            """Get the type definition without field name"""
            if isinstance(field_type, str):
                # Handle multiset syntax
                if field_type.startswith('{{') and field_type.endswith('}}'):
                    inner_type = field_type[2:-2].strip()
                    if inner_type not in type_mapping:
                        raise TypeMappingError(f"Unsupported type in multiset: {inner_type}")
                    return f"{{{{ {type_mapping[inner_type]} }}}}"
                
                # Handle optional types (ending with ?)
                is_optional = field_type.endswith('?')
                base_type = field_type[:-1] if is_optional else field_type
                
                # Check if it's a custom type (ends with 'Type')
                if base_type.endswith('Type'):
                    asterix_type = base_type  # Use the type name as-is
                else:
                    if base_type not in type_mapping:
                        raise TypeMappingError(f"Unsupported type: {base_type}")
                    asterix_type = type_mapping[base_type]
                    
                if is_optional:
                    asterix_type += '?'
                    
                return asterix_type
                    
            elif isinstance(field_type, list):
                # Handle ordered lists/arrays
                if len(field_type) != 1:
                    raise TypeMappingError("Array type must have exactly one element type")
                element_type = get_type_definition(field_type[0])
                return f"[{element_type}]"
                    
            elif isinstance(field_type, dict):
                # Handle nested records
                nested_fields = [
                    f"{validate_identifier(k)}: {get_type_definition(v)}"
                    for k, v in field_type.items()
                ]
                return f"{{ {', '.join(nested_fields)} }}"
                
            raise TypeMappingError(f"Unsupported type structure: {field_type}")

        try:
            fields = [
                f"{validate_identifier(name)}: {get_type_definition(field_type)}"
                for name, field_type in schema.items()
            ]
            return f"{{ {', '.join(fields)} }}"
        except Exception as e:
            raise TypeMappingError(f"Failed to build type definition: {str(e)}")

    # def _build_condition(self, condition: Dict[str, Any]) -> str:
    #     """
    #     Build a WHERE clause from a condition dictionary.
        
    #     Args:
    #         condition: Dictionary of conditions
            
    #     Returns:
    #         WHERE clause string
            
    #     Examples:
    #     {
    #         "id": 1,  # Exact match
    #         "age": {"$gt": 25},  # Greater than
    #         "name": {"$like": "John%"},  # LIKE pattern
    #         "tags": {"$contains": "active"},  # Array/set containment
    #         "$or": [  # OR condition
    #             {"status": "active"},
    #             {"status": "pending"}
    #         ],
    #         "created": {"$between": ["2023-01-01", "2023-12-31"]}  # Between
    #     }
    #     """
    #     operators = {
    #         "$eq": "=",
    #         "$ne": "!=",
    #         "$gt": ">",
    #         "$gte": ">=",
    #         "$lt": "<",
    #         "$lte": "<=",
    #         "$like": "LIKE",
    #         "$contains": "IN",
    #         "$between": "BETWEEN",
    #         "$in": "IN",
    #         "$not": "NOT",
    #         "$exists": "IS NOT NULL",
    #         "$notexists": "IS NULL"
    #     }
        
    #     def format_value(value: Any) -> str:
    #         if value is None:
    #             return "NULL"
    #         elif isinstance(value, (int, float, Decimal)):
    #             return str(value)
    #         elif isinstance(value, bool):
    #             return str(value).lower()
    #         elif isinstance(value, (datetime, date)):
    #             return f"datetime('{value.isoformat()}')"
    #         elif isinstance(value, (list, tuple)):
    #             return f"[{', '.join(format_value(v) for v in value)}]"
    #         elif isinstance(value, dict):
    #             return json.dumps(value)
    #         else:
    #             return f"'{str(value)}'"

    #     def build_single_condition(field: str, value: Any) -> str:
    #         if isinstance(value, dict):
    #             # Handle operator conditions
    #             op = next(iter(value))
    #             if op not in operators:
    #                 raise QueryBuildError(f"Unknown operator: {op}")
                
    #             op_value = value[op]
    #             if op == "$between":
    #                 if not isinstance(op_value, (list, tuple)) or len(op_value) != 2:
    #                     raise QueryBuildError("$between requires a list/tuple of two values")
    #                 return f"{field} BETWEEN {format_value(op_value[0])} AND {format_value(op_value[1])}"
    #             elif op in ("$exists", "$notexists"):
    #                 return f"{field} {operators[op]}"
    #             elif op == "$contains":
    #                 return f"{format_value(op_value)} IN {field}"
    #             else:
    #                 return f"{field} {operators[op]} {format_value(op_value)}"
    #         else:
    #             # Handle direct value comparison
    #             return f"{field} = {format_value(value)}"

    #     def build_composite_condition(cond: Dict[str, Any]) -> str:
    #         clauses = []
            
    #         # Handle special operators
    #         if "$or" in cond:
    #             subconditions = [build_composite_condition(c) if isinstance(c, dict) else build_single_condition(*next(iter(c.items()))) for c in cond["$or"]]
    #             return f"({' OR '.join(subconditions)})"
                
    #         if "$and" in cond:
    #             subconditions = [build_composite_condition(c) if isinstance(c, dict) else build_single_condition(*next(iter(c.items()))) for c in cond["$and"]]
    #             return f"({' AND '.join(subconditions)})"
            
    #         # Handle regular field conditions
    #         for field, value in cond.items():
    #             if not field.startswith("$"):
    #                 clauses.append(build_single_condition(field, value))
            
    #         return " AND ".join(clauses) if clauses else "TRUE"

    #     try:
    #         return build_composite_condition(condition)
    #     except Exception as e:
    #         raise QueryBuildError(f"Failed to build condition: {str(e)}")

    # def _build_updates(self, updates: Dict[str, Any]) -> str:
    #     """
    #     Build a SET clause from an updates dictionary.
        
    #     Args:
    #         updates: Dictionary of field updates
            
    #     Returns:
    #         SET clause string
            
    #     Examples:
    #     {
    #         "status": "inactive",  # Direct value update
    #         "count": {"$inc": 1},  # Increment
    #         "metadata": {"$merge": {"updated": "2023-01-01"}},  # Merge object
    #         "tags": {"$push": "new-tag"},  # Array push
    #         "score": {"$multiply": 1.1}  # Multiply value
    #     }
    #     """
    #     def format_update_value(field: str, value: Any) -> str:
    #         if isinstance(value, dict):
    #             op = next(iter(value))
    #             op_value = value[op]
                
    #             if op == "$inc":
    #                 return f"{field} = {field} + {op_value}"
    #             elif op == "$multiply":
    #                 return f"{field} = {field} * {op_value}"
    #             elif op == "$merge":
    #                 return f"{field} = {field} || {json.dumps(op_value)}"
    #             elif op == "$push":
    #                 if isinstance(op_value, (list, tuple)):
    #                     array_lit = f"[{', '.join(str(v) for v in op_value)}]"
    #                 else:
    #                     array_lit = f"[{op_value}]"
    #                 return f"{field} = array_concat({field}, {array_lit})"
    #             elif op == "$pull":
    #                 # Remove elements from array
    #                 return f"{field} = array_remove({field}, {op_value})"
    #             else:
    #                 raise QueryBuildError(f"Unknown update operator: {op}")
    #         else:
    #             # Direct value assignment
    #             return f"{field} = {self._format_value(value)}"

    #     try:
    #         updates_list = [
    #             format_update_value(field, value)
    #             for field, value in updates.items()
    #         ]
    #         return ", ".join(updates_list)
    #     except Exception as e:
    #         raise QueryBuildError(f"Failed to build updates: {str(e)}")


    def _build_condition(self, alias: str, condition: Dict[str, Any]) -> str:
        """Build WHERE clause from condition dictionary."""
        def format_field_ref(field: str) -> str:
            """Format field reference with proper nesting"""
            parts = field.split('.')
            if len(parts) == 1:
                return f"{alias}.{field}"
            else:
                return f"{alias}.{'.'.join(parts)}"

        def build_comparison(field: str, operator: str, value: Any) -> str:
            """Build a comparison expression"""
            field_ref = format_field_ref(field)
            formatted_value = self._format_value(value)
            
            if operator == "$gt":
                return f"{field_ref} > {formatted_value}"
            elif operator == "$gte":
                return f"{field_ref} >= {formatted_value}"
            elif operator == "$lt":
                return f"{field_ref} < {formatted_value}"
            elif operator == "$lte":
                return f"{field_ref} <= {formatted_value}"
            elif operator == "$eq":
                return f"{field_ref} = {formatted_value}"
            elif operator == "$ne":
                return f"{field_ref} != {formatted_value}"
            elif operator == "$contains":
                # For array containment in AsterixDB
                return f"array_contains({field_ref}, {formatted_value})"
            elif operator == "$in":
                # For checking if value is in array
                return f"{formatted_value} in {field_ref}"
            else:
                raise ValidationError(f"Unsupported operator: {operator}")

        try:
            if not condition:
                return None

            clauses = []
            for field, value in condition.items():
                if isinstance(value, dict):
                    # Handle operator conditions
                    for op, op_value in value.items():
                        clauses.append(build_comparison(field, op, op_value))
                else:
                    # Direct equality comparison
                    field_ref = format_field_ref(field)
                    formatted_value = self._format_value(value)
                    clauses.append(f"{field_ref} = {formatted_value}")

            return " AND ".join(clauses)

        except Exception as e:
            logger.error(f"Failed to build condition: {str(e)}")
            raise ValidationError(f"Failed to build condition: {str(e)}")


    def _format_value(self, value: Any) -> str:
        """Format a Python value for use in SQL++ query."""
        if value is None:
            return "NULL"
        elif isinstance(value, (int, float, Decimal)):
            return str(value)
        elif isinstance(value, bool):
            return str(value).lower()
        elif isinstance(value, datetime):
            return f"datetime('{value.strftime('%Y-%m-%dT%H:%M:%S.%f%z')}')"
        elif isinstance(value, date):
            return f"date('{value.strftime('%Y-%m-%d')}')"
        elif isinstance(value, (list, tuple)):
            return f"[{', '.join(self._format_value(v) for v in value)}]"
        elif isinstance(value, dict):
            formatted_pairs = []
            for k, v in value.items():
                formatted_value = self._format_value(v)
                formatted_pairs.append(f'"{k}": {formatted_value}')
            return f"{{{', '.join(formatted_pairs)}}}"
        elif isinstance(value, str):
            # Properly escape single quotes for SQL++ by doubling them
            escaped_value = value.replace("'", "''")
            return f"'{escaped_value}'"
        else:
            raise ValueError(f"Unsupported value type for SQL++: {type(value)}")
        
    def close(self):
        """Close the client connection."""
        self._http_client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()