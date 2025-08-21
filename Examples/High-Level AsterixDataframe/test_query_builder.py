#!/usr/bin/env python3
"""
Test script for AsterixQueryBuilder implementation.
This script tests query building functionality to verify SQL++ generation logic.
"""

import sys
import os
import json

# Add the project root to the path so we can import our modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import our modules
from src.pyasterix.dataframe.query import AsterixQueryBuilder
from src.pyasterix.dataframe.attribute import AsterixAttribute, AsterixPredicate
from src.pyasterix.exceptions import ValidationError

def print_section(title):
    """Print a section header."""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)

def print_query_test(query, expected=None):
    """Print the result of a query test."""
    print(f"\nGenerated Query: {query}")
    if expected:
        print(f"Expected Query: {expected}")
        if query.strip() == expected.strip():
            print("✓ Query matches expected output")
        else:
            print("✗ Query does not match expected output")

def test_query_builder():
    """Test the AsterixQueryBuilder functionality."""
    print_section("TESTING QUERY BUILDER")
    
    # Test 1: Basic Select
    print("\nTest 1: Basic Select")
    qb = AsterixQueryBuilder()
    qb.from_table("Products")
    qb.select(["id", "name", "price"])
    query = qb.build()
    expected = "SELECT t.id, t.name, t.price FROM Products t;"
    print_query_test(query, expected)
    
    # Test 2: Select with WHERE clause
    print("\nTest 2: Select with WHERE clause")
    qb = AsterixQueryBuilder()
    qb.from_table("Products")
    qb.set_alias("p")  # Custom alias
    qb.select(["id", "name", "price"])
    
    # Create a mock DataFrame and attribute for predicate
    class MockDataFrame:
        def __init__(self, dataset):
            self.dataset = dataset
            self.query_builder = qb
    
    mock_df = MockDataFrame("Products")
    price_attr = AsterixAttribute("price", mock_df)
    
    # Add predicate: price > 100
    price_pred = AsterixPredicate(price_attr, ">", 100)
    qb.where(price_pred)
    
    query = qb.build()
    expected = "SELECT p.id, p.name, p.price FROM Products p WHERE p.price > 100;"
    print_query_test(query, expected)
    
    # Test 3: Select with complex WHERE clause
    print("\nTest 3: Select with complex WHERE clause")
    qb = AsterixQueryBuilder()
    qb.from_table("Products")
    qb.set_alias("p")
    qb.select(["id", "name", "price", "category"])
    
    mock_df = MockDataFrame("Products")
    price_attr = AsterixAttribute("price", mock_df)
    category_attr = AsterixAttribute("category", mock_df)
    
    # Add compound predicate: price > 100 AND category = 'Electronics'
    price_pred = AsterixPredicate(price_attr, ">", 100)
    category_pred = AsterixPredicate(category_attr, "=", "Electronics")
    compound_pred = AsterixPredicate(None, "AND", None, True, price_pred, category_pred)
    qb.where(compound_pred)
    
    query = qb.build()
    expected = "SELECT p.id, p.name, p.price, p.category FROM Products p WHERE (p.price > 100) AND (p.category = 'Electronics');"
    print_query_test(query, expected)
    
    # Test 4: Select with ORDER BY and LIMIT
    print("\nTest 4: Select with ORDER BY and LIMIT")
    qb = AsterixQueryBuilder()
    qb.from_table("Products")
    qb.select(["id", "name", "price"])
    qb.order_by("price", desc=True)
    qb.limit(5)
    
    query = qb.build()
    expected = "SELECT t.id, t.name, t.price FROM Products t ORDER BY t.price DESC LIMIT 5;"
    print_query_test(query, expected)
    
    # Test 5: Join query
    print("\nTest 5: Join query")
    qb = AsterixQueryBuilder()
    qb.from_table("Orders")
    qb.set_alias("o")
    qb.add_join(
        right_table="Products",
        left_on="product_id",
        right_on="id",
        alias_right="p"
    )
    qb.select(["o.id AS order_id", "p.name AS product_name", "o.quantity"])
    
    query = qb.build()
    expected = "SELECT o.id AS order_id, p.name AS product_name, o.quantity FROM Orders o JOIN Products p ON o.product_id = p.id;"
    print_query_test(query, expected)
    
    # Test 6: Multi-join query with filtering
    print("\nTest 6: Multi-join query with filtering")
    qb = AsterixQueryBuilder()
    qb.from_table("Orders")
    qb.set_alias("o")
    
    # Join with Products
    qb.add_join(
        right_table="Products",
        left_on="product_id",
        right_on="id",
        alias_right="p"
    )
    
    # Join with Customers
    qb.add_join(
        right_table="Customers",
        left_on="customer_id",
        right_on="id",
        alias_right="c"
    )
    
    qb.select([
        "o.id AS order_id", 
        "c.name AS customer_name",
        "p.name AS product_name", 
        "o.quantity"
    ])
    
    # Mock setup for predicate
    orders_df = MockDataFrame("Orders")
    status_attr = AsterixAttribute("status", orders_df)
    status_pred = AsterixPredicate(status_attr, "=", "delivered")
    qb.where(status_pred)
    
    # Add LIMIT
    qb.limit(10)
    
    query = qb.build()
    expected = "SELECT o.id AS order_id, c.name AS customer_name, p.name AS product_name, o.quantity FROM Orders o JOIN Products p ON o.product_id = p.id JOIN Customers c ON o.customer_id = c.id WHERE o.status = 'delivered' LIMIT 10;"
    print_query_test(query, expected)
    
    # Test 7: Query with dataverse
    print("\nTest 7: Query with dataverse")
    qb = AsterixQueryBuilder()
    qb.from_table("TestDataFrame.Products")  # With dataverse
    qb.select(["id", "name", "price"])
    
    query = qb.build()
    expected = "USE TestDataFrame; SELECT t.id, t.name, t.price FROM Products t;"
    print_query_test(query, expected)
    
    print_section("QUERY BUILDER TESTS COMPLETED")

if __name__ == "__main__":
    test_query_builder()