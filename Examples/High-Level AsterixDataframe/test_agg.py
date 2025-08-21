import sys
import os
import pandas as pd
from datetime import datetime
import time

# Add the project root to the path so we can import our modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import our modules
from src.pyasterix.connection import Connection
from src.pyasterix.dataframe.base import AsterixDataFrame
from src.pyasterix.dataframe.attribute import AsterixAttribute


def test_dataframe_aggregation():
    """Test AsterixDataFrame aggregation functionality, focusing on the high-level API."""
    # Initialize connection
    conn = Connection(base_url="http://localhost:19002")
    print("\nSetting up test data...")
    
    cursor = conn.cursor()
    # Create test dataverse and dataset with numeric data for aggregation testing
    setup_query = """
    DROP DATAVERSE TestDF IF EXISTS;
    CREATE DATAVERSE TestDF;
    USE TestDF;

    CREATE TYPE SalesType AS {
        id: int,
        product: string,
        category: string,
        price: double,
        quantity: int,
        region: string
    };

    CREATE DATASET Sales(SalesType) PRIMARY KEY id;

    INSERT INTO Sales([
        {"id": 1, "product": "Widget A", "category": "Hardware", "price": 19.99, "quantity": 10, "region": "North"},
        {"id": 2, "product": "Widget B", "category": "Hardware", "price": 24.99, "quantity": 5, "region": "South"},
        {"id": 3, "product": "Software X", "category": "Software", "price": 49.99, "quantity": 2, "region": "East"},
        {"id": 4, "product": "Software Y", "category": "Software", "price": 99.99, "quantity": 1, "region": "West"},
        {"id": 5, "product": "Widget C", "category": "Hardware", "price": 14.99, "quantity": 15, "region": "North"},
        {"id": 6, "product": "Software Z", "category": "Software", "price": 29.99, "quantity": 3, "region": "South"}
    ]);
    """
    cursor.execute(setup_query)
    print("Test data created successfully.")
    
    try:
        # 1. Test basic count aggregation
        print("\n1. Testing count() on DataFrame")
        sales_df = AsterixDataFrame(conn, "TestDF.Sales")
        count_result = sales_df.count().execute()
        print(f"Count result: {count_result.fetchone()}")

        # 2. Test different aggregation functions
        print("\n2. Testing different aggregation functions")
        agg_df = AsterixDataFrame(conn, "TestDF.Sales")
        # Test multiple aggregations in one call
        multi_agg = agg_df.agg({
            "price": "AVG",
            "quantity": "SUM"
        }).execute()
        print("Multiple aggregations:")
        print(multi_agg.fetchone())

        # 3. Test group by with aggregation
        print("\n3. Testing group_by() with aggregation")
        group_df = AsterixDataFrame(conn, "TestDF.Sales")
        category_agg = group_df.select(["category"]) \
                            .group_by("category") \
                            .agg({"price": "AVG", "quantity": "SUM"}) \
                            .execute()
        print("Aggregation by category:")
        for row in category_agg.fetchall():
            print(row)

        # 4. Test group by with multiple groups
        print("\n4. Testing group_by() with multiple dimensions")
        multi_group_df = AsterixDataFrame(conn, "TestDF.Sales")
        region_category_agg = multi_group_df.select(["region", "category"]) \
                                        .group_by(["region", "category"]) \
                                        .agg({"quantity": "SUM"}) \
                                        .execute()
        print("Aggregation by region and category:")
        for row in region_category_agg.fetchall():
            print(row)

        # 5. Test aggregation with ordering
        print("\n5. Testing aggregation with ordering")
        ordered_agg_df = AsterixDataFrame(conn, "TestDF.Sales")
        ordered_agg = ordered_agg_df.select(["category"]) \
                                .group_by("category") \
                                .agg({"price": "AVG"}) \
                                .order_by("price_avg", desc=True) \
                                .execute()
        print("Categories ordered by average price (descending):")
        for row in ordered_agg.fetchall():
            print(row)

    finally:
        # Clean up
        print("\nCleaning up...")
        cursor.execute("DROP DATAVERSE TestDF IF EXISTS;")
        print("Test completed.")
        conn.close()

if __name__ == "__main__":
    test_dataframe_aggregation()