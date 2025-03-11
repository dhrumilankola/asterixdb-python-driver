from src.pyasterix._http_client import AsterixDBHttpClient
from src.pyasterix.connection import Connection
from src.pyasterix.dataframe import AsterixDataFrame
import pandas as pd

def setup_test_data():
    """Set up the test data."""
    client = AsterixDBHttpClient()
    try:
        print("\nSetting up the test dataverse and dataset...")
        client.execute_query("""
            DROP DATAVERSE test IF EXISTS;
            CREATE DATAVERSE test;
            USE test;

            CREATE TYPE CustomerType AS {
                custid: string,
                name: string,
                age: int32,
                address: {
                    street: string,
                    city: string,
                    zipcode: string?
                },
                rating: int32?
            };

            CREATE DATASET Customers(CustomerType)
                PRIMARY KEY custid;

            INSERT INTO Customers([
                {
                    "custid": "C1",
                    "name": "Alice",
                    "age": 30,
                    "address": {
                        "street": "123 Main St",
                        "city": "St. Louis, MO",
                        "zipcode": "63101"
                    },
                    "rating": 700
                },
                {
                    "custid": "C2",
                    "name": "Bob",
                    "age": 40,
                    "address": {
                        "street": "456 Elm St",
                        "city": "Boston, MA",
                        "zipcode": "02118"
                    },
                    "rating": 600
                },
                {
                    "custid": "C3",
                    "name": "Charlie",
                    "age": 35,
                    "address": {
                        "street": "789 Oak St",
                        "city": "Chicago, IL",
                        "zipcode": "60622"
                    },
                    "rating": 650
                }
            ]);
        """)
        print("Test data inserted successfully.")
    finally:
        client.close()

def test_asterix_dataframe_operations():
    """Test AsterixDataFrame operations with proper query context management."""
    with Connection(base_url="http://localhost:19002") as conn:
        df = AsterixDataFrame(conn, "test.Customers")

        # Test 1: Filter by a single condition
        df_stl = df[df['address.city'] == "St. Louis, MO"]
        print("\nAfter filtering by city (address.city == 'St. Louis, MO'):")
        print(df_stl.execute())

        # Test 2: Filter and select specific columns
        filtered_df = df[df["age"] > 25][["name", "age"]]
        print("\nAfter filtering where age > 25 and selecting columns (name, age):")
        print(filtered_df.execute())

        # Test 3: Filter using mask() (aligned with current behavior)
        df_masked = df[df['rating'] <= 600]  # Avoid using NOT conditions unnecessarily
        print("\nAfter masking rows where rating > 600 (current implementation excludes rows):")
        print(df_masked.execute())

        # Pandas-like behavior demonstration
        print("\nNOTE: Pandas mask would return rows with NaN for non-matching rows, not exclude them.")
        print("Example Pandas-like output (for illustrative purposes):")
        print(
            pd.DataFrame({"name": ["Alice", None, "Charlie"], "rating": [500, None, 450]})
        )

        # Test 4: Use isin() to filter rows
        df_cities = df[['name', 'age', 'address.city', 'rating']]
        df_isin = df_cities[df_cities['address.city'].isin(["St. Louis, MO", "Boston, MA"])]
        print("\nAfter filtering using isin(['St. Louis, MO', 'Boston, MA']):")
        print(df_isin.execute())

        # Test 5: Use between() to filter rows
        df_rating_range = df[['name', 'age', 'rating']]
        df_between = df_rating_range[df_rating_range['rating'].between(600, 750)]
        print("\nAfter filtering ratings between 600 and 750:")
        print(df_between.execute())

        # Test 6: Select specific columns
        df_filtered_items = df[['name', 'rating']]
        print("\nAfter selecting columns (name, rating):")
        print(df_filtered_items.execute())

        # Test 7: Column slice (use explicit selection or indices)
        df_column_slice = df[['name', 'age', 'rating']]
        print("\nAfter slicing columns between 'name' and 'rating':")
        print(df_column_slice.execute())

        # Test 8: Limit the number of results
        df_limited = df.limit(2)
        print("\nAfter limiting results to 2 rows:")
        print(df_limited.execute())

        # Test 9: Apply offset
        df_offset = df.limit(2).offset(1)
        print("\nAfter applying offset(1):")
        print(df_offset.execute())

        # Test 10: Combine multiple filters with AND
        combined_filter = df[(df['age'] > 25) & (df['rating'] >= 600)]
        print("\nAfter filtering where age > 25 AND rating >= 600:")
        print(combined_filter.execute())

        # Test 11: Combine multiple filters with OR
        combined_filter_or = df[(df['age'] < 30) | (df['rating'] > 700)]
        print("\nAfter filtering where age < 30 OR rating > 700:")
        print(combined_filter_or.execute())

        # Test 12: Filter with NOT condition
        df_not_filter = df[~(df['age'] < 30)]
        print("\nAfter filtering NOT (age < 30):")
        print(df_not_filter.execute())


        

if __name__ == "__main__":
    setup_test_data()
    test_asterix_dataframe_operations()
