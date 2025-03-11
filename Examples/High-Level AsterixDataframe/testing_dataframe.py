from src.pyasterix._http_client import AsterixDBHttpClient
from src.pyasterix.connection import Connection
from src.pyasterix.dataframe import AsterixDataFrame

def setup_test_data():
    """Set up test dataverse and dataset."""
    client = AsterixDBHttpClient()  # Initialize the client locally
    try:
        print("\nSetting up the test dataverse and dataset...")
        
        # Combine all dataverse, type, and dataset creation into one query
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
        """)
        print("Dataverse, type, and dataset created successfully.")

        # Insert test data
        print("\nInserting test data into Customers dataset...")
        client.execute_query("""
            USE test;

            INSERT INTO Customers([
                {
                    "custid": "C13",
                    "name": "T. Cody",
                    "age": 35,
                    "address": {
                        "street": "201 Main St.",
                        "city": "St. Louis, MO",
                        "zipcode": "63101"
                    },
                    "rating": 750
                },
                {
                    "custid": "C25",
                    "name": "M. Sinclair",
                    "age": 28,
                    "address": {
                        "street": "690 River St.",
                        "city": "Hanover, MA",
                        "zipcode": "02340"
                    },
                    "rating": 690
                },
                {
                    "custid": "C31",
                    "name": "B. Pruitt",
                    "age": 45,
                    "address": {
                        "street": "360 Mountain Ave.",
                        "city": "St. Louis, MO",
                        "zipcode": "63101"
                    }
                },
                {
                    "custid": "C35",
                    "name": "J. Roberts",
                    "age": 22,
                    "address": {
                        "street": "420 Green St.",
                        "city": "Boston, MA",
                        "zipcode": "02115"
                    },
                    "rating": 565
                }
            ]);
        """)
        print("Test data inserted successfully.")

        print("\nTest setup completed successfully!")

    except Exception as e:
        print(f"Error setting up test data: {str(e)}")
        raise
    finally:
        client.close()


def test_basic_queries():
    """Test basic DataFrame operations."""
    # Use Connection instead of AsterixDBHttpClient
    with Connection(base_url="http://localhost:19002") as conn:
        try:
            # Create cursor for verification query
            cursor = conn.cursor()
            
            # Verify dataset exists
            print("\nVerifying test dataset exists...")
            verify_query = """SELECT VALUE ds FROM Metadata.`Dataset` ds 
                            WHERE ds.DatasetName = 'Customers';"""
            cursor.execute(verify_query)
            result = cursor.fetchall()
            if not result:
                raise Exception("Test dataset not found! Please run setup first.")

            # Create DataFrame with connection
            df = AsterixDataFrame(conn, "test.Customers")
            print("\nTesting basic queries:")
            
            # Test 1: Simple selection with specific columns
            print("\nTest 1: Select specific columns from customers")
            result = df[['custid', 'name', 'age']].execute()
            print(f"Selected customers: {result}")
            
            # Test 2: Filter with condition using AsterixAttribute
            print("\nTest 2: Select customers with age > 30")
            result = df[df['age'] > 30][['name', 'age']].execute()
            print(f"Filtered customers: {result}")
            
            # Test 3: Multiple conditions
            print("\nTest 3: Select customers with age > 30 and name starting with 'T'")
            result = df[
                (df['age'] > 30) & (df['name'].like('T%'))
            ][['name', 'age']].execute()
            print(f"Filtered customers with multiple conditions: {result}")
            
            # Test 4: Complex conditions
            print("\nTest 4: Select customers with age > 30 or rating > 700")
            result = df[
                (df['age'] > 30) | (df['rating'] > 700)
            ][['name', 'age', 'rating']].execute()
            print(f"Filtered customers with OR conditions: {result}")
            
        except Exception as e:
            print(f"Test failed: {str(e)}")
            raise

def main():
    try:
        # Set up test data
        setup_test_data()
        
        # Run tests
        test_basic_queries()
        
    except Exception as e:
        print(f"Test failed: {str(e)}")
    finally:
        # Clean up (optional)
        try:
            client = AsterixDBHttpClient()
            client.execute_query("DROP DATAVERSE test IF EXISTS;")
            client.close()
        except:
            pass

if __name__ == "__main__":
    main()