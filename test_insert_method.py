import logging
from src.pyasterix.asterix_client import AsterixClient, AsterixClientError, QueryExecutionError

# Configure logging for detailed debug output
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def test_insert_method():
    client = AsterixClient()
    
    try:
        # Step 1: Create and Use Dataverse
        print("\n1. Creating and using Dataverse 'TestDataverse'...")
        try:
            client.create_dataverse("TestDataverse", if_not_exists=True)
            client.use_dataverse("TestDataverse")
            logger.debug("Dataverse created and selected.")
        except QueryExecutionError as e:
            print("Failed at creating/using Dataverse:", str(e))
            return
        
        # Step 2: Define and Create User Type
        print("\n2. Defining and creating UserType...")
        user_type_schema = {
            "id": "int64",
            "name": "string",
            "email": "string"
        }
        
        try:
            client.create_type("UserType", user_type_schema)
            logger.debug("UserType created with schema: %s", user_type_schema)
        except QueryExecutionError as e:
            print("Failed at creating UserType:", str(e))
            return

        # Step 3: Create Dataset
        print("\n3. Creating Users dataset...")
        try:
            client.create_dataset(
                name="Users",
                type_name="UserType",
                primary_key="id",
                if_not_exists=True
            )
            logger.debug("Dataset 'Users' created with UserType as schema.")
        except QueryExecutionError as e:
            print("Failed at creating dataset:", str(e))
            return

        # Step 4: Prepare and Insert Test Data
        print("\n4. Preparing test data for insertion...")
        test_users = [
            {"id": 1, "name": "John Doe", "email": "john@example.com"},
            {"id": 2, "name": "Jane Smith", "email": "jane@example.com"}
        ]
        logger.debug("Test data prepared for insertion: %s", test_users)

        # Step 5: Attempt Insertion via client.insert
        print("\n5. Inserting test data...")
        try:
            result = client.insert("Users", test_users)
            print("Insertion successful. Result:", result)
        except QueryExecutionError as e:
            print("Insertion failed with QueryExecutionError:", str(e))

    except (AsterixClientError, QueryExecutionError) as e:
        print("Test setup failed due to an error:", str(e))
    finally:
        # Cleanup: Drop the dataverse to reset state
        print("\nCleanup: Dropping 'TestDataverse'...")
        try:
            client.drop_dataverse("TestDataverse", if_exists=True)
            logger.debug("Dataverse 'TestDataverse' dropped.")
        except QueryExecutionError as e:
            print("Cleanup failed:", str(e))
        client.close()
        print("Test completed.")

# Run the test function directly for detailed output
if __name__ == "__main__":
    test_insert_method()
