from src.pyasterix.asterix_client import AsterixClient
from datetime import datetime, timezone
from pprint import pprint

def test_complex_operations():
    """Test complex operations of AsterixClient"""
    
    # Initialize client
    print("\n1. Initializing AsterixClient...")
    client = AsterixClient(host="localhost", port=19002)
    
    try:
        # Create and use dataverse
        print("\n2. Creating and using dataverse...")
        client.create_dataverse("TestDataverse", if_not_exists=True)
        client.use_dataverse("TestDataverse")
        
        # Define address type schema
        print("\n3. Creating types...")
        address_type_schema = {
            "street": "string",
            "city": "string",
            "state": "string",
            "zip": "string"
        }
        client.create_type("AddressType", address_type_schema)
        print("Created AddressType successfully")

        # Define user type schema
        user_type_schema = {
            "id": "int64",
            "name": "string",
            "email": "string",
            "age": "int64",
            "joined_date": "datetime",
            "is_active": "boolean",
            "address": "AddressType",
            "score": "double",
            "tags": ["string"],  # Array of strings
            "preferences": {  # Nested record
                "theme": "string",
                "notifications": "string"
            }
        }
        client.create_type("UserType", user_type_schema)
        print("Created UserType successfully")
        
        # Create dataset
        print("\n4. Creating users dataset...")
        client.create_dataset(
            name="Users",
            type_name="UserType",
            primary_key="id",
            if_not_exists=True
        )
        
        # Insert test data
        print("\n5. Inserting test users...")
        current_time = datetime.now(timezone.utc)
        test_users = [
            {
                "id": 1,
                "name": "John Doe",
                "email": "john@example.com",
                "age": 30,
                "joined_date": current_time,
                "is_active": True,
                "address": {
                    "street": "123 Main St",
                    "city": "New York",
                    "state": "NY",
                    "zip": "10001"
                },
                "score": 85.5,
                "tags": ["premium", "verified"],
                "preferences": {
                    "theme": "dark",
                    "notifications": "enabled"
                }
            },
            {
                "id": 2,
                "name": "Jane Smith",
                "email": "jane@example.com",
                "age": 25,
                "joined_date": current_time,
                "is_active": True,
                "address": {
                    "street": "456 Oak Ave",
                    "city": "Los Angeles",
                    "state": "CA",
                    "zip": "90001"
                },
                "score": 92.0,
                "tags": ["premium", "expert"],
                "preferences": {
                    "theme": "light",
                    "notifications": "disabled"
                }
            }
        ]
        
        result = client.insert("Users", test_users)
        print("Inserted test users successfully:", result)

        # Test find operations
        print("\n6. Testing find operations...")
        
        print("\na) Finding all users:")
        users = client.find("Users")
        pprint(users)

        print("\nb) Finding user by ID:")
        user = client.find_one(
            "Users",
            condition={"id": 1}
        )
        pprint(user)

        print("\nc) Finding users with projection:")
        users = client.find(
            "Users",
            projection=["name", "email", "age"]
        )
        pprint(users)

        print("\nd) Finding active users with age > 25:")
        users = client.find(
            dataset="Users",
            condition={
                "is_active": True,
                "age": {"$gt": 25}
            },
            projection=["name", "age", "is_active"]
        )
        pprint(users)

        # Add more test cases
        print("\ne) Finding users with complex conditions:")
        users = client.find(
            dataset="Users",
            condition={
                "score": {"$gte": 80, "$lte": 95},
                "address.city": "New York"
            },
            order_by={"score": "DESC"}
        )
        pprint(users)

        print("\nf) Finding premium users:")
        users = client.find(
            dataset="Users",
            condition={
                "tags": {"$contains": "premium"},
                "is_active": True
            },
            projection=["name", "tags", "score"]
        )
        pprint(users)
        
        client.drop_dataset("Users", if_exists=True)
        client.drop_type("UserType", if_exists=True)
        client.drop_dataverse("TestDataverse", if_exists=True)

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise
    
    finally:
        client.close()
        print("\nTest completed and cleanup done")
        
if __name__ == "__main__":
    test_complex_operations()