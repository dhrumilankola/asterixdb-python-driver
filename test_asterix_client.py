from src.pyasterix.asterix_client import AsterixClient
from datetime import datetime, timezone
from pprint import pprint

def test_basic_operations():
    """Test basic operations of AsterixClient"""
    
    # Initialize client
    print("\n1. Initializing AsterixClient...")
    client = AsterixClient(host="localhost", port=19002)
    
    try:
        
        
        # Create and use dataverse
        print("\n2. Creating and using dataverse...")
        client.create_dataverse("TestDataverse", if_not_exists=True)
        client.use_dataverse("TestDataverse")
        
        # Define user type schema
        print("\n3. Creating user type...")
        user_type_schema = {
            "id": "int64",
            "name": "string",
            "email": "string"  
        }

        client.create_type("UserType", user_type_schema)
        
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
        test_users = [
            {
                "id": 1,
                "name": "John Doe",
                "email": "john@example.com"
            },
            {
                "id": 2,
                "name": "Jane Smith",
                "email": "jane@example.com"
            }
        ]
        
        result = client.insert("Users", test_users)
        print("Inserted test users successfully: ", result)
        
        
        dataset_schema = client.get_schema("datatype", "UserType")
        print("\n Schema: ",dataset_schema)
                
        # Test find operations
        print("\n6. Testing find operations...")
        
        # Find all active users
        print("\na) Finding all active users:")
        active_users = client.find(
            "Users",
            condition={"is_active": True},
            projection=["id", "name", "email"]
        )
        pprint(active_users)
        
        # Find users older than 30
        print("\nb) Finding users older than 30:")
        older_users = client.find(
            "Users",
            condition={"age": {"$gt": 30}},
            projection=["name", "age"]
        )
        pprint(older_users)
        
        # Find premium users ordered by age
        print("\nc) Finding premium users ordered by age:")
        premium_users = client.find(
            "Users",
            condition={"tags": {"$contains": "premium"}},
            projection=["name", "age", "tags"],
            order_by={"age": "ASC"}
        )
        pprint(premium_users)
        
        # Test find_one
        print("\nd) Finding one user by ID:")
        user = client.find_one(
            "Users",
            condition={"id": 1}
        )
        pprint(user)
        
        # Test count
        print("\n7. Testing count operations...")
        
        # Count all users
        total_users = client.count("Users")
        print(f"Total users: {total_users}")
        
        # Count active users
        active_count = client.count("Users", {"is_active": True})
        print(f"Active users: {active_count}")
        
        # Test aggregation
        print("\n8. Testing aggregation...")
        stats = client.aggregate("Users", [
            {
                "$group": {
                    "by": ["is_active"],
                    "user_count": {"$count": "*"},
                    "avg_age": {"$avg": "age"},
                    "min_age": {"$min": "age"},
                    "max_age": {"$max": "age"}
                }
            }
        ])
        print("User statistics by active status:")
        pprint(stats)
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise
    
    finally:
        # # Cleanup
        # print("\n9. Cleaning up...")
        # client.drop_dataverse("TestDataverse")
        client.close()
        print("Test completed and cleanup done")

if __name__ == "__main__":
    test_basic_operations()