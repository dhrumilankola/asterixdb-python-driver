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
            projection=["name", "email"]
        )
        pprint(users)
                
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

        # Test update operations
        print("\n9. Testing update operations...")

        # Update single user
        print("\na) Updating user name:")
        update_result = client.update(
            dataset="Users",
            condition={"id": 1},
            updates={"name": "John Smith"}
        )
        print("Update result:", update_result)

        # Verify update
        updated_user = client.find_one("Users", {"id": 1})
        print("User after name update:", updated_user)

        # Update multiple fields
        print("\nb) Updating multiple fields:")
        update_result = client.update(
            dataset="Users",
            condition={"id": 2},
            updates={
                "name": "Jane Doe",
                "email": "jane.doe@example.com"
            }
        )
        print("Update result:", update_result)

        # Verify multiple updates
        updated_user = client.find_one("Users", {"id": 2})
        print("User after multiple field updates:", updated_user)

        # Verify all changes
        print("\nc) All users after updates:")
        all_users = client.find("Users")
        pprint(all_users)
        
        print("\n10. Cleaning up...")
        # Drop in reverse order of creation
        client.drop_dataset("Users", if_exists=True)
        client.drop_type("UserType", if_exists=True)
        client.drop_dataverse("TestDataverse", if_exists=True)
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
    finally:
        client.close()
        print("\nTest completed and cleanup done")

if __name__ == "__main__":
    test_basic_operations()

