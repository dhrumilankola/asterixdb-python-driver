import sys
import os
from requests.exceptions import HTTPError

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.pyasterix.connection import Connection
from src.pyasterix.cursor import Cursor
from src.pyasterix.exceptions import DatabaseError

# Initialize connection
try:
    conn = Connection(base_url="http://localhost:19002")
    cursor = conn.cursor()
    print("[INFO] Connection established successfully.")
except Exception as e:
    print(f"[ERROR] Failed to establish connection: {e}")
    sys.exit(1)

def execute_query(query, params=None):
    """Helper function to execute queries with debugging."""
    try:
        print("\n[DEBUG] Executing Query:")
        print(query)
        if params:
            print(f"[DEBUG] Parameters: {params}")
        
        cursor.execute(query, params)
        result = cursor.fetchall()
        print(f"[INFO] Query executed successfully. Retrieved {len(result)} rows.")
        for row in result:
            print(row)

    except HTTPError as http_err:
        print(f"[HTTP ERROR] {http_err}")
        if http_err.response is not None:
            print(f"[HTTP RESPONSE] {http_err.response.text}")
    except DatabaseError as db_err:
        print(f"[DATABASE ERROR] {db_err}")
    except Exception as err:
        print(f"[UNEXPECTED ERROR] {err}")

# Step 1: Create Dataverse and Dataset
setup_query = """
DROP DATAVERSE TestDF IF EXISTS;
CREATE DATAVERSE TestDF;
USE TestDF;

CREATE TYPE UserType AS {
    id: int,
    name: string,
    age: int,
    city: string
};

CREATE DATASET Users(UserType) PRIMARY KEY id;
"""
execute_query(setup_query)

print("\n[TEST] Inserting Sample Users...")
users_data = [
    {"id": 1, "name": "Alice", "age": 25, "city": "New York"},
    {"id": 2, "name": "Bob", "age": 30, "city": "San Francisco"},
    {"id": 3, "name": "Charlie", "age": 35, "city": "Chicago"}
]
execute_query("USE TestDF; INSERT INTO Users(?);", [users_data])

# Step 3: Test Parameterized Queries
print("\n[TEST] Parameterized Query - Fetch user by ID")
execute_query("SELECT * FROM TestDF.Users WHERE id = ?", [1])

print("\n[TEST] Parameterized Query - Fetch users within an ID range")
execute_query("SELECT * FROM TestDF.Users WHERE id >= ? AND id <= ?", [1, 2])

# Step 4: Insert Single User Using Dictionary (Corrected)
print("\n[TEST] Insert Single User with Dictionary")
user_data = {"id": 4, "name": "David", "age": 40, "city": "Los Angeles"}
execute_query("USE TestDF; INSERT INTO Users(?);", [user_data])

# Step 5: Insert Multiple Users (Corrected)
print("\n[TEST] Insert Multiple Users")
users_data = [
    {"id": 5, "name": "Eve", "age": 28, "city": "Miami"},
    {"id": 6, "name": "Frank", "age": 33, "city": "Seattle"}
]
execute_query("USE TestDF; INSERT INTO Users(?);", [users_data])

# Step 6: Verify Inserted Data
print("\n[TEST] Fetch All Users")
execute_query("SELECT * FROM TestDF.Users;")

# Step 7: Cleanup
print("\n[CLEANUP] Dropping Test Dataverse")
execute_query("DROP DATAVERSE TestDF IF EXISTS;")

# Close connection
conn.close()
print("[INFO] Connection closed.")
