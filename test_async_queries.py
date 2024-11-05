from src.pyasterix._http_client import AsterixDBHttpClient
import time

def test_async_queries():
    client = AsterixDBHttpClient()

    try:
        # Setup: Create necessary dataverse and dataset
        print("\nSetup: Creating dataverse and dataset for async test")
        setup_result = client.execute_query("""
            DROP DATAVERSE TinySocial IF EXISTS;
            CREATE DATAVERSE TinySocial;
            USE TinySocial;

            CREATE TYPE GleambookUserType AS {
                id: int,
                alias: string,
                name: string,
                userSince: datetime,
                friendIds: {{ int }},
                employment: [ { organizationName: string, startDate: date, endDate: date? } ],
                nickname: string?
            };

            CREATE DATASET GleambookUsers(GleambookUserType)
                PRIMARY KEY id;
        """)
        print("Dataverse and dataset created")

        # Insert sample data using immediate mode (since it's a DML operation)
        print("\nInserting sample data")
        insert_result = client.execute_query("""
            USE TinySocial;

            INSERT INTO GleambookUsers([
                { "id": 1, "alias": "Willis", "name": "WillisWynne", "userSince": datetime("2005-01-17T10:10:00.000Z"), "friendIds": {{ 1, 3, 7 }}, "employment": [ { "organizationName": "jaydax", "startDate": date("2009-05-15") } ] },
                { "id": 2, "alias": "Isbel", "name": "IsbelDull", "userSince": datetime("2011-01-22T10:10:00.000Z"), "friendIds": {{ 1, 4 }}, "employment": [ { "organizationName": "Hexviafind", "startDate": date("2010-04-27") } ] }
            ]);
        """)
        print("Insert completed:", insert_result)

        # Test 1: Asynchronous select query
        print("\nTest 1: Asynchronous select query")
        async_result = client.execute_query("""
            USE TinySocial;
            SELECT u.id, u.name, u.alias 
            FROM GleambookUsers u 
            WHERE u.id >= 1 
            ORDER BY u.id;
        """, mode="async")

        print("Async Query Submitted. Awaiting completion...")
        print("Initial async response:", async_result)

        if not async_result.get('handle'):
            print("Error: No handle received for async query")
            return

        # Polling for completion
        attempt = 1
        max_attempts = 10
        while attempt <= max_attempts:
            status_result = client._get_query_status(async_result['handle'])
            print(f"Status check {attempt}: {status_result}")
            
            if status_result.get("status") == "SUCCESS":
                print("Async query completed successfully.")
                final_result = client._get_query_result(status_result['handle'])
                print("Final result:", final_result)
                break
            elif status_result.get("status") in ("FAILED", "FATAL"):
                print(f"Async query failed with status: {status_result.get('status')}")
                break
            
            print(f"Attempt {attempt}/{max_attempts}: Query still running...")
            attempt += 1
            time.sleep(1)  # Increased sleep time to 1 second

        # Test 2: Try another async query with a longer operation
        print("\nTest 2: Another async query with aggregation")
        async_result2 = client.execute_query("""
            USE TinySocial;
            SELECT 
                u.employment[0].organizationName as org,
                COUNT(*) as emp_count
            FROM GleambookUsers u
            GROUP BY u.employment[0].organizationName
            ORDER BY emp_count DESC;
        """, mode="async")

        print("Second async query submitted:", async_result2)

        # Cleanup: Dropping the dataverse
        print("\nCleanup: Dropping dataverse")
        cleanup_result = client.execute_query("DROP DATAVERSE TinySocial IF EXISTS;")
        print("Cleanup completed:", cleanup_result)

    except Exception as e:
        print(f"Error occurred during async test: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()

if __name__ == "__main__":
    test_async_queries()