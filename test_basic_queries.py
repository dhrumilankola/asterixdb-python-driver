from src.pyasterix._http_client import AsterixDBHttpClient

def test_queries():
    # Initialize client
    client = AsterixDBHttpClient()

    try:
        # Setup: Creating dataverse and datasets
        print("\nSetup: Creating necessary dataverse and datasets")
        client.execute_query("""
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

            CREATE TYPE GleambookMessageType AS {
                messageId: int,
                authorId: int,
                message: string,
                senderLocation: point  // Ensures compatibility with spatial queries
            };

            CREATE DATASET GleambookMessages(GleambookMessageType)
                PRIMARY KEY messageId;
        """)
        print("Dataverse and datasets created")

        # Insert sample data
        print("\nSetup: Inserting sample data")
        client.execute_query("""
            USE TinySocial;

            INSERT INTO GleambookUsers([
                { "id": 1, "alias": "Willis", "name": "WillisWynne", "userSince": datetime("2005-01-17T10:10:00.000Z"), "friendIds": {{ 1, 3, 7 }}, "employment": [ { "organizationName": "jaydax", "startDate": date("2009-05-15") } ] },
                { "id": 2, "alias": "Isbel", "name": "IsbelDull", "userSince": datetime("2011-01-22T10:10:00.000Z"), "friendIds": {{ 1, 4 }}, "employment": [ { "organizationName": "Hexviafind", "startDate": date("2010-04-27") } ] }
            ]);

            INSERT INTO GleambookMessages([
                { "messageId": 1, "authorId": 1, "message": "I love AsterixDB", "senderLocation": point("34.7,90.76") },
                { "messageId": 2, "authorId": 2, "message": "Exploring SQL++ joins", "senderLocation": point("35.2,90.76") }
            ]);
        """)
        print("Sample data inserted")

        # Test 1: Simple arithmetic query
        print("\nTest 1: Simple arithmetic")
        result = client.execute_query("SELECT VALUE 1 + 1;")
        print("Arithmetic result:", result)

        # Test 2: Range Scan query
        print("\nTest 2: Range Scan query")
        result = client.execute_query("""
            USE TinySocial;
            SELECT VALUE user
            FROM GleambookUsers user
            WHERE user.id >= 2 AND user.id <= 4;
        """)
        print("Range Scan result:", result)

        # Test 3: Filter by date range
        print("\nTest 3: Filter by date range")
        result = client.execute_query("""
            USE TinySocial;
            SELECT VALUE user
            FROM GleambookUsers user
            WHERE user.userSince >= datetime('2010-07-22T00:00:00')
              AND user.userSince <= datetime('2012-07-29T23:59:59');
        """)
        print("Date Range Filter result:", result)

        # Test 4: Simple join query
        print("\nTest 4: Simple join query")
        result = client.execute_query("""
            USE TinySocial;
            SELECT user.name AS uname, msg.message AS message
            FROM GleambookUsers user, GleambookMessages msg
            WHERE msg.authorId = user.id;
        """)
        print("Simple join result:", result)

        # Test 5: Index join with hint
        print("\nTest 5: Index join with hint")
        result = client.execute_query("""
            USE TinySocial;
            SELECT user.name AS uname, msg.message AS message
            FROM GleambookUsers user, GleambookMessages msg
            WHERE msg.authorId /*+ indexnl */ = user.id;
        """)
        print("Index join result:", result)

        # Test 6: Nested outer join
        print("\nTest 6: Nested outer join")
        result = client.execute_query("""
            USE TinySocial;
            SELECT user.name AS uname,
                   (SELECT VALUE msg.message
                    FROM GleambookMessages msg
                    WHERE msg.authorId = user.id) AS messages
            FROM GleambookUsers user;
        """)
        print("Nested outer join result:", result)

        # Test 7: Theta join with spatial distance
        print("\nTest 7: Theta join with spatial distance")
        result = client.execute_query("""
            USE TinySocial;
            SELECT cm1.message AS message,
                   (SELECT VALUE cm2.message
                    FROM GleambookMessages cm2
                    WHERE spatial_distance(cm1.senderLocation, cm2.senderLocation) <= 1
                      AND cm2.messageId < cm1.messageId) AS nearbyMessages
            FROM GleambookMessages cm1;
        """)
        print("Theta join result:", result)

        # Test 8: Fuzzy join with edit distance
        print("\nTest 8: Fuzzy join with edit distance")
        result = client.execute_query("""
            USE TinySocial;
            SET simfunction "edit-distance";
            SET simthreshold "3";
            SELECT gbu.id AS id, gbu.name AS name,
                   (SELECT cm.authorId AS chirpScreenname,
                           cm.message AS chirpMessage
                    FROM GleambookMessages cm
                    WHERE cm.message ~= gbu.name) AS similarUsers
            FROM GleambookUsers gbu;
        """)
        print("Fuzzy join result:", result)

        # Test 9: Existential query
        print("\nTest 9: Existential query")
        result = client.execute_query("""
            USE TinySocial;
            SELECT VALUE gbu
            FROM GleambookUsers gbu
            WHERE (SOME e IN gbu.employment SATISFIES e.endDate IS UNKNOWN);
        """)
        print("Existential query result:", result)

        # Test 10: Universal query
        print("\nTest 10: Universal query")
        result = client.execute_query("""
            USE TinySocial;
            SELECT VALUE gbu
            FROM GleambookUsers gbu
            WHERE (EVERY e IN gbu.employment SATISFIES e.endDate IS NOT UNKNOWN);
        """)
        print("Universal query result:", result)

        # Cleanup
        print("\nCleanup: Dropping dataverse")
        client.execute_query("DROP DATAVERSE TinySocial IF EXISTS;")
        print("Cleanup completed")

    except Exception as e:
        print(f"Error occurred: {str(e)}")
    finally:
        client.close()

if __name__ == "__main__":
    test_queries()




