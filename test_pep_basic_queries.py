from src.pyasterix.connection import Connection

def test_queries():
    try:
        # Initialize connection
        with Connection(base_url="http://localhost:19002") as conn:
            print("\nConnection initialized.")

            # Create a cursor
            cursor = conn.cursor()
            print("Cursor created.")

            # Setup: Creating dataverse and datasets
            print("\nSetup: Creating necessary dataverse and datasets")
            cursor.execute("""
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
                    senderLocation: point
                };

                CREATE DATASET GleambookMessages(GleambookMessageType)
                    PRIMARY KEY messageId;
            """)
            print("Dataverse and datasets created.")

            # Insert sample data
            print("\nSetup: Inserting sample data")
            cursor.execute("""
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
            print("Sample data inserted.")

            # Test queries
            queries = [
                ("Simple arithmetic", "SELECT VALUE 1 + 1;"),
                ("Range Scan query", """
                    USE TinySocial;
                    SELECT VALUE user
                    FROM GleambookUsers user
                    WHERE user.id >= 2 AND user.id <= 4;
                """),
                ("Filter by date range", """
                    USE TinySocial;
                    SELECT VALUE user
                    FROM GleambookUsers user
                    WHERE user.userSince >= datetime('2010-07-22T00:00:00')
                      AND user.userSince <= datetime('2012-07-29T23:59:59');
                """),
                ("Simple join query", """
                    USE TinySocial;
                    SELECT user.name AS uname, msg.message AS message
                    FROM GleambookUsers user, GleambookMessages msg
                    WHERE msg.authorId = user.id;
                """),
                ("Index join with hint", """
                    USE TinySocial;
                    SELECT user.name AS uname, msg.message AS message
                    FROM GleambookUsers user, GleambookMessages msg
                    WHERE msg.authorId /*+ indexnl */ = user.id;
                """),
                ("Nested outer join", """
                    USE TinySocial;
                    SELECT user.name AS uname,
                           (SELECT VALUE msg.message
                            FROM GleambookMessages msg
                            WHERE msg.authorId = user.id) AS messages
                    FROM GleambookUsers user;
                """),
                ("Theta join with spatial distance", """
                    USE TinySocial;
                    SELECT cm1.message AS message,
                           (SELECT VALUE cm2.message
                            FROM GleambookMessages cm2
                            WHERE spatial_distance(cm1.senderLocation, cm2.senderLocation) <= 1
                              AND cm2.messageId < cm1.messageId) AS nearbyMessages
                    FROM GleambookMessages cm1;
                """),
                ("Existential query", """
                    USE TinySocial;
                    SELECT VALUE gbu
                    FROM GleambookUsers gbu
                    WHERE (SOME e IN gbu.employment SATISFIES e.endDate IS UNKNOWN);
                """),
                ("Universal query", """
                    USE TinySocial;
                    SELECT VALUE gbu
                    FROM GleambookUsers gbu
                    WHERE (EVERY e IN gbu.employment SATISFIES e.endDate IS NOT UNKNOWN);
                """)
            ]

            for test_name, query in queries:
                print(f"\nTest: {test_name}")
                cursor.execute(query)
                results = cursor.fetchall()
                print(f"Results: {results}")

            # Cleanup
            print("\nCleanup: Dropping dataverse")
            cursor.execute("DROP DATAVERSE TinySocial IF EXISTS;")
            print("Cleanup completed.")

    except Exception as e:
        print(f"Error occurred: {e}")


if __name__ == "__main__":
    test_queries()
