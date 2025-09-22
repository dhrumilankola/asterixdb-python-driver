import os
import sys
import time
from datetime import datetime

# Add the root path to the system path for imports
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, root_path)

from src.pyasterix import (
    connect, 
    ObservabilityConfig, 
    MetricsConfig, 
    TracingConfig, 
    LoggingConfig,
    initialize_observability
) 

def print_header(title, description):
    """Print a formatted header for each query example."""
    print("\n" + "=" * 80)
    print(f"** {title} **")
    print("-" * 80)
    print(f"{description}")
    print("-" * 80)

def execute_query(cursor, query, title):
    """Execute a query and print results with proper formatting."""
    print(f"Executing Query: \n{query}")
    start_time = time.time()
    cursor.execute(query)
    end_time = time.time()
    
    results = cursor.fetchall()
    execution_time = end_time - start_time
    
    print(f"Execution time: {execution_time:.6f} seconds")
    print(f"Results ({len(results)} items):")
    
    # Format results for better readability
    for i, result in enumerate(results):
        if i < 10:  # Limit to first 10 results if there are many
            print(f"  {result}")
        elif i == 10:
            print(f"  ... and {len(results) - 10} more items")
            break
    
    return results

def setup_observability():
    """Setup observability for PEP249 basic queries testing."""
    config = ObservabilityConfig(
        metrics=MetricsConfig(
            enabled=True,
            namespace="pyasterix_pep249_basic",
            prometheus_port=8002
        ),
        tracing=TracingConfig(
            enabled=True,
            service_name="pep249_basic_queries",
            sample_rate=1.0,
            exporter="console"
        ),
        logging=LoggingConfig(
            structured=True,
            level="INFO",
            correlation_enabled=True,
            include_trace_info=True
        )
    )
    
    observability = initialize_observability(config)
    print("✅ Observability initialized for PEP249 basic queries")
    return observability

def test_queries():
    try:
        # Setup observability
        observability = setup_observability()
        logger = observability.get_logger("pep249.basic_queries")
        
        # Initialize connection with observability
        with connect(
            host="localhost",
            port=19002,
            observability_config=observability.config
        ) as conn:
            
            # Start overall test span
            with observability.start_span("pep249.basic_queries_test", kind="INTERNAL") as test_span:
                logger.info("Starting PEP249 basic queries test", extra={
                    "test_type": "basic_queries",
                    "connection_type": "pep249"
                })
                
                print("\nConnection to AsterixDB established successfully with observability.")

                # Create a cursor
                cursor = conn.cursor()
                print("Database cursor created.")

                # Setup: Creating dataverse and datasets with observability tracking
                print("\nSetting up test environment: Creating necessary dataverse and datasets")
                
                with observability.start_span("pep249.setup_environment", kind="INTERNAL") as setup_span:
                    logger.info("Setting up test environment", extra={
                        "operation": "setup",
                        "task": "create_dataverse_datasets"
                    })
                    
                    setup_query = """
                DROP DATAVERSE TinySocial IF EXISTS;
                CREATE DATAVERSE TinySocial;
                USE TinySocial;

                CREATE TYPE ChirpUserType AS {
                    screenName: string,
                    lang: string,
                    friendsCount: int,
                    statusesCount: int,
                    name: string,
                    followersCount: int
                };

                CREATE TYPE ChirpMessageType AS closed {
                    chirpId: string,
                    user: ChirpUserType,
                    senderLocation: point?,
                    sendTime: datetime,
                    referredTopics: {{ string }},
                    messageText: string
                };

                CREATE TYPE EmploymentType AS {
                    organizationName: string,
                    startDate: date,
                    endDate: date?
                };

                CREATE TYPE GleambookUserType AS {
                    id: int,
                    alias: string,
                    name: string,
                    userSince: datetime,
                    friendIds: {{ int }},
                    employment: [EmploymentType],
                    nickname: string?
                };

                CREATE TYPE GleambookMessageType AS {
                    messageId: int,
                    authorId: int,
                    inResponseTo: int?,
                    senderLocation: point?,
                    message: string
                };

                CREATE DATASET GleambookUsers(GleambookUserType)
                    PRIMARY KEY id;

                CREATE DATASET GleambookMessages(GleambookMessageType)
                    PRIMARY KEY messageId;

                CREATE DATASET ChirpUsers(ChirpUserType)
                    PRIMARY KEY screenName;

                CREATE DATASET ChirpMessages(ChirpMessageType)
                    PRIMARY KEY chirpId;
            """
            cursor.execute(setup_query)
            print("Dataverse and datasets created successfully.")

            # Insert sample data
            print("\nPopulating datasets with sample data")
            
            # Insert GleambookUsers data
            gleambook_users_data = """
                USE TinySocial;
                INSERT INTO GleambookUsers([
                    {"id":1,"alias":"Margarita","name":"MargaritaStoddard","nickname":"Mags","userSince":datetime("2012-08-20T10:10:00"),"friendIds":{{2,3,6,10}},"employment":[{"organizationName":"Codetechno","startDate":date("2006-08-06")},{"organizationName":"geomedia","startDate":date("2010-06-17"),"endDate":date("2010-01-26")}],"gender":"F"},
                    {"id":2,"alias":"Isbel","name":"IsbelDull","nickname":"Izzy","userSince":datetime("2011-01-22T10:10:00"),"friendIds":{{1,4}},"employment":[{"organizationName":"Hexviafind","startDate":date("2010-04-27")}]},
                    {"id":3,"alias":"Emory","name":"EmoryUnk","userSince":datetime("2012-07-10T10:10:00"),"friendIds":{{1,5,8,9}},"employment":[{"organizationName":"geomedia","startDate":date("2010-06-17"),"endDate":date("2010-01-26")}]},
                    {"id":4,"alias":"Nicholas","name":"NicholasStroh","userSince":datetime("2010-12-27T10:10:00"),"friendIds":{{2}},"employment":[{"organizationName":"Zamcorporation","startDate":date("2010-06-08")}]},
                    {"id":5,"alias":"Von","name":"VonKemble","userSince":datetime("2010-01-05T10:10:00"),"friendIds":{{3,6,10}},"employment":[{"organizationName":"Kongreen","startDate":date("2010-11-27")}]},
                    {"id":6,"alias":"Willis","name":"WillisWynne","userSince":datetime("2005-01-17T10:10:00"),"friendIds":{{1,3,7}},"employment":[{"organizationName":"jaydax","startDate":date("2009-05-15")}]},
                    {"id":7,"alias":"Suzanna","name":"SuzannaTillson","userSince":datetime("2012-08-07T10:10:00"),"friendIds":{{6}},"employment":[{"organizationName":"Labzatron","startDate":date("2011-04-19")}]},
                    {"id":8,"alias":"Nila","name":"NilaMilliron","userSince":datetime("2008-01-01T10:10:00"),"friendIds":{{3}},"employment":[{"organizationName":"Plexlane","startDate":date("2010-02-28")}]},
                    {"id":9,"alias":"Woodrow","name":"WoodrowNehling","nickname":"Woody","userSince":datetime("2005-09-20T10:10:00"),"friendIds":{{3,10}},"employment":[{"organizationName":"Zuncan","startDate":date("2003-04-22"),"endDate":date("2009-12-13")}]},
                    {"id":10,"alias":"Bram","name":"BramHatch","userSince":datetime("2010-10-16T10:10:00"),"friendIds":{{1,5,9}},"employment":[{"organizationName":"physcane","startDate":date("2007-06-05"),"endDate":date("2011-11-05")}]}
                ]);
            """
            cursor.execute(gleambook_users_data)
            
            # Insert GleambookMessages data
            gleambook_messages_data = """
                USE TinySocial;
                INSERT INTO GleambookMessages([
                    {"messageId":1,"authorId":3,"inResponseTo":2,"senderLocation":point("47.16,77.75"),"message":" love product-b its shortcut-menu is awesome:)"},
                    {"messageId":2,"authorId":1,"inResponseTo":4,"senderLocation":point("41.66,80.87"),"message":" dislike x-phone its touch-screen is horrible"},
                    {"messageId":3,"authorId":2,"inResponseTo":4,"senderLocation":point("48.09,81.01"),"message":" like product-y the plan is amazing"},
                    {"messageId":4,"authorId":1,"inResponseTo":2,"senderLocation":point("37.73,97.04"),"message":" can't stand acast the network is horrible:("},
                    {"messageId":5,"authorId":6,"inResponseTo":2,"senderLocation":point("34.7,90.76"),"message":" love product-b the customization is mind-blowing"},
                    {"messageId":6,"authorId":2,"inResponseTo":1,"senderLocation":point("31.5,75.56"),"message":" like product-z its platform is mind-blowing"},
                    {"messageId":7,"authorId":5,"inResponseTo":15,"senderLocation":point("32.91,85.05"),"message":" dislike product-b the speed is horrible"},
                    {"messageId":8,"authorId":1,"inResponseTo":11,"senderLocation":point("40.33,80.87"),"message":" like ccast the 3G is awesome:)"},
                    {"messageId":9,"authorId":3,"inResponseTo":12,"senderLocation":point("34.45,96.48"),"message":" love ccast its wireless is good"},
                    {"messageId":10,"authorId":1,"inResponseTo":12,"senderLocation":point("42.5,70.01"),"message":" can't stand product-w the touch-screen is terrible"},
                    {"messageId":11,"authorId":1,"inResponseTo":1,"senderLocation":point("38.97,77.49"),"message":" can't stand acast its plan is terrible"},
                    {"messageId":12,"authorId":10,"inResponseTo":6,"senderLocation":point("42.26,77.76"),"message":" can't stand product-z its voicemail-service is OMG:("},
                    {"messageId":13,"authorId":10,"inResponseTo":4,"senderLocation":point("42.77,78.92"),"message":" dislike x-phone the voice-command is bad:("},
                    {"messageId":14,"authorId":9,"inResponseTo":12,"senderLocation":point("41.33,85.28"),"message":" love acast its 3G is good:)"},
                    {"messageId":15,"authorId":7,"inResponseTo":11,"senderLocation":point("44.47,67.11"),"message":" like x-phone the voicemail-service is awesome"}
                ]);
            """
            cursor.execute(gleambook_messages_data)
            
            # Insert ChirpUsers data
            chirp_users_data = """
                USE TinySocial;
                INSERT INTO ChirpUsers([
                    {"screenName":"NathanGiesen@211","lang":"en","friendsCount":18,"statusesCount":473,"name":"Nathan Giesen","followersCount":49416},
                    {"screenName":"ColineGeyer@63","lang":"en","friendsCount":121,"statusesCount":362,"name":"Coline Geyer","followersCount":17159},
                    {"screenName":"NilaMilliron_tw","lang":"en","friendsCount":445,"statusesCount":164,"name":"Nila Milliron","followersCount":22649},
                    {"screenName":"ChangEwing_573","lang":"en","friendsCount":182,"statusesCount":394,"name":"Chang Ewing","followersCount":32136}
                ]);
            """
            cursor.execute(chirp_users_data)
            
            # Insert ChirpMessages data
            chirp_messages_data = """
                USE TinySocial;
                INSERT INTO ChirpMessages([
                    {"chirpId":"1","user":{"screenName":"NathanGiesen@211","lang":"en","friendsCount":39339,"statusesCount":473,"name":"Nathan Giesen","followersCount":49416},"senderLocation":point("47.44,80.65"),"sendTime":datetime("2008-04-26T10:10:00"),"referredTopics":{{"product-z","customization"}},"messageText":" love product-z its customization is good:)"},
                    {"chirpId":"2","user":{"screenName":"ColineGeyer@63","lang":"en","friendsCount":121,"statusesCount":362,"name":"Coline Geyer","followersCount":17159},"senderLocation":point("32.84,67.14"),"sendTime":datetime("2010-05-13T10:10:00"),"referredTopics":{{"ccast","shortcut-menu"}},"messageText":" like ccast its shortcut-menu is awesome:)"},
                    {"chirpId":"3","user":{"screenName":"NathanGiesen@211","lang":"en","friendsCount":39339,"statusesCount":473,"name":"Nathan Giesen","followersCount":49416},"senderLocation":point("29.72,75.8"),"sendTime":datetime("2006-11-04T10:10:00"),"referredTopics":{{"product-w","speed"}},"messageText":" like product-w the speed is good:)"},
                    {"chirpId":"4","user":{"screenName":"NathanGiesen@211","lang":"en","friendsCount":39339,"statusesCount":473,"name":"Nathan Giesen","followersCount":49416},"senderLocation":point("39.28,70.48"),"sendTime":datetime("2011-12-26T10:10:00"),"referredTopics":{{"product-b","voice-command"}},"messageText":" like product-b the voice-command is mind-blowing:)"},
                    {"chirpId":"5","user":{"screenName":"NathanGiesen@211","lang":"en","friendsCount":39339,"statusesCount":473,"name":"Nathan Giesen","followersCount":49416},"senderLocation":point("40.09,92.69"),"sendTime":datetime("2006-08-04T10:10:00"),"referredTopics":{{"product-w","speed"}},"messageText":" can't stand product-w its speed is terrible:("},
                    {"chirpId":"6","user":{"screenName":"ColineGeyer@63","lang":"en","friendsCount":121,"statusesCount":362,"name":"Coline Geyer","followersCount":17159},"senderLocation":point("47.51,83.99"),"sendTime":datetime("2010-05-07T10:10:00"),"referredTopics":{{"x-phone","voice-clarity"}},"messageText":" like x-phone the voice-clarity is good:)"},
                    {"chirpId":"7","user":{"screenName":"ChangEwing_573","lang":"en","friendsCount":182,"statusesCount":394,"name":"Chang Ewing","followersCount":32136},"senderLocation":point("36.21,72.6"),"sendTime":datetime("2011-08-25T10:10:00"),"referredTopics":{{"product-y","platform"}},"messageText":" like product-y the platform is good"},
                    {"chirpId":"8","user":{"screenName":"NathanGiesen@211","lang":"en","friendsCount":39339,"statusesCount":473,"name":"Nathan Giesen","followersCount":49416},"senderLocation":point("46.05,93.34"),"sendTime":datetime("2005-10-14T10:10:00"),"referredTopics":{{"product-z","shortcut-menu"}},"messageText":" like product-z the shortcut-menu is awesome:)"},
                    {"chirpId":"9","user":{"screenName":"NathanGiesen@211","lang":"en","friendsCount":39339,"statusesCount":473,"name":"Nathan Giesen","followersCount":49416},"senderLocation":point("36.86,74.62"),"sendTime":datetime("2012-07-21T10:10:00"),"referredTopics":{{"ccast","voicemail-service"}},"messageText":" love ccast its voicemail-service is awesome"},
                    {"chirpId":"10","user":{"screenName":"ColineGeyer@63","lang":"en","friendsCount":121,"statusesCount":362,"name":"Coline Geyer","followersCount":17159},"senderLocation":point("29.15,76.53"),"sendTime":datetime("2008-01-26T10:10:00"),"referredTopics":{{"ccast","voice-clarity"}},"messageText":" hate ccast its voice-clarity is OMG:("},
                    {"chirpId":"11","user":{"screenName":"NilaMilliron_tw","lang":"en","friendsCount":445,"statusesCount":164,"name":"Nila Milliron","followersCount":22649},"senderLocation":point("37.59,68.42"),"sendTime":datetime("2008-03-09T10:10:00"),"referredTopics":{{"x-phone","platform"}},"messageText":" can't stand x-phone its platform is terrible"},
                    {"chirpId":"12","user":{"screenName":"OliJackson_512","lang":"en","friendsCount":445,"statusesCount":164,"name":"Oli Jackson","followersCount":22649},"senderLocation":point("24.82,94.63"),"sendTime":datetime("2010-02-13T10:10:00"),"referredTopics":{{"product-y","voice-command"}},"messageText":" like product-y the voice-command is amazing:)"}
                ]);
            """
            cursor.execute(chirp_messages_data)
            print("Sample data inserted successfully.")

            # Run example queries
            # Query 0-A - Exact-Match Lookup
            print_header(
                "Query 0-A - Exact-Match Lookup",
                "Find a Gleambook user based on their ID. This query demonstrates a primary key lookup using SQL++."
            )
            query_0a = """
                USE TinySocial;

                SELECT VALUE user
                FROM GleambookUsers user
                WHERE user.id = 8;
            """
            execute_query(cursor, query_0a, "Exact-Match Lookup")
            
            # Query 0-B - Range Scan
            print_header(
                "Query 0-B - Range Scan", 
                "Find Gleambook users with IDs between 2 and 4. This demonstrates querying with range predicates."
            )
            query_0b = """
                USE TinySocial;

                SELECT VALUE user
                FROM GleambookUsers user
                WHERE user.id >= 2 AND user.id <= 4;
            """
            execute_query(cursor, query_0b, "Range Scan")
            
            # Query 1 - Date Range Filter
            print_header(
                "Query 1 - Date Range Filter",
                "Find users who joined between specified dates. This shows filtering with datetime comparison."
            )
            query_1 = """
                USE TinySocial;

                SELECT VALUE user
                FROM GleambookUsers user
                WHERE user.userSince >= datetime('2010-07-22T00:00:00')
                  AND user.userSince <= datetime('2012-07-29T23:59:59');
            """
            execute_query(cursor, query_1, "Date Range Filter")
            
            # Query 2-A - Equijoin
            print_header(
                "Query 2-A - Equijoin",
                "Join users with their messages to see who posted what. This demonstrates basic table joins in SQL++."
            )
            query_2a = """
                USE TinySocial;

                SELECT user.name AS uname, msg.message AS message
                FROM GleambookUsers user, GleambookMessages msg
                WHERE msg.authorId = user.id;
            """
            execute_query(cursor, query_2a, "Equijoin")
            
            # Query 2-B - Index Join
            print_header(
                "Query 2-B - Index Join with Hint",
                "Same as previous query but using an index join hint to influence query execution."
            )
            query_2b = """
                USE TinySocial;

                SELECT user.name AS uname, msg.message AS message
                FROM GleambookUsers user, GleambookMessages msg
                WHERE msg.authorId /*+ indexnl */ = user.id;
            """
            execute_query(cursor, query_2b, "Index Join")
            
            # Query 3 - Nested Outer Join
            print_header(
                "Query 3 - Nested Outer Join",
                "For each user, get their name and all messages they've authored. Shows nested queries and collections."
            )
            query_3 = """
                USE TinySocial;

                SELECT user.name AS uname,
                       (SELECT VALUE msg.message
                        FROM GleambookMessages msg
                        WHERE msg.authorId = user.id) AS messages
                FROM GleambookUsers user;
            """
            execute_query(cursor, query_3, "Nested Outer Join")
            
            # Query 4 - Theta Join (Spatial)
            print_header(
                "Query 4 - Theta Join with Spatial Function",
                "Find messages sent from locations near other messages. Demonstrates spatial functions in queries."
            )
            query_4 = """
                USE TinySocial;

                SELECT cm1.messageText AS message,
                       (SELECT VALUE cm2.messageText
                        FROM ChirpMessages cm2
                        WHERE spatial_distance(cm1.senderLocation, cm2.senderLocation) <= 1
                          AND cm2.chirpId < cm1.chirpId) AS nearbyMessages
                FROM ChirpMessages cm1;
            """
            execute_query(cursor, query_4, "Theta Join (Spatial)")
            
            # Query 5 - Fuzzy Join
            print_header(
                "Query 5 - Fuzzy Join with Similarity Function", 
                "Match Gleambook users with Chirp users who have similar names. Shows fuzzy search capabilities."
            )
            query_5 = """
                USE TinySocial;
                SET simfunction "edit-distance";
                SET simthreshold "3";

                SELECT gbu.id AS id, gbu.name AS name,
                       (SELECT cm.user.screenName AS chirpScreenname,
                               cm.user.name AS chirpName
                        FROM ChirpMessages cm
                        WHERE cm.user.name ~= gbu.name) AS similarUsers
                FROM GleambookUsers gbu;
            """
            execute_query(cursor, query_5, "Fuzzy Join")
            
            # Query 6 - Existential Quantification
            print_header(
                "Query 6 - Existential Quantification",
                "Find currently employed users using SOME quantifier. Shows how to query nested collections."
            )
            query_6 = """
                USE TinySocial;

                SELECT VALUE gbu
                FROM GleambookUsers gbu
                WHERE (SOME e IN gbu.employment SATISFIES e.endDate IS UNKNOWN);
            """
            execute_query(cursor, query_6, "Existential Quantification")
            
            # Query 7 - Universal Quantification
            print_header(
                "Query 7 - Universal Quantification",
                "Find unemployed users using EVERY quantifier. Shows universal quantification over collections."
            )
            query_7 = """
                USE TinySocial;

                SELECT VALUE gbu
                FROM GleambookUsers gbu
                WHERE (EVERY e IN gbu.employment SATISFIES e.endDate IS NOT UNKNOWN);
            """
            execute_query(cursor, query_7, "Universal Quantification")
            
            # Query 8 - Simple Aggregation
            print_header(
                "Query 8 - Simple Aggregation",
                "Count total number of Gleambook users. Demonstrates basic aggregation functions."
            )
            query_8 = """
                USE TinySocial;

                SELECT COUNT(gbu) AS numUsers FROM GleambookUsers gbu;
            """
            execute_query(cursor, query_8, "Simple Aggregation")
            
            # Query 9-A - Grouping and Aggregation
            print_header(
                "Query 9-A - Grouping and Aggregation",
                "Count chirps by user. Shows GROUP BY functionality with aggregation."
            )
            query_9a = """
                USE TinySocial;

                SELECT uid AS user, COUNT(cm) AS count
                FROM ChirpMessages cm
                GROUP BY cm.user.screenName AS uid;
            """
            execute_query(cursor, query_9a, "Grouping and Aggregation")
            
            # Query 9-B - Hash-Based Grouping
            print_header(
                "Query 9-B - Hash-Based Grouping with Hint",
                "Same as previous query but using hash-based grouping hint."
            )
            query_9b = """
                USE TinySocial;

                SELECT uid AS user, COUNT(cm) AS count
                FROM ChirpMessages cm
                 /*+ hash */
                GROUP BY cm.user.screenName AS uid;
            """
            execute_query(cursor, query_9b, "Hash-Based Grouping")
            
            # Query 10 - Grouping with Limits
            print_header(
                "Query 10 - Grouping with Limits",
                "Find top 3 users by number of chirps. Shows ordered aggregation with LIMIT."
            )
            query_10 = """
                USE TinySocial;

                SELECT uid AS user, c AS count
                FROM ChirpMessages cm
                GROUP BY cm.user.screenName AS uid WITH c AS count(cm)
                ORDER BY c DESC
                LIMIT 3;
            """
            execute_query(cursor, query_10, "Grouping with Limits")
            
            # Query 11 - Fuzzy Join on Topic Similarity
            print_header(
                "Query 11 - Fuzzy Join on Topic Similarity",
                "Find chirps with similar topics using Jaccard similarity. Demonstrates advanced similarity matching."
            )
            query_11 = """
                USE TinySocial;
                SET simfunction "jaccard";
                SET simthreshold "0.3";

                SELECT cm1.chirpId AS chirpId, cm1.messageText AS message,
                       (SELECT VALUE cm2.chirpId
                        FROM ChirpMessages cm2
                        WHERE cm2.referredTopics ~= cm1.referredTopics
                          AND cm2.chirpId > cm1.chirpId) AS similarChirps
                FROM ChirpMessages cm1;
            """
            execute_query(cursor, query_11, "Fuzzy Join on Topic Similarity")
            
            # Bonus Query: Insertions and Deletions
            print_header(
                "Bonus Query: Data Modification",
                "Demonstrate inserting and deleting data with SQL++."
            )
            
            # Insert a new chirp
            insert_query = """
                USE TinySocial;

                INSERT INTO ChirpMessages
                (
                   {"chirpId": "13",
                    "user":
                        {"screenName": "NathanGiesen@211",
                         "lang": "en",
                         "friendsCount": 39345,
                         "statusesCount": 479,
                         "name": "Nathan Giesen",
                         "followersCount": 49420
                        },
                    "senderLocation": point("47.44,80.65"),
                    "sendTime": datetime("2008-04-26T10:10:35"),
                    "referredTopics": {{"chirping"}},
                    "messageText": "chirpy chirp, my fellow chirpers!"
                   }
                );
            """
            print(f"Executing Insert: \n{insert_query}")
            cursor.execute(insert_query)
            print("Insertion completed.")
            
            # Verify the insertion
            verify_query = """
                USE TinySocial;
                SELECT VALUE cm
                FROM ChirpMessages cm
                WHERE cm.chirpId = "13";
            """
            print(f"\nVerifying insertion with query: \n{verify_query}")
            cursor.execute(verify_query)
            results = cursor.fetchall()
            print("Verification results:")
            for result in results:
                print(f"  {result}")
            
            # Delete the chirp
            delete_query = """
                USE TinySocial;
                DELETE FROM ChirpMessages cm WHERE cm.chirpId = "13";
            """
            print(f"\nExecuting Delete: \n{delete_query}")
            cursor.execute(delete_query)
            print("Deletion completed.")
            
            # Verify the deletion
            print(f"\nVerifying deletion with query: \n{verify_query}")
            cursor.execute(verify_query)
            results = cursor.fetchall()
            print(f"Verification results: {results} (empty means successful deletion)")

            # Cleanup
            print("\nCleaning up: Dropping dataverse")
            cleanup_query = "DROP DATAVERSE TinySocial IF EXISTS;"
            cursor.execute(cleanup_query)
            print("Cleanup completed.")

    except Exception as e:
        print(f"Error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("PyAsterixDB Demonstration - SQL++ Query Examples")
    print("================================================")
    print(f"Running at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    test_queries()