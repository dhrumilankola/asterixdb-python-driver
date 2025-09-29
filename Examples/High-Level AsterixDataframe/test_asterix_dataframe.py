#!/usr/bin/env python3
"""
Test script for AsterixDataFrame implementation.
This script tests the basic functionality of the AsterixDataFrame class using the 
TinySocial sample dataset that comes with AsterixDB documentation.
"""

import sys
import os
import pandas as pd
from datetime import datetime

# Add the project root to the path so we can import our modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import our modules
from src.pyasterix import (
    connect, 
    ObservabilityConfig, 
    MetricsConfig, 
    TracingConfig, 
    LoggingConfig,
    initialize_observability
)
from src.pyasterix.dataframe.base import AsterixDataFrame
from src.pyasterix.dataframe.attribute import AsterixAttribute
from src.pyasterix.exceptions import QueryError, ValidationError

def print_section(title):
    """Print a section header."""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)

def setup_observability():
    """Setup observability for DataFrame testing."""
    config = ObservabilityConfig(
        metrics=MetricsConfig(
            enabled=True,
            namespace="pyasterix_dataframe_test",
            prometheus_port=8004
        ),
        tracing=TracingConfig(
            enabled=True,
            service_name="dataframe_test_service",
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
    print("✅ Observability initialized for DataFrame testing")
    return observability

def test_asterix_dataframe():
    """Test the AsterixDataFrame implementation using TinySocial dataset with observability."""
    print_section("INITIALIZING CONNECTION WITH OBSERVABILITY")
    
    # Setup observability
    observability = setup_observability()
    logger = observability.get_logger("dataframe_test")
    
    # Connect to AsterixDB with observability
    try:
        with observability.start_span("dataframe_test.initialization", kind="INTERNAL") as init_span:
            logger.info("Initializing DataFrame test", extra={
                "test_type": "dataframe_operations",
                "dataset": "TinySocial"
            })
            
            conn = connect(
                host="localhost",
                port=19002,
                observability_config=observability.config
            )
            
            print("✓ Connection initialized successfully with observability")
            
            init_span.set_attribute("connection_status", "success")
            
    except Exception as e:
        print(f"✗ Failed to connect to AsterixDB: {e}")
        if observability:
            observability.record_metric(
                "dataframe_test.connection_failures",
                1,
                {"error_type": type(e).__name__}
            )
        return
    
    # Step 1: Create test dataverse and dataset
    print_section("CREATING TINYSOCIAL DATAVERSE AND DATASETS")
    try:
        cursor = conn.cursor()
        print("Database cursor created.")

        # Setup: Creating dataverse and datasets
        print("\nSetting up TinySocial environment")
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
        print("✓ Dataverse and datasets created successfully")
    except Exception as e:
        print(f"✗ Failed to create dataverse and datasets: {e}")
        return
    
    # Step 2: Insert test data
    print_section("INSERTING SAMPLE DATA")
    try:
        # Insert GleambookUsers data
        print("\nInserting GleambookUsers data")
        gleambook_users_data = """
            USE TinySocial;
            INSERT INTO GleambookUsers([
                {"id":1,"alias":"Margarita","name":"MargaritaStoddard","nickname":"Mags","userSince":datetime("2012-08-20T10:10:00"),"friendIds":{{2,3,6,10}},"employment":[{"organizationName":"Codetechno","startDate":date("2006-08-06")},{"organizationName":"geomedia","startDate":date("2010-06-17"),"endDate":date("2010-01-26")}]},
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
        print("✓ GleambookUsers data inserted successfully")
        
        # Insert GleambookMessages data
        print("\nInserting GleambookMessages data")
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
        print("✓ GleambookMessages data inserted successfully")
        
        # Insert ChirpUsers data
        print("\nInserting ChirpUsers data")
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
        print("✓ ChirpUsers data inserted successfully")
        
        # Insert ChirpMessages data
        print("\nInserting ChirpMessages data")
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
        print("✓ ChirpMessages data inserted successfully")
        
        print("\n✓ All sample data inserted successfully")
    except Exception as e:
        print(f"✗ Failed to insert sample data: {e}")
        return
    
    # Step 3: Test DataFrame operations
    print_section("TESTING DATAFRAME OPERATIONS")
    
    # Test 1: Basic Select from GleambookUsers
    print("\nTest 1: Basic Selection of Columns from GleambookUsers")
    try:
        users_df = AsterixDataFrame(conn, "TinySocial.GleambookUsers")
        result = users_df.select(["id", "name", "alias"]).execute()
        
        print(f"Query executed: {users_df._query}")
        print(f"Result count: {len(result.result_set)}")
        print("First row:", result.result_set[0] if result.result_set else "No results")
        
        # Convert to pandas for easy viewing
        pd_df = result.to_pandas()
        print("\nAs Pandas DataFrame:")
        print(pd_df.head())
        
        print("✓ Basic selection test passed")
    except Exception as e:
        print(f"✗ Basic selection test failed: {e}")
    
    # Test 2: Filtering GleambookUsers with simple predicate
    print("\nTest 2: Filtering GleambookUsers with Simple Predicate")
    try:
        users_df = AsterixDataFrame(conn, "TinySocial.GleambookUsers")
        id_attr = AsterixAttribute("id", users_df)
        
        # Users with id > 5
        result = users_df.filter(id_attr > 5).execute()
        
        print(f"Query executed: {users_df._query}")
        print(f"Result count: {len(result.result_set)}")
        print("First row:", result.result_set[0] if result.result_set else "No results")
        
        # Convert to pandas for easy viewing
        pd_df = result.to_pandas()
        print("\nAs Pandas DataFrame:")
        print(pd_df.head())
        
        print("✓ Simple filtering test passed")
    except Exception as e:
        print(f"✗ Simple filtering test failed: {e}")
    
    # Test 3: Joining GleambookUsers and GleambookMessages
    print("\nTest 3: Joining GleambookUsers and GleambookMessages")
    try:
        users_df = AsterixDataFrame(conn, "TinySocial.GleambookUsers")
        messages_df = AsterixDataFrame(conn, "TinySocial.GleambookMessages")
        
        # Join users with messages
        joined_df = users_df.join(
            messages_df, 
            left_on="id", 
            right_on="authorId",
            alias_left="u",
            alias_right="m"
        )
        
        # Select relevant columns
        result = joined_df.select([
            "u.name AS user_name", 
            "m.message", 
            "m.messageId"
        ]).execute()
        
        print(f"Query executed: {joined_df._query}")
        print(f"Result count: {len(result.result_set)}")
        print("First row:", result.result_set[0] if result.result_set else "No results")
        
        # Convert to pandas for easy viewing
        pd_df = result.to_pandas()
        print("\nAs Pandas DataFrame:")
        print(pd_df.head())
        
        print("✓ Join test passed")
    except Exception as e:
        print(f"✗ Join test failed: {e}")
    
    # Test 4: Complex filtering with multiple predicates
    print("\nTest 4: Complex Filtering with Multiple Predicates")
    try:
        users_df = AsterixDataFrame(conn, "TinySocial.GleambookUsers")
        user_since_attr = AsterixAttribute("userSince", users_df)
        id_attr = AsterixAttribute("id", users_df)
        
        # Users who joined after 2010-01-01 with id < 5
        # Use the datetime function directly - don't wrap it in quotes
        predicate = (user_since_attr > datetime(2010, 1, 1)) & (id_attr < 5)
        result = users_df.select(["id", "name", "userSince"]).filter(predicate).execute()
        
        print(f"Query executed: {users_df._query}")
        print(f"Result count: {len(result.result_set)}")
        print("First row:", result.result_set[0] if result.result_set else "No results")
        
        # Convert to pandas for easy viewing
        pd_df = result.to_pandas()
        print("\nAs Pandas DataFrame:")
        print(pd_df.head())
        
        print("✓ Complex filtering test passed")
    except Exception as e:
        print(f"✗ Complex filtering test failed: {e}")
    
    # Test 5: Limit and Order By
    print("\nTest 5: Limit and Order By")
    try:
        messages_df = AsterixDataFrame(conn, "TinySocial.GleambookMessages")
        
        # Get top 5 messages by ID in descending order
        result = messages_df.select(["messageId", "authorId", "message"]) \
                           .order_by("messageId", desc=True) \
                           .limit(5) \
                           .execute()
        
        print(f"Query executed: {messages_df._query}")
        print(f"Result count: {len(result.result_set)}")
        print("First row:", result.result_set[0] if result.result_set else "No results")
        
        # Convert to pandas for easy viewing
        pd_df = result.to_pandas()
        print("\nAs Pandas DataFrame:")
        print(pd_df.head())
        
        print("✓ Limit and order by test passed")
    except Exception as e:
        print(f"✗ Limit and order by test failed: {e}")
    
    # Test 6: Working with nested data (ChirpMessages)
    print("\nTest 6: Working with Nested Data in ChirpMessages")
    try:
        chirp_df = AsterixDataFrame(conn, "TinySocial.ChirpMessages")
        
        # Custom query to access nested fields - note this is using direct SQL++ execution
        # since our current framework doesn't fully handle nested field access
        cursor.execute("""
        USE TinySocial;
        SELECT VALUE {
            "chirpId": c.chirpId,
            "userName": c.user.name,
            "screenName": c.user.screenName,
            "messageText": c.messageText,
            "sendTime": c.sendTime
        }
        FROM ChirpMessages c
        WHERE c.user.friendsCount > 100
        LIMIT 5;
        """)
        
        chirp_results = cursor.fetchall()
        print(f"Result count: {len(chirp_results)}")
        print("First row:", chirp_results[0] if chirp_results else "No results")
        
        print("✓ Nested data query test passed")
        
        # Now let's try to simulate this with our DataFrame API by using a direct query string
        # This demonstrates how we could handle this in future with proper nested field support
        print("\n(Future feature demo) - Accessing nested fields with DataFrame API:")
        
        # This will only work if we enhance our DataFrame API with nested field support
        # For now, this is just a placeholder for how it might work
        cursor.execute("""
        SELECT c.chirpId, c.user.name AS userName, c.messageText 
        FROM TinySocial.ChirpMessages c
        LIMIT 3;
        """)
        nested_results = cursor.fetchall()
        print(f"Result count via direct query: {len(nested_results)}")
        print("Sample results:", nested_results[0] if nested_results else "No results")
        
    except Exception as e:
        print(f"✗ Nested data query test failed: {e}")
    
    # Step 5: Clean up
    print_section("CLEANUP")
    try:
        cursor.execute("DROP DATAVERSE TinySocial IF EXISTS;")
        print("✓ Cleanup successful")
    except Exception as e:
        print(f"✗ Cleanup failed: {e}")
    
    print_section("TEST COMPLETED")

if __name__ == "__main__":
    test_asterix_dataframe()