import pandas as pd
import time
import os
import sys
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
from src.pyasterix.dataframe import AsterixDataFrame

def setup_observability():
    """Setup observability for Yelp querying."""
    config = ObservabilityConfig(
        metrics=MetricsConfig(
            enabled=True,
            namespace="yelp_queries",
            prometheus_port=8006
        ),
        tracing=TracingConfig(
            enabled=True,
            service_name="yelp_queries_service",
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
    print("✅ Observability initialized for Yelp queries")
    return observability

# Initialize observability and connection
observability = setup_observability()
conn = connect(
    host="localhost",
    port=19002,
    observability_config=observability.config
)

def measure_time(func, query_name="unknown"):
    """Utility to measure the execution time of a function with observability."""
    logger = observability.get_logger("yelp_queries.performance")
    
    with observability.start_span(f"yelp_query.{query_name}", kind="INTERNAL") as span:
        logger.info(f"Starting query: {query_name}", extra={
            "query_name": query_name,
            "operation": "yelp_dataframe_query"
        })
        
        start_time = time.time()
        result = func()
        end_time = time.time()
        
        execution_time = end_time - start_time
        print(f"Execution time: {execution_time:.2f} seconds")
        
        # Record performance metric
        observability.record_query_duration(
            execution_time,
            query_name=query_name,
            operation="dataframe_query"
        )
        
        # Set span attributes
        span.set_attribute("execution_time", execution_time)
        span.set_attribute("query_name", query_name)
        span.set_attribute("result_count", len(result) if hasattr(result, '__len__') else 0)
        
        logger.info(f"Query completed: {query_name}", extra={
            "query_name": query_name,
            "execution_time": execution_time,
            "result_count": len(result) if hasattr(result, '__len__') else 0
        })
        
        return result

# Query 3.1: Find reviews with over 100 useful votes
def query_3_1():
    print("\nQuery 3.1: Reviews with over 100 useful votes")
    
    # AsterixDataFrame query with observability
    df_reviews = AsterixDataFrame(conn, "YelpDataverse.Reviews")
    df_filtered = df_reviews[df_reviews['useful'] > 100].select(['review_id', 'user_id', 'useful'])
    
    # Measure query execution time with observability
    result = measure_time(df_filtered.execute, "reviews_over_100_useful")
    print(result)
    
    # Create index to optimize (commented out since we're using observability-enabled connection)
    print("\nIndex creation would be handled by cursor operations...")
    
    # Measure query execution time after hypothetical index
    result_with_index = measure_time(df_filtered.execute, "reviews_after_index")
    print(result_with_index)

# Query 3.2: Cities ranked by total number of reviews
def query_3_2():
    print("\nQuery 3.2: Cities ranked by total number of reviews")

    # AsterixDataFrame query
    df_businesses = AsterixDataFrame(conn, "YelpDataverse.Businesses")
    df_grouped = (
        df_businesses
        .group_by("city")  # Group by city
        .agg({"review_count": "SUM"})  # Aggregate review_count
        .select(["city AS city", "SUM(review_count) AS total_review_count"])  # Add proper aliases
        .order_by("total_review_count", desc=True)  # Use the alias for ordering
    )

    # Measure query execution time
    result = measure_time(df_grouped.execute, "cities_by_review_count")
    print("AsterixDB result: \n", result)
    
    
# Query 3.3: Average review scores for top 10 cities
def query_3_3():
    print("\nQuery 3.3: Average review scores for top 10 cities")

    # AsterixDataFrame query
    df_businesses = AsterixDataFrame(conn, "YelpDataverse.Businesses")
    df_avg_scores = (
        df_businesses
        .group_by("city")  # Group by city
        .agg({"stars": "AVG", "review_count": "SUM"})  # Aggregate stars and review_count
        .select([
            "city AS city",  # Alias for city
            "AVG(stars) AS avg_review_score",  # Alias for AVG(stars)
            "SUM(review_count) AS total_review_count"  # Alias for SUM(review_count)
        ])
        .order_by("total_review_count", desc=True)  # Use alias for ordering
        .limit(10)  # Limit to top 10
    )

    # Measure query execution time
    result = measure_time(df_avg_scores.execute, "avg_scores_top_cities")
    print(result)


# Query 3.4: Average review scores for different business categories
def query_3_4():
    print("\nQuery 3.4: Average review scores for different business categories")
    
    # AsterixDataFrame query
    df_businesses = AsterixDataFrame(conn, "YelpDataverse.Businesses")
    df_categories = (
        df_businesses
        .unnest("categories", "c", "split(b.categories, ',')")  # Correct alias 'b'
        .select(["c AS category", "AVG(b.stars) AS avg_review_score"])  # Consistent aliasing
        .group_by("c")  # Use the UNNEST alias 'c'
        .order_by("avg_review_score", desc=True)
    )

    # Measure query execution time
    result = measure_time(df_categories.execute, "categories_avg_scores")
    print(result)



# Query 3.5: Restaurants in Philadelphia with the highest ratings and most customer engagement
def query_3_5():
    print("\nQuery 3.5: Top restaurants in Philadelphia")

    # AsterixDataFrame query
    df_businesses = AsterixDataFrame(conn, "YelpDataverse.Businesses")
    df_philly = (
        df_businesses
        .filter(
            (df_businesses["city"] == "Philadelphia") &
            df_businesses["categories"].like("%Restaurants%")
        )
        .select([
            "name AS name",  # Alias for name
            "stars AS stars",  # Alias for stars
            "review_count AS review_count"  # Alias for review_count
        ])
        .order_by(
            {"stars": True, "review_count": True}  # Descending order for both stars and review_count
        )
        .limit(10)  # Limit to top 10
    )

    # Measure query execution time
    result = measure_time(df_philly.execute, "top_philly_restaurants")
    print(result)


# Query 3.6: Coffee shops in Santa Barbara with the highest ratings and most customer engagement
def query_3_6():
    print("\nQuery 3.6: Top coffee shops in Santa Barbara")
    
    # AsterixDataFrame query
    df_businesses = AsterixDataFrame(conn, "YelpDataverse.Businesses")
    df_santa_coffee = (
        df_businesses
        .filter(
            (df_businesses["city"] == "Santa Barbara") &
            df_businesses["categories"].like("%Coffee%")
        )
        .select([
            "name AS name",  # Alias for name
            "stars AS stars",  # Alias for stars
            "review_count AS review_count"  # Alias for review_count
        ])
        .order_by(
            {"stars": True, "review_count": True}  # Descending order for both stars and review_count
        )
        .limit(10)  # Limit to top 10
    )
    
    # Measure query execution time
    result = measure_time(df_santa_coffee.execute, "santa_barbara_coffee")
    print(result)

# Query 3.7: Most Common Types of Businesses in New Orleans
def query_3_7():
    print("\nQuery 3.7: Most common types of businesses in New Orleans")

    # AsterixDataFrame query
    df_businesses = AsterixDataFrame(conn, "YelpDataverse.Businesses")
    df_business_types = (
        df_businesses
        .filter(df_businesses["city"] == "New Orleans")
        .unnest("categories", "c", "split(t.categories, ',')")
        .select(["c AS category", "COUNT(c) AS category_count"])
        .group_by("c")  # Use the alias from UNNEST
        .order_by("category_count", desc=True)
        .limit(10)
    )
    
    # Measure query execution time
    result = measure_time(df_business_types.execute, "new_orleans_business_types")
    print(result)

# Query 3.8: Monthly Trends in Customer Reviews for Restaurants in Philadelphia
def query_3_8():
    print("\nQuery 3.8: Monthly trends in customer reviews for restaurants in Philadelphia")

    # AsterixDataFrame query
    df_reviews = AsterixDataFrame(conn, "YelpDataverse.Reviews")
    df_businesses = AsterixDataFrame(conn, "YelpDataverse.Businesses")
    df_trends = (
        df_reviews
        .join(
            df_businesses,
            on="business_id",
            alias_left="r",
            alias_right="b"
        )
        .filter(
            (df_businesses["city"] == "Philadelphia") &
            df_businesses["categories"].contains("Restaurants")
        )
        .select([
            "get_month(parse_datetime(r.date, 'YYYY-MM-DD hh:mm:ss')) AS month",
            "COUNT(r.review_id) AS review_count"
        ])
        .group_by("month")
        .order_by("month", desc=True)
    )
    
    # Measure query execution time
    result = measure_time(df_trends.execute, "philly_review_trends")
    print(result)



# Query 3.9: Most Influential Users in Tampa
def query_3_9():
    print("\nQuery 3.9: Most influential users in Tampa")

    # AsterixDataFrame query
    df_reviews = AsterixDataFrame(conn, "YelpDataverse.Reviews")
    df_businesses = AsterixDataFrame(conn, "YelpDataverse.Businesses")
    df_influential_users = (
        df_reviews
        .join(
            df_businesses,
            on="business_id",
            alias_left="r",
            alias_right="b"
        )
        .filter(
            (df_businesses["city"] == "Tampa")  # Correct alias usage for filtering
        )
        .select([
            "r.user_id AS user_id",           # Correct alias usage in SELECT
            "SUM(r.useful) AS useful_votes"  # Correct agg alias
        ])
        .group_by("user_id")  # Use alias defined in SELECT for GROUP BY
        .order_by("useful_votes", desc=True)  # Use agg alias for ORDER BY
        .limit(10)
    )
    
    # Measure query execution time
    result = measure_time(df_influential_users.execute, "tampa_influential_users")
    print(result)


# Query 3.10: Average Length of Customer Reviews Based on Star Ratings
def query_3_10():
    print("\nQuery 3.10: Average length of customer reviews based on star ratings")

    # AsterixDataFrame query
    df_reviews = AsterixDataFrame(conn, "YelpDataverse.Reviews")
    df_businesses = AsterixDataFrame(conn, "YelpDataverse.Businesses")
    df_review_length = (
        df_reviews
        .join(
            df_businesses,
            on="business_id",
            alias_left="r",
            alias_right="b"
        )
        .select([
            "r.stars AS stars",                    # Correct alias for stars
            "AVG(LENGTH(r.text)) AS avg_review_length"  # Correct aggregation
        ])
        .group_by("stars")  # Group by the alias defined in SELECT
        .order_by("stars", desc=True)  # Order by alias
    )
    
    # Measure query execution time
    result = measure_time(df_review_length.execute, "review_length_by_stars")
    print(result)


# Query 3.11: Bars in Tucson
def query_3_11():
    """Test: Bars in Tucson with most tips and their star ratings."""

    # Setup: Create AsterixDataFrame objects for Businesses and Tips
    df_businesses = AsterixDataFrame(conn, "YelpDataverse.Businesses")
    df_tips = AsterixDataFrame(conn, "YelpDataverse.Tips")

    # Query construction
    df_bars_tips = (
        df_businesses
        .join(
            df_tips,
            on="business_id",
            alias_left="b",
            alias_right="t"
        )
        .filter(
            (df_businesses["city"] == "Tucson") & 
            df_businesses["categories"].contains("Bars")
        )
        .select([
            "b.name AS name",  # Bar name
            "COUNT(t.tip_id) AS tips_count",  # Number of tips
            "b.stars AS rating"  # Star ratings
        ])
        .group_by(["name", "rating"])  # Group by the column aliases
        .order_by("tips_count", desc=True)  # Order by tips count in descending order
    )

    # Execution and validation
    result = measure_time(df_bars_tips.execute, "tucson_bars_with_tips")
    print(result)


def query_3_12():
    """Test: Most reviewed businesses and top 10 common words in their reviews."""

    # Part 1: Find the top 3 businesses with the most reviews
    df_businesses = AsterixDataFrame(conn, "YelpDataverse.Businesses")
    df_most_reviewed = (
        df_businesses
        .select([
            "business_id AS business_id",  # No alias prefix for columns directly referenced
            "name AS name",
            "review_count AS review_count"
        ])
        .order_by("review_count", desc=True)  # Use alias directly
        .limit(3)
    )
    result_most_reviewed = measure_time(df_most_reviewed.execute, "top_reviewed_businesses")
    print("Top 3 Most Reviewed Businesses:", result_most_reviewed)
    
    # Get the business ID of the most-reviewed business (offset 0 for the first record)
    most_reviewed_business_id = result_most_reviewed.head(1)[0]["business_id"]

    # Part 2: Find the top 10 common words in reviews for the most-reviewed business
    df_reviews = AsterixDataFrame(conn, "YelpDataverse.Reviews")
    df_common_words = (
        df_reviews
        .unnest(
            field="text",
            alias="w",
            function="split(r.text, ' ')"  # Ensure alias consistency
        )
        .filter(df_reviews["business_id"] == most_reviewed_business_id)
        .select([
            "w AS word",  # Word alias
            "COUNT(w) AS word_count"  # Word count
        ])
        .group_by("word")  # Group by alias directly
        .order_by("word_count", desc=True)  # Sort by word count
        .limit(10)
    )
    result_common_words = measure_time(df_common_words.execute, "common_words_analysis")
    print("Top 10 Common Words:", result_common_words)






# Execute all queries
#query_3_1()
#query_3_2()
#query_3_3()
# query_3_4()
query_3_5()
# query_3_6()
# query_3_7()
# query_3_8()
# query_3_9()
# query_3_10()
# query_3_11()
# query_3_12()

