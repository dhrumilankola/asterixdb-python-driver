from src.pyasterix._http_client import AsterixDBHttpClient
from src.pyasterix.dataframe import AsterixDataFrame
import time

# Initialize the client
client = AsterixDBHttpClient()

def measure_time(func):
    """Utility to measure the execution time of a function."""
    start_time = time.time()
    result = func()
    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.2f} seconds")
    return result

# Query 3.1: Find reviews with over 100 useful votes
def query_3_1():
    print("\nQuery 3.1: Reviews with over 100 useful votes")
    
    # AsterixDataFrame query
    df_reviews = AsterixDataFrame(client, "YelpDataverse.Reviews")
    df_filtered = df_reviews[df_reviews['useful'] > 100].select(['review_id', 'user_id', 'useful'])
    
    # Measure query execution time
    result = measure_time(df_filtered.execute)
    print(result)
    
    # Create index to optimize
    print("\nCreating index for optimization...")
    client.execute_query("CREATE INDEX useful_index ON Reviews(useful);")
    
    # Measure query execution time after index
    result_with_index = measure_time(df_filtered.execute)
    print(result_with_index)

# Query 3.2: Cities ranked by total number of reviews
def query_3_2():
    print("\nQuery 3.2: Cities ranked by total number of reviews")

    # AsterixDataFrame query
    df_businesses = AsterixDataFrame(client, "YelpDataverse.Businesses")
    df_grouped = (
        df_businesses
        .groupby("city")  # Group by city
        .aggregate({"review_count": "SUM"})  # Aggregate review_count
        .select(["city AS city", "SUM(review_count) AS total_review_count"])  # Add proper aliases
        .order_by("total_review_count", desc=True)  # Use the alias for ordering
    )

    # Measure query execution time
    result = measure_time(df_grouped.execute)
    print(result)


# Query 3.3: Average review scores for top 10 cities
def query_3_3():
    print("\nQuery 3.3: Average review scores for top 10 cities")

    # AsterixDataFrame query
    df_businesses = AsterixDataFrame(client, "YelpDataverse.Businesses")
    df_avg_scores = (
        df_businesses
        .groupby("city")  # Group by city
        .aggregate({"stars": "AVG", "review_count": "SUM"})  # Aggregate stars and review_count
        .select([
            "city AS city",  # Alias for city
            "AVG(stars) AS avg_review_score",  # Alias for AVG(stars)
            "SUM(review_count) AS total_review_count"  # Alias for SUM(review_count)
        ])
        .order_by("total_review_count", desc=True)  # Use alias for ordering
        .limit(10)  # Limit to top 10
    )

    # Measure query execution time
    result = measure_time(df_avg_scores.execute)
    print(result)


# Query 3.4: Average review scores for different business categories
def query_3_4():
    print("\nQuery 3.4: Average review scores for different business categories")
    
    # AsterixDataFrame query
    df_businesses = AsterixDataFrame(client, "YelpDataverse.Businesses")
    df_categories = (
        df_businesses
        .unnest("categories", "c", "split(t.categories, ',')")
        .select(["c AS category", "AVG(t.stars) AS avg_review_score"])
        .groupby("c")  # Use the UNNEST alias 'c' instead of the SELECT alias 'category'
        .order_by("avg_review_score", desc=True)
    )

    # Measure query execution time
    result = measure_time(df_categories.execute)
    print(result)


# Query 3.5: Restaurants in Philadelphia with the highest ratings and most customer engagement
def query_3_5():
    print("\nQuery 3.5: Top restaurants in Philadelphia")

    # AsterixDataFrame query
    df_businesses = AsterixDataFrame(client, "YelpDataverse.Businesses")
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
    result = measure_time(df_philly.execute)
    print(result)


# Query 3.6: Coffee shops in Santa Barbara with the highest ratings and most customer engagement
def query_3_6():
    print("\nQuery 3.6: Top coffee shops in Santa Barbara")
    
    # AsterixDataFrame query
    df_businesses = AsterixDataFrame(client, "YelpDataverse.Businesses")
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
    result = measure_time(df_santa_coffee.execute)
    print(result)

# Query 3.7: Most Common Types of Businesses in New Orleans
def query_3_7():
    print("\nQuery 3.7: Most common types of businesses in New Orleans")

    # AsterixDataFrame query
    df_businesses = AsterixDataFrame(client, "YelpDataverse.Businesses")
    df_business_types = (
        df_businesses
        .filter(df_businesses["city"] == "New Orleans")
        .unnest("categories", "c", "split(t.categories, ',')")
        .select(["c AS category", "COUNT(c) AS category_count"])
        .groupby("c")  # Use the alias from UNNEST
        .order_by("category_count", desc=True)
        .limit(10)
    )
    
    # Measure query execution time
    result = measure_time(df_business_types.execute)
    print(result)

# Query 3.8: Monthly Trends in Customer Reviews for Restaurants in Philadelphia
def query_3_8():
    print("\nQuery 3.8: Monthly trends in customer reviews for restaurants in Philadelphia")

    # AsterixDataFrame query
    df_reviews = AsterixDataFrame(client, "YelpDataverse.Reviews")
    df_businesses = AsterixDataFrame(client, "YelpDataverse.Businesses")
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
        .groupby("month")
        .order_by("month", desc=True)
    )
    
    # Measure query execution time
    result = measure_time(df_trends.execute)
    print(result)



# Query 3.9: Most Influential Users in Tampa
def query_3_9():
    print("\nQuery 3.9: Most influential users in Tampa")

    # AsterixDataFrame query
    df_reviews = AsterixDataFrame(client, "YelpDataverse.Reviews")
    df_influential_users = (
        df_reviews
        .join(
            right_table="YelpDataverse.Businesses",
            on="business_id",
            alias_left="r",
            alias_right="b"
        )
        .filter(
            df_reviews["b.city"] == "Tampa"
        )
        .select([
            "r.user_id AS user_id",
            "SUM(r.useful) AS useful_votes"
        ])
        .groupby("r.user_id")
        .order_by("useful_votes", desc=True)
        .limit(10)
    )
    
    # Measure query execution time
    result = measure_time(df_influential_users.execute)
    print(result)


# Query 3.10: Average Length of Customer Reviews Based on Star Ratings
def query_3_10():
    print("\nQuery 3.10: Average length of customer reviews based on star ratings")

    # AsterixDataFrame query
    df_reviews = AsterixDataFrame(client, "YelpDataverse.Reviews")
    df_review_length = (
        df_reviews
        .join(
            right_table="YelpDataverse.Businesses",
            on="business_id",
            alias_left="r",
            alias_right="b"
        )
        .select([
            "r.stars AS stars",
            "AVG(LENGTH(r.text)) AS avg_review_length"
        ])
        .groupby("r.stars")
        .order_by("r.stars", desc=True)
    )
    
    # Measure query execution time
    result = measure_time(df_review_length.execute)
    print(result)




# Execute all queries
# query_3_1()
query_3_2()
query_3_3()
query_3_4()
query_3_5()
query_3_6()
query_3_7()
query_3_8()
query_3_9()
query_3_10()


