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
    df_grouped = df_businesses.groupby("city").aggregate({"review_count": "SUM"}).order_by("SUM(review_count)", desc=True)
    
    # Measure query execution time
    result = measure_time(df_grouped.execute)
    print(result)



# Query 3.3: Average review scores for top 10 cities with the highest number of reviews
def query_3_3():
    print("\nQuery 3.3: Average review scores for top 10 cities")
    
    # AsterixDataFrame query
    df_businesses = AsterixDataFrame(client, "YelpDataverse.Businesses")
    df_avg_scores = (df_businesses.groupby("city")
                     .aggregate({"stars": "AVG", "review_count": "SUM"})
                     .order_by("SUM(review_count)", desc=True)
                     .limit(10))
    
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
        .unnest("categories", alias="c", function="split(t.categories, ',')")
        .aggregate({"stars": "AVG"}, group_by="c")
        .select(["c AS category", "AVG(stars) AS avg_review_score"])
        .order_by("avg_review_score", desc=True)  # Correct usage
    )

    
    # Measure query execution time
    result = measure_time(df_categories.execute)
    print(result)


# Query 3.5: Restaurants in Philadelphia with the highest ratings and most customer engagement
def query_3_5():
    print("\nQuery 3.5: Top restaurants in Philadelphia")
    
    # AsterixDataFrame query
    df_businesses = AsterixDataFrame(client, "YelpDataverse.Businesses")
    df_philly_restaurants = (df_businesses[(df_businesses['city'] == "Philadelphia") & (df_businesses['categories'].contains("Restaurants"))]
                             .select(['name', 'stars', 'review_count'])
                             .order_by(["stars", "review_count"], desc=True)
                             .limit(10))
    
    # Measure query execution time
    result = measure_time(df_philly_restaurants.execute)
    print(result)

# Query 3.6: Coffee shops in Santa Barbara with the highest ratings and most customer engagement
def query_3_6():
    print("\nQuery 3.6: Top coffee shops in Santa Barbara")
    
    # AsterixDataFrame query
    df_businesses = AsterixDataFrame(client, "YelpDataverse.Businesses")
    df_santa_coffee = (df_businesses[(df_businesses['city'] == "Santa Barbara") & (df_businesses['categories'].contains("Coffee"))]
                       .select(['name', 'stars', 'review_count'])
                       .order_by(["stars", "review_count"], desc=True)
                       .limit(10))
    
    # Measure query execution time
    result = measure_time(df_santa_coffee.execute)
    print(result)

# Execute all queries
# query_3_1()
query_3_2()
query_3_3()
# query_3_4()
# query_3_5()
# query_3_6()
