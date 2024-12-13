import pandas as pd
from src.pyasterix.dataframe.base import AsterixDataFrame
from src.pyasterix._http_client import AsterixDBHttpClient
from src.pyasterix.connection import Connection


def test_query_workflow():
    """Test the complete workflow: query construction, execution, and Pandas conversion."""
    print("Testing the complete query workflow...")
    
    with Connection(base_url="http://localhost:19002") as conn:
    
        # Query 3.2: Cities ranked by total number of reviews
        print("\nQuery 3.2: Cities ranked by total number of reviews")
        
        # Construct the query using AsterixDataFrame
        df_businesses = AsterixDataFrame(conn, "YelpDataverse.Businesses")
        df_grouped = (
            df_businesses
            .groupby("city")
            .aggregate({"review_count": "SUM"})
            .select(["city AS city", "SUM(review_count) AS total_review_count"])
            .order_by("total_review_count", desc=True)
        )

        # Execute the query and fetch the AsterixDataFrame
        result = df_grouped.execute()
        assert isinstance(result, AsterixDataFrame), "Result should be an AsterixDataFrame"

        # Convert the result to a Pandas DataFrame
        pandas_df = result.to_pandas()
        assert isinstance(pandas_df, pd.DataFrame), "Result should be a Pandas DataFrame"
        
        # Print the Pandas DataFrame
        print("Pandas DataFrame (Cities Ranked by Total Reviews):")
        print(pandas_df)

        # Perform some Pandas operations
        total_reviews = pandas_df["total_review_count"].sum()
        assert total_reviews > 0, "Total review count should be greater than 0"
        print(f"Total Reviews Across All Cities: {total_reviews}")

        # Get the top city
        top_city = pandas_df.iloc[0]
        print(f"Top City by Reviews: {top_city['city']} with {top_city['total_review_count']} reviews")

        # Query 3.3: Average review scores for the top 10 cities with the highest number of reviews
        print("\nQuery 3.3: Average review scores for the top 10 cities with the highest number of reviews")

        # Construct the query
        df_top_cities = (
            df_businesses
            .groupby("city")
            .aggregate({"stars": "AVG", "review_count": "SUM"})
            .select([
                "city AS city",
                "AVG(stars) AS avg_review_score",
                "SUM(review_count) AS total_review_count"
            ])
            .order_by("total_review_count", desc=True)
            .limit(10)
        )

        # Execute the query and fetch the AsterixDataFrame
        result = df_top_cities.execute()
        assert isinstance(result, AsterixDataFrame), "Result should be an AsterixDataFrame"

        # Convert the result to a Pandas DataFrame
        pandas_df = result.to_pandas()
        assert isinstance(pandas_df, pd.DataFrame), "Result should be a Pandas DataFrame"

        # Print the Pandas DataFrame
        print("Pandas DataFrame (Average Review Scores for Top 10 Cities):")
        print(pandas_df)

        # Perform some Pandas operations
        avg_review_score = pandas_df["avg_review_score"].mean()
        assert avg_review_score > 0, "Average review score should be greater than 0"
        print(f"Average Review Score Across Top 10 Cities: {avg_review_score}")

        # Analyze the top city
        top_city = pandas_df.iloc[0]
        print(f"Top City by Total Reviews: {top_city['city']} with Avg Review Score: {top_city['avg_review_score']} and Total Reviews: {top_city['total_review_count']}")

if __name__ == "__main__":
    # Run the test
    test_query_workflow()
