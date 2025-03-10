from src.pyasterix.connection import Connection
import pandas as pd
import matplotlib.pyplot as plt


def analyze_users_data():
    try:
        # Initialize connection
        with Connection(base_url="http://localhost:19002") as conn:
            print("\nConnection initialized.")

            # Create a cursor
            cursor = conn.cursor()
            print("Cursor created.")

            # Setup: Creating dataverse and dataset
            print("\nSetup: Creating dataverse and dataset for analysis.")
            cursor.execute("""
                DROP DATAVERSE AnalyticsDB IF EXISTS;
                CREATE DATAVERSE AnalyticsDB;
                USE AnalyticsDB;

                CREATE TYPE UserType AS {
                    id: int,
                    name: string,
                    age: int,
                    city: string,
                    purchases: [int]
                };

                CREATE DATASET Users(UserType)
                    PRIMARY KEY id;
            """)
            print("Dataverse and dataset created.")

            # Insert sample data
            print("\nInserting sample data.")
            cursor.execute("""
                USE AnalyticsDB;

                INSERT INTO Users([
                    { "id": 1, "name": "Alice", "age": 30, "city": "New York", "purchases": [100, 200, 150] },
                    { "id": 2, "name": "Bob", "age": 25, "city": "Los Angeles", "purchases": [300, 400] },
                    { "id": 3, "name": "Charlie", "age": 35, "city": "Chicago", "purchases": [50, 75] },
                    { "id": 4, "name": "Diana", "age": 28, "city": "New York", "purchases": [200, 300, 100, 50] }
                ]);
            """)
            print("Sample data inserted.")

            # Fetch user data
            print("\nFetching user data for analysis.")
            cursor.execute("""
                USE AnalyticsDB;
                SELECT VALUE u FROM Users u;
            """)
            users_data = cursor.fetchall()

            # Convert to Pandas DataFrame
            print("\nConverting fetched data into Pandas DataFrame.")
            df = pd.DataFrame(users_data)
            print("\nDataFrame:\n", df)

            # Analysis 1: Average age
            avg_age = df['age'].mean()
            print("\nAverage Age of Users:", avg_age)

            # Analysis 2: Total purchases by user
            df['total_purchases'] = df['purchases'].apply(sum)
            print("\nTotal Purchases by User:\n", df[['name', 'total_purchases']])

            # Analysis 3: Top cities by user count
            city_counts = df['city'].value_counts()
            print("\nUser Count by City:\n", city_counts)

            # Visualization: Purchases by User
            plt.figure(figsize=(10, 6))
            plt.bar(df['name'], df['total_purchases'], color='skyblue')
            plt.title("Total Purchases by User")
            plt.xlabel("User")
            plt.ylabel("Total Purchases")
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.show()

            # Visualization: Users by City
            city_counts.plot(kind='bar', color='orange', title="Users by City")
            plt.xlabel("City")
            plt.ylabel("Number of Users")
            plt.tight_layout()
            plt.show()

            # Cleanup
            print("\nCleanup: Dropping dataverse.")
            cursor.execute("DROP DATAVERSE AnalyticsDB IF EXISTS;")
            print("Dataverse dropped and cleanup completed.")

    except Exception as e:
        print(f"Error occurred during analytics execution: {e}")


if __name__ == "__main__":
    analyze_users_data()
