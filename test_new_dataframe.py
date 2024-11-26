from src.pyasterix._http_client import AsterixDBHttpClient
from src.pyasterix.dataframe import AsterixDataFrame as ad

# Initialize the client and DataFrame
client = AsterixDBHttpClient()
df = ad(client, "test.Customers")

# Test 1: Filter using where()
df_stl = df.where(df['address.city'] == "St. Louis, MO")
print("\nAfter filtering by city using where():")
print(df_stl.head())

# Test 2: Select specific columns
df_stl = df_stl[['name', 'rating']]
print("\nAfter selecting columns using select:")
print(df_stl.head())

# Test 3: Filter using mask()
df_masked = df.mask(df['rating'] > 600)
print("\nAfter masking rows where rating > 600:")
print(df_masked.head())

# Test 4: Use isin() to filter rows
df_isin = df.isin('address.city', ["St. Louis, MO", "Boston, MA"])
print("\nAfter filtering using isin():")
print(df_isin.head())

# Test 5: Use between() to filter rows
df_between = df.between('rating', 600, 750)
print("\nAfter filtering ratings between 600 and 750 using between():")
print(df_between.head())

# Test 6: Use filter_items() as an alternative to select
df_filtered_items = df.filter_items(['name', 'rating'])
print("\nAfter filtering columns using filter_items():")
print(df_filtered_items.head())

# Test 7: Use column_slice() to select columns
df_column_slice = df.column_slice('name', 'rating')
print("\nAfter slicing columns between 'name' and 'rating':")
print(df_column_slice.head())

# Test 8: Limit the number of results
df_limited = df.limit(2)
print("\nAfter applying limit(2):")
print(df_limited.head())

# Test 9: Apply offset
df_offset = df.offset(1)
print("\nAfter applying offset(1):")
print(df_offset.head())

# Test 10: Execute a final query
result_stl = df_stl.execute()
print("\nFinal result after execution:")
print(result_stl)

# Uncomment if to_pandas() functionality is needed
# print("Final result as Pandas DataFrame:")
# print(result_stl.to_pandas())
