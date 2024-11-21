from src.pyasterix.asterix_client import AsterixClient
from datetime import datetime, timezone
from pprint import pprint

client = AsterixClient(host="localhost", port=19002)


print("\na) Finding all users:")
users = client.find("Users")
pprint(users)

print("\nb) Finding user by ID:")
user = client.find_one(
    "Users",
    condition={"id": 1}
)
pprint(user)

print("\nc) Finding users with projection:")
users = client.find(
    "Users",
    projection=["name", "email"]
)
pprint(users)






