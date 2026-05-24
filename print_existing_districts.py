import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")

mongo_client = MongoClient(MONGODB_CONNECTION_STRING)
db = mongo_client['prop_main']

props = db['props'].find({}, {'address.en.district': 1})

districts = set()
for prop in props:
    district = prop.get('address', {}).get('en', {}).get('district')
    if district:
        districts.add(district)

for district in sorted(districts):
    print(district)
