import requests
from datetime import datetime
from models.prop import Prop

def review(db, driver, prop):
    response = requests.get(prop['source_url'], allow_redirects=False)
    if response.status_code != 200:
        Prop(db, prop).archive()
        print(f"Archived place {prop['source_id']} due to inaccessible URL.")
    else:
        print(f"Place {prop['source_id']} is still accessible.")