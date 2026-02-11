from datetime import datetime
import os
import time
from pymongo import MongoClient
from dotenv import load_dotenv
from selenium.webdriver.support.ui import WebDriverWait
import undetected_chromedriver as uc

load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")

batch_size = 3000

def check_batch(collection, driver, filter, skip=0, limit=batch_size):
    properties = collection.find(filter).skip(skip).limit(limit)
    count = 0
    for prop in properties:
        try:
            driver.get(prop['source_url'])

            time.sleep(3) 

            current_url = driver.current_url
            if current_url == prop['source_url'] or current_url == prop['source_url'] + "/":
                print(f"Place {prop['source_id']} is still accessible.")
            else:
                raise Exception("URL redirected, likely inaccessible.")
        except:
            collection.update_one(
                { 'source_id': prop['source_id'] },
                { '$set': { 
                    'status': "archived", 
                    "updated_at": datetime.now().timestamp(), 
                } }
            )
            print(f"Archived place {prop['source_id']} due to inaccessible URL.")
        count += 1
    if count < limit:
        return False
    return True

def main():
    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client['prop_main']
    collection = db['props']

    options = uc.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = uc.Chrome(options=options, use_subprocess=True, version_main=145)

    f = {
        'type': "apartment",
        'status': { "$ne": "archived" },
    }
    skip = 0
    while True:
        if not check_batch(collection, driver, f, skip=skip):
            break
        skip += batch_size
    driver.quit()
    print("Review completed.")
    client.close()

if __name__ == '__main__':
   main()