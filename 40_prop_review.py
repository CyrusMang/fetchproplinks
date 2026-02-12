import time
import os
import undetected_chromedriver as uc
from pymongo import MongoClient
from dotenv import load_dotenv
from reviewers import n28hse, house730

load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")

batch_size = 3000

def check_batch(db, driver, filter, skip=0, limit=batch_size):
    properties = db['props'].find(filter).skip(skip).limit(limit)
    count = 0
    sleep_counter = 0
    for prop in properties:
        sleep_counter += 1
        if sleep_counter >= 30:
            time.sleep(5)
            sleep_counter = 0
        if prop['source_channel'] == 'house730':
            house730.review(db, driver, prop)
        elif prop['source_channel'] == '28hse':
            n28hse.review(db, driver, prop)
        count += 1
    if count < limit:
        return False
    return True

def main():
    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client['prop_main']

    options = uc.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = uc.Chrome(options=options, use_subprocess=True, version_main=145)

    f = {
        'type': "apartment",
        'source_channel': { "$in": ["28hse","house730"] },
        'status': { "$ne": "archived" },
    }
    skip = 0
    while True:
        if not check_batch(db, driver, f, skip=skip):
            break
        skip += batch_size
    print("Review completed.")
    driver.quit()
    client.close()

if __name__ == '__main__':
   main()