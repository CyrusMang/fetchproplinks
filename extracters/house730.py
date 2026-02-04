# import csv
import datetime
import os
import time
import re
import random
from pymongo import MongoClient
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from models.prop import Prop
# from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
ARTIFACTS_FOLDER = os.getenv("ARTIFACTS_FOLDER")

settings = {
    "RENT_URL": "https://www.house730.com/rent/t1/",
    "BUY_URL": "https://www.house730.com/buy/t1/"
}

def extract_details(db, driver2, link):
    link_parts = link.split('/')
    if len(link_parts) < 4:
        return
    link_part = link_parts[3].split('-')
    prop_id = link_part[-1]
    prop_post_type = link_part[0]
    source_id = f"house730-{prop_id}"

    prop = Prop.get_by_id(db, source_id)
    now = datetime.datetime.now().timestamp()
    if prop and 'updated_at' in prop.data:
        # if within 3 day
        if now - prop.data['updated_at'] < 3 * 24 * 60 * 60:
            print(f"Skip existing prop {source_id}")
            return

    driver2.get(link)
    wait = WebDriverWait(driver2, 10)
    try:
        wait.until(EC.presence_of_element_located((By.ID, 'pc-services-detail')))
        random_number = random.randint(2, 10)

        time.sleep(random_number)  # Wait for page load
    except:
        pass

    content_body_div = driver2.find_element(By.ID, 'pc-services-detail')

    meta = {
        "source_channel": "house730",
        "source_id": source_id,
        "source_url": link,
        "type": 'apartment',
        "post_type": prop_post_type,
        "updated_at": datetime.datetime.now().timestamp(),
        "source_html_content": content_body_div.get_attribute('outerHTML'),
        "status": "pending_extraction",
    }

    if prop:
        prop.update(meta)
        print(f"Updated prop {source_id}")
    else:
        prop = Prop.create(db, meta)
        print(f"Created prop {source_id}")

def extract_rent(db, driver1, driver2):
    driver1.get(settings["RENT_URL"])
    
    # Wait for content to load
    wait = WebDriverWait(driver1, 10)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.service-list-contnet')))
    
    # menu = driver.find_element(By.ID, 'mainMenuDiv')
    # button = menu.find_element(By.CSS_SELECTOR, '[data-value="hk"]')
    # button.click()

    # file_path = os.path.join(FOLDER, f"28hse_links.csv")
    
    def fetch_link():
        # with open(file_path, "a") as of:
        #     writer = csv.writer(of)
        content = driver1.find_element(By.CSS_SELECTOR, '.service-list-contnet')
        search_results_links = content.find_elements(By.CSS_SELECTOR, 'a.card-content-title')
        for link_element in search_results_links:
            link = link_element.get_attribute('href')
            # writer.writerow([link])
            try:
                extract_details(db, driver2, link)
            except Exception as e:
                print(f"Error extracting details for {link}: {e}")
    
    def go_next_page(num):
        #try:
        content = driver1.find_element(By.CSS_SELECTOR, '.service-list-contnet')
        # pagination = content.find_element(By.CSS_SELECTOR, '.page-number')
        #wait = WebDriverWait(driver2, 10)
        #page_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//div[contains(@class, "pagination")]/div/div//p[contains(text(), "{}")]'.format(num))))
        page_button = content.find_element(By.XPATH, '//div[contains(@class, "pagination")]//p[contains(text(), "{}")]'.format(num))
        # if page_button.is_displayed():
        print(page_button.text)
        if page_button:
            # driver2.execute_script('arguments[0].click()', page_button)
            page_button.click()
            time.sleep(7)  # Wait for page load
            return True
        return False
        #except:
        #    return False
        #return False
    
    init_page = 1
    while True:
        fetch_link()
        time.sleep(3)
        init_page += 1
        has_next = go_next_page(init_page)
        if not has_next:
            break


def extract():
    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client['prop_main']

    options = uc.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = uc.Chrome(options=options, use_subprocess=True, version_main=143)

    options2 = uc.ChromeOptions()
    options2.add_argument('--no-sandbox')
    options2.add_argument('--disable-dev-shm-usage')
    driver2 = uc.Chrome(options=options2, use_subprocess=True, version_main=143)

    extract_rent(db, driver, driver2)

    driver.quit()
    driver2.quit()