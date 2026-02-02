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

DIR = os.path.dirname(os.path.abspath(__file__))
FOLDER = os.path.join(DIR, '..', ARTIFACTS_FOLDER)

settings = {
    "RENT_URL": "https://www.midland.com.hk/zh-hk/list/rent"
}

def remove_html_tags(text):
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)

def extract_details(db, driver, link):
    link_parts = link.split('/')
    if len(link_parts) < 6:
        return
    
    prop_id = link_parts[-1]
    prop_type = link_parts[-2]
    prop_post_type = link_parts[-3]
    source_id = f"midland-{prop_id.split('-')[-1]}"

    prop = Prop.get_by_id(db, source_id)
    now = datetime.datetime.now().timestamp()
    if prop and 'updated_at' in prop.data:
        # if within 3 day
        if now - prop.data['updated_at'] < 3 * 24 * 60 * 60:
            print(f"Skip existing prop {source_id}")
            return

    driver.get(link)
    wait = WebDriverWait(driver, 10)
    photo_element = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '[mediatype="photo"]')))
    photo_element.click()

    time.sleep(2)  # Wait for page load

    # page_source = driver.page_source
    # soup = BeautifulSoup(page_source, 'html.parser')
    # text_content = soup.get_text(separator=' ', strip=True)

    image_div = driver.find_element(By.CSS_SELECTOR, "div[class^='SwiperContainer__']")
    image_links = []
    images = image_div.find_elements(By.CSS_SELECTOR, '.swiper-slide a img')
    for img in images:
        img_src = img.get_attribute('src')
        if img_src and img_src not in image_links:
            image_links.append(img_src)
    
    thumb_links = []
    thumbs_div = driver.find_element(By.CSS_SELECTOR, "div[class^='SwiperThumbnails__']")
    thumbs = thumbs_div.find_elements(By.CSS_SELECTOR, '.swiper-slide div div')
    for thumb in thumbs:
        thumb_src = thumb.get_attribute('src')
        if thumb_src and thumb_src not in thumb_links:
            thumb_links.append(thumb_src)

    content_body_div = driver.find_element(By.CSS_SELECTOR, 'main')
    
    meta = {
        "source_channel": "midland",
        "source_id": source_id,
        "source_url": link,
        "type": prop_type,
        "post_type": prop_post_type,
        "image_links": image_links,
        "thumb_links": thumb_links,
        "updated_at": datetime.datetime.now().timestamp(),
        "source_html_content": content_body_div.get_attribute('outerHTML'),
        "status": "pending_extraction",
    }

    if prop:
        prop.update(meta)
        print(f"Updated prop {source_id}")
    else:
        Prop.create(db, meta)
        print(f"Created prop {source_id}")
    
    random_number = random.randint(2, 10)

    time.sleep(random_number)  # Wait for page load


def extract_rent(db, driver1, driver2):
    driver1.get(settings["RENT_URL"])
    
    # Wait for content to load
    wait = WebDriverWait(driver1, 10)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.rmc-tabs-content-wrap')))
    
    def fetch_link():
        # with open(file_path, "a") as of:
        #     writer = csv.writer(of)
        content = driver1.find_element(By.CSS_SELECTOR, '.rmc-tabs-content-wrap')
        search_results_divs = content.find_element(By.CSS_SELECTOR, 'a[data-gtm-name="ListingPage--Select--Rent"]')
        for div in search_results_divs:
            link = div.get_attribute('href')
            # writer.writerow([link])
            try:
                extract_details(db, driver2, link)
            except Exception as e:
                print(f"Error extracting details for {link}: {e}")
    
    def go_next_page(num):
        try:
            content = driver1.find_element(By.CSS_SELECTOR, '.rmc-tabs-content-wrap')
            pagination = content.find_element(By.CSS_SELECTOR, '[role="navigation"]')
            page_button = pagination.find_element(By.CSS_SELECTOR, '[rel="next"]')
            if page_button.is_displayed():
                page_button.click()
                time.sleep(2)  # Wait for page load
                return True
        except:
            return False
        return False
    
    while True:
        fetch_link()
        time.sleep(3)
        has_next = go_next_page()
        if not has_next:
            break

def extract_sell(db, driver1, driver2):
    driver1.get(settings["RENT_URL"])
    
    # Wait for content to load
    wait = WebDriverWait(driver1, 10)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.rmc-tabs-content-wrap')))
    
    def fetch_link():
        # with open(file_path, "a") as of:
        #     writer = csv.writer(of)
        content = driver1.find_element(By.CSS_SELECTOR, '.rmc-tabs-content-wrap')
        search_results_divs = content.find_element(By.CSS_SELECTOR, 'a[data-gtm-name="ListingPage--Select--Rent"]')
        for div in search_results_divs:
            link = div.get_attribute('href')
            # writer.writerow([link])
            try:
                extract_details(db, driver2, link)
            except Exception as e:
                print(f"Error extracting details for {link}: {e}")
    
    def go_next_page(num):
        try:
            content = driver1.find_element(By.CSS_SELECTOR, '.rmc-tabs-content-wrap')
            pagination = content.find_element(By.CSS_SELECTOR, '[role="navigation"]')
            page_button = pagination.find_element(By.CSS_SELECTOR, '[rel="next"]')
            if page_button.is_displayed():
                page_button.click()
                time.sleep(2)  # Wait for page load
                return True
        except:
            return False
        return False
    
    while True:
        fetch_link()
        time.sleep(3)
        has_next = go_next_page()
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
    extract_sell(db, driver, driver2)

    driver.quit()
    driver2.quit()