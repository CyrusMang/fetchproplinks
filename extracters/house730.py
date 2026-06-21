# import csv
import datetime
import os
import uuid
import time
import re
import random
from pymongo import MongoClient
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchWindowException
from models.prop import Prop
# from bs4 import BeautifulSoup
from dotenv import load_dotenv
from utils.uc_driver import create_uc_driver

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

    # get the image links from meta tag
    image_links = []
    meta_tags = driver2.find_elements(By.CSS_SELECTOR, 'meta[property="og:image"]')
    for meta_tag in meta_tags:
        image_url = meta_tag.get_attribute('content')
        if image_url:
            image_links.append(image_url)

    content_body_div = driver2.find_element(By.ID, 'pc-services-detail')

    meta = {
        "source_channel": "house730",
        "source_id": source_id,
        "source_url": link,
        "type": 'apartment',
        "post_type": prop_post_type,
        "updated_at": datetime.datetime.now().timestamp(),
        "image_links": image_links,
        "source_html_content": content_body_div.get_attribute('outerHTML'),
    }

    if prop:
        prop.update(meta)
        print(f"Updated prop {source_id}")
    else:
        meta['created_at'] = datetime.datetime.now().timestamp()
        meta['status'] = "pending_extraction"
        prop = Prop.create(db, {**meta, "id": str(uuid.uuid4())})
        print(f"Created prop {source_id}")

def _create_driver():
    options = uc.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    return create_uc_driver(options=options, use_subprocess=True, version_main=148)


def _is_window_closed_error(error):
    message = str(error).lower()
    return (
        isinstance(error, NoSuchWindowException)
        or "no such window" in message
        or "target window already closed" in message
        or "web view not found" in message
    )


def _ensure_driver(driver):
    try:
        _ = driver.current_url
        _ = driver.window_handles
        return driver
    except Exception as e:
        if _is_window_closed_error(e):
            print("Driver window closed unexpectedly, recreating driver")
            try:
                driver.quit()
            except Exception:
                pass
            return _create_driver()
        raise


def _open_listing_page(driver, url):
    driver = _ensure_driver(driver)
    try:
        driver.get(url)
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.service-list-contnet')))
        return driver
    except Exception as e:
        if _is_window_closed_error(e):
            print("List page driver closed while loading page, recreating and retrying")
            try:
                driver.quit()
            except Exception:
                pass
            driver = _create_driver()
            driver.get(url)
            wait = WebDriverWait(driver, 10)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.service-list-contnet')))
            return driver
        raise

def extract_rent(db, driver1, driver2):
    driver1 = _open_listing_page(driver1, settings["RENT_URL"])
    
    # menu = driver.find_element(By.ID, 'mainMenuDiv')
    # button = menu.find_element(By.CSS_SELECTOR, '[data-value="hk"]')
    # button.click()

    # file_path = os.path.join(FOLDER, f"28hse_links.csv")
    
    def fetch_link():
        nonlocal driver2
        # with open(file_path, "a") as of:
        #     writer = csv.writer(of)
        content = driver1.find_element(By.CSS_SELECTOR, '.service-list-contnet')
        search_results_links = content.find_elements(By.CSS_SELECTOR, 'a.card-content-title')
        for link_element in search_results_links:
            # writer.writerow([link])
            try:
                link = link_element.get_attribute('href')
                try:
                    extract_details(db, driver2, link)
                except Exception as e:
                    print(f"Error extracting details for {link_element}: {e}")
                    if _is_window_closed_error(e):
                        print("Recreating detail driver due to closed window")
                        try:
                            driver2.quit()
                        except Exception:
                            pass
                        driver2 = _create_driver()
            except Exception as e:
                pass
    
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
        time.sleep(7)
        init_page += 1
        has_next = go_next_page(init_page)
        if not has_next:
            break

    return driver1, driver2

def extract():
    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client['prop_main']

    options = uc.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = create_uc_driver(options=options, use_subprocess=True, version_main=148)

    options2 = uc.ChromeOptions()
    options2.add_argument('--no-sandbox')
    options2.add_argument('--disable-dev-shm-usage')
    driver2 = create_uc_driver(options=options2, use_subprocess=True, version_main=148)

    driver, driver2 = extract_rent(db, driver, driver2)

    try:
        driver.quit()
    except Exception:
        pass
    try:
        driver2.quit()
    except Exception:
        pass