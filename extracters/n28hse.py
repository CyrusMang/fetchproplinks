import csv
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
    "RENT_URL": "https://www.28hse.com/rent"
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
    cid = f"28hse-{prop_id.split('-')[-1]}"

    prop = Prop.get_by_id(db, cid)
    now = datetime.datetime.now().timestamp()
    if prop and 'updated_at' in prop.data:
        # if within 3 day
        if now - prop.data['updated_at'] < 3 * 24 * 60 * 60:
            print(f"Skip existing prop {cid}")
            return

    driver.get(link)
    wait = WebDriverWait(driver, 10)
    phone_element = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '[attr="phone"]')))
    phone_element.click()

    random_number = random.randint(2, 10)

    time.sleep(random_number)  # Wait for page load

    # page_source = driver.page_source
    # soup = BeautifulSoup(page_source, 'html.parser')
    # text_content = soup.get_text(separator=' ', strip=True)

    breadcrumb = driver.find_element(By.CSS_SELECTOR, 'ol.breadcrumb')
    breadcrumb_items = breadcrumb.find_elements(By.CSS_SELECTOR, 'a span[itemprop="name"]')
    location_parts = [item.text for item in breadcrumb_items[2:]]

    search_results_div = driver.find_element(By.CSS_SELECTOR, '.content_body .ten')

    title = search_results_div.find_element(By.CSS_SELECTOR, '.header').text
    description = search_results_div.find_element(By.ID, 'desc_normal').text
    labels = search_results_div.find_elements(By.CSS_SELECTOR, '.labels .label')
    label_texts = [label.text for label in labels]

    contacts = search_results_div.find_elements(By.CSS_SELECTOR, '.contactsDiv')
    contacts_data = []
    for contact in contacts:
        name = contact.find_element(By.CSS_SELECTOR, '.header').text
        phones = contact.find_elements(By.CSS_SELECTOR, '[attr="phone"]')
        wtsapps = contact.find_elements(By.CSS_SELECTOR, '[attr="whatsapp"]')
        contacts_data.append({
            "name": name,
            "phones": [phone.get_attribute('href') for phone in phones],
            "wtsapps": [wtsapp.get_attribute('href') for wtsapp in wtsapps],
        })

    property_dates_div = search_results_div.find_element(By.CSS_SELECTOR, '.propertyDate')
    property_dates = remove_html_tags(property_dates_div.text).split('|')
    posted_date = property_dates[0].replace('刊登:', '').strip()
    updated_date = property_dates[1].replace('更新:', '').strip()
    
    pair_values = search_results_div.find_elements(By.CSS_SELECTOR, 'table.tablePair tr')
    info = {}
    for pair in pair_values:
        names = pair.find_elements(By.CSS_SELECTOR, 'td.table_left')
        if names:
            values = pair.find_elements(By.CSS_SELECTOR, '.pairValue')
            if values:
                info[remove_html_tags(names[0].text)] = remove_html_tags(values[0].text)

    meta = {
        "cid": cid,
        "source_url": link,
        "type": prop_type,
        "post_type": prop_post_type,
        "location_parts": location_parts,
        "title": title,
        "description": description,
        "labels": label_texts,
        "contacts": contacts_data,
        "source_posted_date": posted_date,
        "source_updated_date": updated_date,
        "info": info,
        "updated_at": datetime.datetime.now().timestamp(),
    }

    if prop:
        prop.update(meta)
        print(f"Updated prop {cid}")
    else:
        Prop.create(db, meta)
        print(f"Created prop {cid}")


def extract_rent(db, driver1, driver2):
    driver1.get(settings["RENT_URL"])
    
    # Wait for content to load
    wait = WebDriverWait(driver1, 10)
    wait.until(EC.presence_of_element_located((By.ID, 'main_content')))
    
    # menu = driver.find_element(By.ID, 'mainMenuDiv')
    # button = menu.find_element(By.CSS_SELECTOR, '[data-value="hk"]')
    # button.click()

    # file_path = os.path.join(FOLDER, f"28hse_links.csv")
    
    def fetch_link():
        # with open(file_path, "a") as of:
        #     writer = csv.writer(of)
        content = driver1.find_element(By.ID, 'main_content')
        search_results_divs = content.find_elements(By.CSS_SELECTOR, '.property_item')
        for div in search_results_divs:
            detail_page_link = div.find_element(By.CSS_SELECTOR, 'a.detail_page')
            link = detail_page_link.get_attribute('href')
            # writer.writerow([link])
            extract_details(db, driver2, link)
    
    def go_next_page(num):
        try:
            content = driver1.find_element(By.ID, 'main_content')
            pagination = content.find_element(By.CSS_SELECTOR, '.pagination')
            page_button = pagination.find_element(By.CSS_SELECTOR, '[attr1="{}"]'.format(num))
            if page_button.is_displayed():
                page_button.click()
                time.sleep(2)  # Wait for page load
                return True
        except:
            return False
        return False
    
    init_page = 1
    while True:
        fetch_link()
        time.sleep(3)
        init_page += 1
        has_next = go_next_page(init_page)
        if not has_next:
            break

def extract_sell(db, driver1, driver2):
    driver1.get(settings["RENT_URL"])
    
    # Wait for content to load
    wait = WebDriverWait(driver1, 10)
    wait.until(EC.presence_of_element_located((By.ID, 'main_content')))
    
    # menu = driver.find_element(By.ID, 'mainMenuDiv')
    # button = menu.find_element(By.CSS_SELECTOR, '[data-value="hk"]')
    # button.click()

    file_path = os.path.join(FOLDER, f"28hse_links.csv")
    
    def fetch_link():
        with open(file_path, "a") as of:
            writer = csv.writer(of)
            content = driver1.find_element(By.ID, 'main_content')
            search_results_divs = content.find_elements(By.CSS_SELECTOR, '.property_item')
            for div in search_results_divs:
                detail_page_link = div.find_element(By.CSS_SELECTOR, 'a.detail_page')
                link = detail_page_link.get_attribute('href')
                writer.writerow([link])
                extract_details(db, driver2, link)
    
    def go_next_page(num):
        try:
            content = driver1.find_element(By.ID, 'main_content')
            pagination = content.find_element(By.CSS_SELECTOR, '.pagination')
            page_button = pagination.find_element(By.CSS_SELECTOR, '[attr1="{}"]'.format(num))
            if page_button.is_displayed():
                page_button.click()
                time.sleep(2)  # Wait for page load
                return True
        except:
            return False
        return False
    
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
    extract_sell(db, driver, driver2)

    driver.quit()
    driver2.quit()