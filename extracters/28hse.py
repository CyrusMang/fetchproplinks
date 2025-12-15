import csv
import os
import time
from pymongo import MongoClient
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from models.prop import Prop
from dotenv import load_dotenv

load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
ARTIFACTS_FOLDER = os.getenv("ARTIFACTS_FOLDER")

DIR = os.path.dirname(os.path.abspath(__file__))
FOLDER = os.path.join(DIR, ARTIFACTS_FOLDER)

settings = {
    "RENT_URL": "https://www.28hse.com/rent"
}

def save_prop(db, data):
    pass

def extract_details(db, driver, link):
    driver.get(link)
    wait = WebDriverWait(driver, 10)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.property_title')))
    pass

def extract_rent(db, driver1, driver2):
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

    driver = uc.Chrome(headless=False)
    driver2 = uc.Chrome(headless=False)

    extract_rent(db, driver, driver2)
    extract_sell(db, driver, driver2)

    driver.quit()
    driver2.quit()