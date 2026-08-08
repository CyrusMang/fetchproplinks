# import csv
import datetime
import os
import uuid
import time
import re
import random
import json
from urllib.parse import urlparse
from pymongo import MongoClient
import undetected_chromedriver as uc
import httpx
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchWindowException
from models.prop import Prop
# from bs4 import BeautifulSoup
from dotenv import load_dotenv
from utils.uc_driver import create_uc_driver
import trafilatura
from trafilatura.utils import trim

load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
ARTIFACTS_FOLDER = os.getenv("ARTIFACTS_FOLDER")

DIR = os.path.dirname(os.path.abspath(__file__))
FOLDER = os.path.join(DIR, '..', ARTIFACTS_FOLDER)

settings = {
    "RENT_URL": "https://www.28hse.com/rent",
    #"BUY_URL": "https://www.28hse.com/buy"
}

HTTP_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)
HTTP_TIMEOUT_SECONDS = 20
HTTP_RETRY_ATTEMPTS = 3

def _create_driver():
    options = uc.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    return create_uc_driver(options=options, use_subprocess=True)


def _is_window_closed_error(error):
    message = str(error).lower()
    return (
        isinstance(error, NoSuchWindowException)
        or "no such window" in message
        or "target window already closed" in message
        or "web view not found" in message
    )


def _get_source_fields(link):
    parsed = urlparse(link)
    path_parts = [part for part in parsed.path.split('/') if part]
    if len(path_parts) < 3:
        return None

    prop_id = path_parts[-1]
    prop_type = path_parts[-2]
    prop_post_type = path_parts[-3]
    source_id = f"28hse-{prop_id.split('-')[-1]}"

    return {
        "prop_id": prop_id,
        "prop_type": prop_type,
        "prop_post_type": prop_post_type,
        "source_id": source_id,
    }


def _fetch_html_with_retries(client, url, retries=HTTP_RETRY_ATTEMPTS):
    for attempt in range(1, retries + 1):
        try:
            response = client.get(url)
            response.raise_for_status()
            return response.text
        except Exception as e:
            if attempt == retries:
                raise
            delay = min(5, attempt)
            print(f"HTTP fetch failed for {url} (attempt {attempt}/{retries}): {e}")
            time.sleep(delay)


def _extract_listing_links_from_html(html):
    soup = BeautifulSoup(html, 'lxml')

    links = []
    seen = set()

    for script in soup.select('script[type="application/ld+json"]'):
        script_text = script.string or script.get_text() or ""
        script_text = script_text.strip()
        if not script_text:
            continue

        try:
            payload = json.loads(script_text)
        except Exception:
            continue

        payload_items = payload if isinstance(payload, list) else [payload]
        for item in payload_items:
            if not isinstance(item, dict):
                continue

            item_list = item.get('itemListElement')
            if not isinstance(item_list, list):
                continue

            for item_entry in item_list:
                if not isinstance(item_entry, dict):
                    continue
                url = item_entry.get('url')
                if isinstance(url, str) and '/property-' in url and url not in seen:
                    seen.add(url)
                    links.append(url)

    if links:
        return links

    # Fallback to DOM selectors if JSON-LD is unavailable.
    for anchor in soup.select('a.detail_page[href]'):
        href = anchor.get('href')
        if not href:
            continue
        if href.startswith('/'):
            href = f"https://www.28hse.com{href}"
        if '/property-' in href and href not in seen:
            seen.add(href)
            links.append(href)

    return links


def _extract_prop_meta_from_detail_html(link, html):
    source_fields = _get_source_fields(link)
    if not source_fields:
        return None

    soup = BeautifulSoup(html, 'lxml')

    content_body_div = soup.select_one('.content_body .ten')
    if content_body_div is None:
        raise ValueError(f"Failed to locate detail content for {link}")

    breadcrumb_items = soup.select('ol.breadcrumb a span[itemprop="name"]')
    location_parts = [item.get_text(strip=True) for item in breadcrumb_items[2:]]

    image_links = []
    for image in soup.select('.slider-block img'):
        img_src = image.get('src')
        if img_src and img_src not in image_links:
            image_links.append(img_src)

    title_node = content_body_div.select_one('.message .header')
    description_node = content_body_div.select_one('#desc_normal')
    title = title_node.get_text(strip=True) if title_node else ""
    description = description_node.get_text("\n", strip=True) if description_node else ""

    labels = [label.get_text(strip=True) for label in content_body_div.select('.labels .label')]

    contacts_data = []
    for contact in content_body_div.select('.contactsDiv'):
        header = contact.select_one('.header')
        name = header.get_text(strip=True) if header else ""
        license_no = None
        for span in contact.select('.content span.less_span'):
            span_text = span.get_text(strip=True)
            if '牌照號碼' in span_text:
                license_no = span_text.replace('代理個人牌照號碼:', '').strip()
        contacts_data.append({
            "name": name,
            "license_no": license_no,
        })

    posted_date = ""
    updated_date = ""
    property_dates_div = content_body_div.select_one('.propertyDate')
    if property_dates_div:
        property_dates = remove_html_tags(property_dates_div.get_text(" ", strip=True)).split('|')
        if len(property_dates) > 0:
            posted_date = property_dates[0].replace('刊登:', '').strip()
        if len(property_dates) > 1:
            updated_date = property_dates[1].replace('更新:', '').strip()

    info = {}
    for pair in content_body_div.select('table.tablePair tr'):
        name_node = pair.select_one('td.table_left')
        value_node = pair.select_one('.pairValue')
        if name_node and value_node:
            info[remove_html_tags(name_node.get_text(" ", strip=True))] = remove_html_tags(
                value_node.get_text(" ", strip=True)
            )

    return {
        "source_channel": "28hse",
        "source_id": source_fields["source_id"],
        "source_url": link,
        "type": source_fields["prop_type"],
        "post_type": source_fields["prop_post_type"],
        "location_parts": location_parts,
        "title": title,
        "description": description,
        "labels": labels,
        "contacts": contacts_data,
        "source_posted_date": posted_date,
        "source_updated_date": updated_date,
        "info": info,
        "image_links": image_links,
        "updated_at": datetime.datetime.now().timestamp(),
        "source_html_content": str(content_body_div),
    }


def _upsert_prop(db, meta):
    source_id = meta["source_id"]
    prop = Prop.get_by_id(db, source_id)
    if prop:
        prop.update(meta)
        print(f"Updated prop {source_id}")
    else:
        create_meta = {**meta}
        create_meta['status'] = "pending_extraction"
        Prop.create(db, create_meta)
        print(f"Created prop {source_id}")


def extract_details_http(db, client, link):
    source_fields = _get_source_fields(link)
    if not source_fields:
        return

    source_id = source_fields["source_id"]
    prop = Prop.get_by_id(db, source_id)
    now = datetime.datetime.now().timestamp()
    if prop and 'updated_at' in prop.data:
        if now - prop.data['updated_at'] < 3 * 24 * 60 * 60:
            print(f"Skip existing prop {source_id}")
            return

    html = _fetch_html_with_retries(client, link)
    meta = _extract_prop_meta_from_detail_html(link, html)
    if not meta:
        return
    _upsert_prop(db, meta)


def extract_rent_http(db):
    headers = {
        "User-Agent": HTTP_USER_AGENT,
        "Accept-Language": "zh-HK,zh;q=0.9,en;q=0.8",
    }

    processed_count = 0
    with httpx.Client(headers=headers, follow_redirects=True, timeout=HTTP_TIMEOUT_SECONDS) as client:
        page_number = 1
        max_pages = int(os.getenv("N28HSE_HTTP_MAX_PAGES", "1000"))

        while page_number <= max_pages:
            page_url = settings["RENT_URL"] if page_number == 1 else f"{settings['RENT_URL']}/page-{page_number}"
            page_html = _fetch_html_with_retries(client, page_url)
            links = _extract_listing_links_from_html(page_html)

            if not links:
                if page_number == 1:
                    raise RuntimeError("No listing links found on first rent page")
                print(f"No links found on page {page_number}, stopping rent HTTP extraction")
                break

            print(f"Rent page {page_number}: found {len(links)} links")

            for link in links:
                try:
                    extract_details_http(db, client, link)
                    processed_count += 1
                    time.sleep(random.uniform(0.8, 1.8))
                except Exception as e:
                    print(f"Error extracting details for {link}: {e}")

            page_number += 1
            time.sleep(random.uniform(1.2, 2.2))

    print(f"HTTP rent extraction processed {processed_count} listing detail URLs")


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
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            driver = _ensure_driver(driver)
            driver.get(url)
            wait = WebDriverWait(driver, 10)
            wait.until(EC.presence_of_element_located((By.ID, 'main_content')))
            return driver
        except Exception as e:
            if not _is_window_closed_error(e):
                raise

            print(
                f"List page driver closed while loading page "
                f"(attempt {attempt}/{max_attempts}), recreating and retrying"
            )
            try:
                driver.quit()
            except Exception:
                pass

            if attempt == max_attempts:
                raise

            driver = _create_driver()

    return driver

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
    source_id = f"28hse-{prop_id.split('-')[-1]}"

    prop = Prop.get_by_id(db, source_id)
    now = datetime.datetime.now().timestamp()
    if prop and 'updated_at' in prop.data:
        # if within 3 day
        if now - prop.data['updated_at'] < 3 * 24 * 60 * 60:
            print(f"Skip existing prop {source_id}")
            return

    driver.get(link)
    wait = WebDriverWait(driver, 10)

    time.sleep(2)  # Wait for page load
    # try:
    #     phone_element = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '[attr="phone"]')))
    #     phone_element.click()

    #     random_number = random.randint(2, 10)

    #     time.sleep(random_number)  # Wait for page load
    # except:
    #     pass

    # page_source = driver.page_source
    # soup = BeautifulSoup(page_source, 'html.parser')
    # text_content = soup.get_text(separator=' ', strip=True)

    breadcrumb = driver.find_element(By.CSS_SELECTOR, 'ol.breadcrumb')
    breadcrumb_items = breadcrumb.find_elements(By.CSS_SELECTOR, 'a span[itemprop="name"]')
    location_parts = [item.text for item in breadcrumb_items[2:]]

    image_links = []
    image_div = driver.find_element(By.CSS_SELECTOR, '.slider-block')
    if image_div:
        images = image_div.find_elements(By.CSS_SELECTOR, 'img')
        for img in images:
            img_src = img.get_attribute('src')
            if img_src and img_src not in image_links:
                image_links.append(img_src)
    
    # thumb_links = []
    # thumbs_div = driver.find_element(By.ID, 'mySliderPictures_thumbDiv')
    # if thumbs_div:
    #     thumbs = thumbs_div.find_elements(By.CSS_SELECTOR, 'img')
    #     for thumb in thumbs:
    #         thumb_src = thumb.get_attribute('src')
    #         if thumb_src and thumb_src not in thumb_links:
    #             thumb_links.append(thumb_src)

    content_body_div = driver.find_element(By.CSS_SELECTOR, '.content_body .ten')

    title = content_body_div.find_element(By.CSS_SELECTOR, '.message .header').text
    description = content_body_div.find_element(By.ID, 'desc_normal').text
    labels = content_body_div.find_elements(By.CSS_SELECTOR, '.labels .label')
    label_texts = [label.text for label in labels]

    contacts = content_body_div.find_elements(By.CSS_SELECTOR, '.contactsDiv')
    contacts_data = []
    for contact in contacts:
        name = contact.find_element(By.CSS_SELECTOR, '.header').text
        license_no = None
        content_spans = contact.find_elements(By.CSS_SELECTOR, '.content span.less_span')
        for span in content_spans:
            span_text = span.text
            if '牌照號碼' in span_text:
                license_no = span_text.replace('代理個人牌照號碼:', '').strip()
        # phones = contact.find_elements(By.CSS_SELECTOR, '[attr="phone"]')
        # wtsapps = contact.find_elements(By.CSS_SELECTOR, '[attr="whatsapp"]')
        contacts_data.append({
            "name": name,
            "license_no": license_no,
            # "phones": [phone.get_attribute('href') for phone in phones],
            # "wtsapps": [wtsapp.get_attribute('href') for wtsapp in wtsapps],
        })

    property_dates_div = content_body_div.find_element(By.CSS_SELECTOR, '.propertyDate')
    property_dates = remove_html_tags(property_dates_div.text).split('|')
    posted_date = property_dates[0].replace('刊登:', '').strip()
    updated_date = property_dates[1].replace('更新:', '').strip()
    
    pair_values = content_body_div.find_elements(By.CSS_SELECTOR, 'table.tablePair tr')
    info = {}
    for pair in pair_values:
        names = pair.find_elements(By.CSS_SELECTOR, 'td.table_left')
        if names:
            values = pair.find_elements(By.CSS_SELECTOR, '.pairValue')
            if values:
                info[remove_html_tags(names[0].text)] = remove_html_tags(values[0].text)

    # html = trafilatura.extract(
    #     content_body_div.get_attribute('outerHTML'),
    #     output_format="markdown",
    #     include_tables=True,
    #     include_links=True,
    #     include_images=True,
    #     include_comments=True,
    #     deduplicate=True,
    # )

    # html = trim(html)

    meta = {
        "source_channel": "28hse",
        "source_id": source_id,
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
        "image_links": image_links,
        #"thumb_links": thumb_links,
        "updated_at": datetime.datetime.now().timestamp(),
        # "source_html_content": content_body_div.get_attribute('outerHTML'),
        #"is_markdown": True,
    }

    if prop:
        prop.update(meta)
        print(f"Updated prop {source_id}")
    else:
        meta['source_html_content'] = content_body_div.get_attribute('outerHTML')
        meta['status'] = "pending_extraction"
        prop = Prop.create(db, meta)
        print(f"Created prop {source_id}")
    
    # prop.download_photos()


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
        content = driver1.find_element(By.ID, 'main_content')
        search_results_divs = content.find_elements(By.CSS_SELECTOR, '.property_item')
        for div in search_results_divs:
            try:
                detail_page_link = div.find_element(By.CSS_SELECTOR, 'a.detail_page')
                link = detail_page_link.get_attribute('href')
                # writer.writerow([link])
                try:
                    extract_details(db, driver2, link)
                except Exception as e:
                    print(f"Error extracting details for {link}: {e}")
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

    return driver1, driver2

def extract_sell(db, driver1, driver2):
    buy_url = settings.get("BUY_URL")
    if not buy_url:
        print("BUY_URL is not configured, skipping sale extraction")
        return driver1, driver2

    driver1 = _open_listing_page(driver1, buy_url)
    
    # menu = driver.find_element(By.ID, 'mainMenuDiv')
    # button = menu.find_element(By.CSS_SELECTOR, '[data-value="hk"]')
    # button.click()

    # file_path = os.path.join(FOLDER, f"28hse_links.csv")
    
    def fetch_link():
        nonlocal driver2
        # with open(file_path, "a") as of:
        #     writer = csv.writer(of)
        content = driver1.find_element(By.ID, 'main_content')
        search_results_divs = content.find_elements(By.CSS_SELECTOR, '.property_item')
        for div in search_results_divs:
            # writer.writerow([link])
            try:
                detail_page_link = div.find_element(By.CSS_SELECTOR, 'a.detail_page')
                link = detail_page_link.get_attribute('href')
                try:
                    extract_details(db, driver2, link)
                except Exception as e:
                    print(f"Error extracting details for {link}: {e}")
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

    return driver1, driver2

def extract():
    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client['prop_main']

    mode = os.getenv("N28HSE_EXTRACT_MODE", "http_first").strip().lower()
    if mode in {"http", "http_first"}:
        try:
            print("Running 28hse extraction in HTTP mode")
            extract_rent_http(db)
            if mode == "http":
                return
        except Exception as e:
            if mode == "http":
                raise
            print(f"HTTP mode failed ({e}), falling back to Selenium")

    options = uc.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = create_uc_driver(options=options, use_subprocess=True)

    options2 = uc.ChromeOptions()
    options2.add_argument('--no-sandbox')
    options2.add_argument('--disable-dev-shm-usage')
    driver2 = create_uc_driver(options=options2, use_subprocess=True)

    driver, driver2 = extract_rent(db, driver, driver2)
    driver, driver2 = extract_sell(db, driver, driver2)

    try:
        driver.quit()
    except Exception:
        pass
    try:
        driver2.quit()
    except Exception:
        pass