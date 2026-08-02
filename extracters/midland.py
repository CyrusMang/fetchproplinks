# import csv
import datetime
import os
import uuid
import time
import re
import random
from urllib.parse import urlparse
import trafilatura
from trafilatura.utils import trim
from pymongo import MongoClient
import undetected_chromedriver as uc
import httpx
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
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

DIR = os.path.dirname(os.path.abspath(__file__))
FOLDER = os.path.join(DIR, '..', ARTIFACTS_FOLDER)

settings = {
    "RENT_URL": "https://www.midland.com.hk/zh-hk/list/rent"
}

HTTP_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)
HTTP_TIMEOUT_SECONDS = 20
HTTP_RETRY_ATTEMPTS = 3


def _is_valid_midland_property_link(link):
    if not link:
        return False

    parsed = urlparse(link)
    host = parsed.netloc.lower()
    if host.startswith('www.'):
        host = host[4:]

    if host != 'midland.com.hk':
        return False

    return re.match(r'^/(zh-hk|zh-cn|en)/property/', parsed.path.lower()) is not None


def _normalize_midland_link(href):
    if not href:
        return None
    if href.startswith('//'):
        href = f"https:{href}"
    elif href.startswith('/'):
        href = f"https://www.midland.com.hk{href}"
    return href.split('#')[0]

def remove_html_tags(text):
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)


def _get_source_fields(link):
    if not _is_valid_midland_property_link(link):
        return None

    parsed = urlparse(link)
    path_parts = [part for part in parsed.path.split('/') if part]
    if len(path_parts) < 3:
        return None

    prop_id = path_parts[-1]
    prop_type = path_parts[-2]
    prop_post_type = path_parts[-3]
    source_id = f"midland-{prop_id.split('-')[-1]}"

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


def _extract_midland_links_from_listing_html(html):
    soup = BeautifulSoup(html, 'lxml')
    links = []
    seen = set()

    for anchor in soup.select('a[href*="/property/"]'):
        href = _normalize_midland_link(anchor.get('href'))
        if href and _is_valid_midland_property_link(href) and href not in seen:
            seen.add(href)
            links.append(href)

    if links:
        return links

    # Fallback: find links embedded in scripts/state payloads.
    for match in re.finditer(r'"(/(?:zh-hk|zh-cn|en)/property/[^"]+)"', html):
        href = _normalize_midland_link(match.group(1))
        if href and _is_valid_midland_property_link(href) and href not in seen:
            seen.add(href)
            links.append(href)

    return links


def _extract_prop_meta_from_detail_html(link, html):
    source_fields = _get_source_fields(link)
    if not source_fields:
        return None

    soup = BeautifulSoup(html, 'lxml')
    content_body_div = soup.select_one('main')
    if content_body_div is None:
        raise ValueError(f"Failed to locate detail content for {link}")

    return {
        "source_channel": "midland",
        "source_id": source_fields["source_id"],
        "source_url": link,
        "type": "apartment",
        "post_type": "rent",
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
        create_meta['created_at'] = datetime.datetime.now().timestamp()
        create_meta['status'] = "pending_extraction"
        Prop.create(db, {**create_meta, "id": str(uuid.uuid4())})
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
    previous_page_links = None
    with httpx.Client(headers=headers, follow_redirects=True, timeout=HTTP_TIMEOUT_SECONDS) as client:
        page_number = 1
        max_pages = int(os.getenv("MIDLAND_HTTP_MAX_PAGES", "1000"))

        while page_number <= max_pages:
            page_url = settings["RENT_URL"] if page_number == 1 else f"{settings['RENT_URL']}/page-{page_number}"
            page_html = _fetch_html_with_retries(client, page_url)
            links = _extract_midland_links_from_listing_html(page_html)

            if not links:
                if page_number == 1:
                    raise RuntimeError("No listing links found on first rent page")
                print(f"No links found on page {page_number}, stopping rent HTTP extraction")
                break

            links_signature = tuple(links)
            if previous_page_links == links_signature:
                print(f"Links repeated on page {page_number}, stopping rent HTTP extraction")
                break
            previous_page_links = links_signature

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
    # photo_element = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '[mediatype="photo"]')))
    # photo_element.click()

    time.sleep(2)  # Wait for page load

    # page_source = driver.page_source
    # soup = BeautifulSoup(page_source, 'html.parser')
    # text_content = soup.get_text(separator=' ', strip=True)

    # image_div = driver.find_element(By.CSS_SELECTOR, "div[class^='SwiperContainer__']")
    # image_links = []
    # images = image_div.find_elements(By.CSS_SELECTOR, '.swiper-slide a img')
    # for img in images:
    #     img_src = img.get_attribute('src')
    #     if img_src and img_src not in image_links:
    #         image_links.append(img_src)
    
    # thumb_links = []
    # thumbs_div = driver.find_element(By.CSS_SELECTOR, "div[class^='SwiperThumbnails__']")
    # thumbs = thumbs_div.find_elements(By.CSS_SELECTOR, '.swiper-slide div div')
    # for thumb in thumbs:
    #     thumb_src = thumb.get_attribute('src')
    #     if thumb_src and thumb_src not in thumb_links:
    #         thumb_links.append(thumb_src)

    content_body_div = driver.find_element(By.CSS_SELECTOR, 'main')

    # html = trafilatura.extract(
    #     content_body_div.get_attribute('outerHTML'),
    #     output_format="markdown",
    #     include_tables=True,
    #     include_links=True,
    #     include_images=True,
    #     include_comments=False,
    #     deduplicate=True,
    # )

    # html = trim(html)
    
    meta = {
        "source_channel": "midland",
        "source_id": source_id,
        "source_url": link,
        "type": "apartment",
        "post_type": 'rent',
        # "image_links": [],
        # "thumb_links": [],
        "updated_at": datetime.datetime.now().timestamp(),
        # "source_html_content": content_body_div.get_attribute('outerHTML'),
        #"is_markdown": True,
    }

    if prop:
        prop.update(meta)
        print(f"Updated prop {source_id}")
    else:
        meta['created_at'] = datetime.datetime.now().timestamp()
        meta['source_html_content'] = content_body_div.get_attribute('outerHTML')
        meta['status'] = "pending_extraction"
        prop = Prop.create(db, {**meta, "id": str(uuid.uuid4())})
        print(f"Created prop {source_id}")
    
    random_number = random.randint(2, 10)

    time.sleep(random_number)  # Wait for page load


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
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.sc-10pgf2f-3')))
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

def extract_rent(db, driver1, driver2):
    driver1 = _open_listing_page(driver1, settings["RENT_URL"])
    
    def fetch_link():
        nonlocal driver2
        # with open(file_path, "a") as of:
        #     writer = csv.writer(of)
        content = driver1.find_element(By.CSS_SELECTOR, '.sc-10pgf2f-3')
        search_results_divs = content.find_elements(By.CSS_SELECTOR, 'a[href*="/property/"]')
        print(f"Found {len(search_results_divs)} properties in rent page.")
        for div in search_results_divs:
            link = div.get_attribute('href')
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
    
    def go_next_page():
        # try:
        content = driver1.find_element(By.CSS_SELECTOR, '.pagetor')
        page_button = content.find_element(By.CSS_SELECTOR, 'a[rel="next"]')
        if page_button.is_displayed():
            driver1.execute_script("arguments[0].scrollIntoView();", page_button)
            # ActionChains(driver1).move_to_element(page_button).click().perform()
            time.sleep(5)  # Wait for page load
            page_button.click()
            time.sleep(5)  # Wait for page load
            return True
        # except:
        #     print("No next page button found.")
        #     return False
        return False
    
    while True:
        fetch_link()
        time.sleep(3)
        has_next = go_next_page()
        if not has_next:
            break

    return driver1, driver2

# def extract_sell(db, driver1, driver2):
#     driver1.get(settings["SELL_URL"])
    
#     # Wait for content to load
#     wait = WebDriverWait(driver1, 10)
#     wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.rmc-tabs-content-wrap')))
    
#     def fetch_link():
#         # with open(file_path, "a") as of:
#         #     writer = csv.writer(of)
#         content = driver1.find_element(By.CSS_SELECTOR, '.rmc-tabs-content-wrap')
#         search_results_divs = content.find_elements(By.CSS_SELECTOR, 'a[href*="/property/"]')
#         print(f"Found {len(search_results_divs)} properties in sell page.")
#         for div in search_results_divs:
#             link = div.get_attribute('href')
#             # writer.writerow([link])
#             try:
#                 extract_details(db, driver2, link)
#             except Exception as e:
#                 print(f"Error extracting details for {link}: {e}")
    
#     def go_next_page():
#         # try:
#         content = driver1.find_element(By.CSS_SELECTOR, '.pagetor')
#         page_button = pagination.find_element(By.CSS_SELECTOR, 'a[rel="next"]')
#         if page_button.is_displayed():
#             page_button.click()
#             time.sleep(2)  # Wait for page load
#             return True
#         # except:
#         #     print("No next page button found.")
#         #     return False
#         return False
    
#     while True:
#         fetch_link()
#         time.sleep(3)
#         has_next = go_next_page()
#         if not has_next:
#             break

def extract():
    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client['prop_main']

    mode = os.getenv("MIDLAND_EXTRACT_MODE", "http_first").strip().lower()
    if mode in {"http", "http_first"}:
        try:
            print("Running Midland extraction in HTTP mode")
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
    # extract_sell(db, driver, driver2)

    try:
        driver.quit()
    except Exception:
        pass
    try:
        driver2.quit()
    except Exception:
        pass