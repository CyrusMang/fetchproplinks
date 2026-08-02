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

HTTP_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)
HTTP_TIMEOUT_SECONDS = 20
HTTP_RETRY_ATTEMPTS = 3


def _is_valid_house730_property_link(link):
    if not link:
        return False

    parsed = urlparse(link)
    host = parsed.netloc.lower()
    if host.startswith('www.'):
        host = host[4:]
    if host != 'house730.com':
        return False

    path_parts = [part for part in parsed.path.split('/') if part]
    if len(path_parts) < 2:
        return False
    if path_parts[0] not in {'rent', 'buy'}:
        return False

    return re.search(r'-\d+$', path_parts[-1]) is not None


def _normalize_house730_link(href):
    if not href:
        return None
    if href.startswith('//'):
        href = f"https:{href}"
    elif href.startswith('/'):
        href = f"https://www.house730.com{href}"
    return href.split('#')[0]


def _is_cloudflare_blocked_html(html):
    lowered = html.lower()
    return (
        'attention required! | cloudflare' in lowered
        or 'sorry, you have been blocked' in lowered
        or 'id="cf-error-details"' in lowered
    )


def _get_source_fields(link):
    if not _is_valid_house730_property_link(link):
        return None

    parsed = urlparse(link)
    path_parts = [part for part in parsed.path.split('/') if part]
    slug = path_parts[-1]
    match = re.search(r'-(\d+)$', slug)
    if not match:
        return None

    prop_id = match.group(1)
    prop_post_type = path_parts[0]
    source_id = f"house730-{prop_id}"

    return {
        "prop_id": prop_id,
        "prop_post_type": prop_post_type,
        "source_id": source_id,
    }


def _fetch_html_with_retries(client, url, retries=HTTP_RETRY_ATTEMPTS):
    for attempt in range(1, retries + 1):
        try:
            response = client.get(url)
            response.raise_for_status()
            html = response.text
            if _is_cloudflare_blocked_html(html):
                raise RuntimeError("Cloudflare blocked HTTP request")
            return html
        except Exception as e:
            if attempt == retries:
                raise
            delay = min(5, attempt)
            print(f"HTTP fetch failed for {url} (attempt {attempt}/{retries}): {e}")
            time.sleep(delay)


def _extract_house730_links_from_listing_html(html):
    soup = BeautifulSoup(html, 'lxml')
    links = []
    seen = set()

    for anchor in soup.select('a.card-content-title[href], a[href*="/rent/"][href], a[href*="/buy/"][href]'):
        href = _normalize_house730_link(anchor.get('href'))
        if href and _is_valid_house730_property_link(href) and href not in seen:
            seen.add(href)
            links.append(href)

    return links


def _extract_prop_meta_from_detail_html(link, html):
    source_fields = _get_source_fields(link)
    if not source_fields:
        return None

    soup = BeautifulSoup(html, 'lxml')
    content_body_div = soup.select_one('#pc-services-detail')
    if content_body_div is None:
        raise ValueError(f"Failed to locate detail content for {link}")

    image_links = []
    for meta_tag in soup.select('meta[property="og:image"]'):
        image_url = meta_tag.get('content')
        if image_url and image_url not in image_links:
            image_links.append(image_url)

    return {
        "source_channel": "house730",
        "source_id": source_fields["source_id"],
        "source_url": link,
        "type": 'apartment',
        "post_type": source_fields["prop_post_type"],
        "updated_at": datetime.datetime.now().timestamp(),
        "image_links": image_links,
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
        max_pages = int(os.getenv("HOUSE730_HTTP_MAX_PAGES", "1000"))

        while page_number <= max_pages:
            page_url = settings["RENT_URL"] if page_number == 1 else f"{settings['RENT_URL']}p{page_number}/"
            page_html = _fetch_html_with_retries(client, page_url)
            links = _extract_house730_links_from_listing_html(page_html)

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
        "source_channel": "house730",
        "source_id": source_id,
        "source_url": link,
        "type": 'apartment',
        "post_type": prop_post_type,
        "updated_at": datetime.datetime.now().timestamp(),
        "image_links": image_links,
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
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.service-list-contnet')))
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

    mode = os.getenv("HOUSE730_EXTRACT_MODE", "http_first").strip().lower()
    if mode in {"http", "http_first"}:
        try:
            print("Running House730 extraction in HTTP mode")
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

    try:
        driver.quit()
    except Exception:
        pass
    try:
        driver2.quit()
    except Exception:
        pass