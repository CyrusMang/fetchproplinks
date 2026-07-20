import json
import os
import re
import time
import uuid
from typing import Iterable
from urllib.parse import urljoin
from urllib.request import Request, urlopen

try:
    from dotenv import load_dotenv
except Exception:  # noqa: BLE001
    load_dotenv = None

try:
    from pymongo import MongoClient
except Exception:  # noqa: BLE001
    MongoClient = None


BASE_URL = "https://www.28hse.com"
START_URL = f"{BASE_URL}/agent/"
PAGE_URL_TEMPLATE = f"{BASE_URL}/agent/?page={{page}}"

AGENCY_URL_PATTERN = re.compile(r"^https://www\.28hse\.com/agent/(\d+)/?$")
PAGINATION_ATTR_PATTERN = re.compile(r"attr1='(\d+)'")
HREF_PATTERN = re.compile(r'href="([^"]+)"')
JSON_LD_PATTERN = re.compile(
    r'<script type="application/ld\+json">(.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)
TEL_HREF_PATTERN = re.compile(r'href="tel:([^"]+)"', re.IGNORECASE)
MAILTO_PATTERN = re.compile(r'href="mailto:([^"]+)"', re.IGNORECASE)
WHATSAPP_PATTERN = re.compile(r"whatsapp|wa\.me|api\.whatsapp\.com", re.IGNORECASE)


def fetch_html(url: str, headers: dict[str, str], retries: int = 3) -> str:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = Request(url=url, headers=headers)
            with urlopen(req, timeout=30) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(1.5 * attempt)

    raise RuntimeError(f"Failed to fetch URL after {retries} attempts: {url}") from last_error


def parse_total_pages(first_page_html: str) -> int:
    page_numbers = [int(m.group(1)) for m in PAGINATION_ATTR_PATTERN.finditer(first_page_html)]
    return max(page_numbers) if page_numbers else 1


def extract_agency_links(html: str) -> Iterable[tuple[str, str]]:
    seen: set[str] = set()

    for match in HREF_PATTERN.finditer(html):
        href = match.group(1)
        full_url = urljoin(BASE_URL, href).split("?")[0].rstrip("/")
        parsed = AGENCY_URL_PATTERN.match(full_url)
        if not parsed:
            continue
        if full_url in seen:
            continue

        seen.add(full_url)
        yield full_url, parsed.group(1)


def _safe_json_loads(raw: str) -> dict | None:
    try:
        return json.loads(raw)
    except Exception:
        return None


def extract_real_estate_agent_schema(html: str) -> dict:
    for block in JSON_LD_PATTERN.findall(html):
        payload = _safe_json_loads(block)
        if isinstance(payload, dict) and payload.get("@type") == "RealEstateAgent":
            return payload
    return {}


def first_regex_group(pattern: re.Pattern[str], text: str) -> str | None:
    match = pattern.search(text)
    if not match:
        return None
    return match.group(1).strip() or None


def normalize_tel(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = value.replace("tel:", "").strip()
    return cleaned or None


def build_company_doc(
    agency_id: str,
    agency_url: str,
    zh_html: str,
    en_html: str,
) -> dict:
    zh_schema = extract_real_estate_agent_schema(zh_html)
    en_schema = extract_real_estate_agent_schema(en_html)

    name_zh = zh_schema.get("name") or None
    name_en = en_schema.get("name") or None

    license_number = (
        zh_schema.get("leiCode")
        or en_schema.get("leiCode")
        or None
    )

    contact_email = (
        zh_schema.get("email")
        or en_schema.get("email")
        or first_regex_group(MAILTO_PATTERN, zh_html)
        or None
    )

    contact_number = (
        normalize_tel(zh_schema.get("telephone"))
        or normalize_tel(en_schema.get("telephone"))
        or first_regex_group(TEL_HREF_PATTERN, zh_html)
        or None
    )

    logo_link = (
        zh_schema.get("logo")
        or en_schema.get("logo")
        or ""
    )

    is_whatsapp = bool(WHATSAPP_PATTERN.search(zh_html))

    now_ts = int(time.time())
    doc = {
        "name": {
            "en": name_en,
            "zh-hk": name_zh,
        },
        "type": "agency",
        "contactEmail": contact_email,
        "contactNumber": contact_number,
        "contactNumberIsWhatsapp": is_whatsapp,
        "status": "active",
        "28hse_link": agency_url,
        "28hse_company_logo_link": logo_link,
        "updatedAt": now_ts,
    }

    if license_number:
        doc["licenseNumber"] = license_number

    return doc


def main() -> None:
    if load_dotenv is None or MongoClient is None:
        print("Missing dependencies. Please install: pymongo python-dotenv")
        return

    load_dotenv()
    mongodb_connection_string = os.getenv("MONGODB_CONNECTION_STRING")
    if not mongodb_connection_string:
        print("Missing MONGODB_CONNECTION_STRING in environment.")
        return

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36",
        "Accept-Language": "zh-HK,zh;q=0.9,en;q=0.8",
    }

    mongo_client = MongoClient(mongodb_connection_string)
    db = mongo_client["prop_main"]
    collection = db["companies"]

    # Ensure uniqueness by 28hse_link and keep behavior deterministic across reruns.
    collection.create_index("28hse_link", unique=True)

    first_html = fetch_html(START_URL, headers=headers)
    total_pages = parse_total_pages(first_html)
    print(f"Detected total pages: {total_pages}")

    global_seen: set[str] = set()
    processed = 0
    inserted = 0
    updated = 0
    failed = 0

    for page in range(1, total_pages + 1):
        page_url = START_URL if page == 1 else PAGE_URL_TEMPLATE.format(page=page)
        page_html = first_html if page == 1 else fetch_html(page_url, headers=headers)

        page_processed = 0
        for agency_url, agency_id in extract_agency_links(page_html):
            if agency_url in global_seen:
                continue
            global_seen.add(agency_url)

            page_processed += 1
            processed += 1

            try:
                zh_html = fetch_html(agency_url, headers=headers)
                en_html = fetch_html(f"{BASE_URL}/en/agent/{agency_id}", headers=headers)
                doc = build_company_doc(agency_id, agency_url, zh_html, en_html)

                existing = collection.find_one({"28hse_link": agency_url}, {"code": 1})
                company_code = (existing or {}).get("code") or str(uuid.uuid4())
                doc["code"] = company_code

                now_ts = int(time.time())
                result = collection.update_one(
                    {"28hse_link": agency_url},
                    {
                        "$set": doc,
                        "$setOnInsert": {"createdAt": now_ts},
                    },
                    upsert=True,
                )

                if result.upserted_id is not None:
                    inserted += 1
                else:
                    updated += 1

            except Exception as exc:
                failed += 1
                print(f"Failed to process {agency_url}: {exc}")

            time.sleep(0.2)

        print(
            f"Page {page}/{total_pages}: processed {page_processed} links "
            f"(inserted={inserted}, updated={updated}, failed={failed})"
        )

        if page < total_pages:
            time.sleep(0.35)

    print(
        f"Done. processed={processed}, inserted={inserted}, "
        f"updated={updated}, failed={failed}."
    )

    mongo_client.close()


if __name__ == "__main__":
    main()
