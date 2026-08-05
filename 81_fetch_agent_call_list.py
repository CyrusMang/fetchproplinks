import os
import time
import trafilatura
from urllib.request import Request, urlopen

try:
	from bs4 import BeautifulSoup
except Exception:  # noqa: BLE001
	BeautifulSoup = None

try:
	from dotenv import load_dotenv
except Exception:  # noqa: BLE001
	load_dotenv = None

try:
	from pymongo import MongoClient
except Exception:  # noqa: BLE001
	MongoClient = None


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


def clean_text(value: str | None) -> str:
	return (value or "").strip()


def normalize_hk_phone(value: str) -> str:
	phone = clean_text(value)
	phone = phone.replace("tel:", "")
	phone = phone.replace("+852", "")
	phone = phone.replace("852-", "")
	phone = phone.replace("-", "")
	phone = phone.replace(" ", "")
	return phone


def is_hk_mobile_phone(value: str) -> bool:
	phone = normalize_hk_phone(value)
	if len(phone) != 8 or not phone.isdigit():
		return False
	# HK mobile numbers typically begin with 5, 6, 7, 8, or 9.
	return phone[0] in {"5", "6", "7", "8", "9"}


def parse_agents_from_company_html(html: str) -> list[dict[str, str]]:
	soup = BeautifulSoup(html, "html.parser")
	result: list[dict[str, str]] = []

	agent_items = soup.select("#agentContactDiv .item")
	for item in agent_items:
		content = item.select_one(".content")
		if not content:
			continue
		header = content.select_one(".ui.header")
		name = ""
		if header:
			header_divs = header.find_all("div", recursive=False)
			if header_divs:
				name = clean_text(header_divs[0].get_text())
			if not name:
				name = clean_text(header.get_text(" ", strip=True))

		linence_no = ""
		contact_phone = ""

		for span in content.find_all("span"):
			label = clean_text(span.get_text())
			if "E-" in label or "S-" in label:
				linence_no = label.strip()
			elif span.find("a", href=lambda x: x and x.startswith("tel:")):
				tel_links = span.select('a[href^="tel:"]')
				for tel_link in tel_links:
					raw_phone = clean_text(tel_link.get_text())
					if is_hk_mobile_phone(raw_phone):
						contact_phone = raw_phone
						break
			if linence_no and contact_phone:
				break

		if not linence_no or not contact_phone:
			continue

		result.append(
			{
				"name": name,
				"licenseNumber": linence_no,
				"contactNumber": contact_phone,
			}
		)

	return result


def main() -> None:
	if load_dotenv is None or MongoClient is None or BeautifulSoup is None:
		print("Missing dependencies. Please install: pymongo python-dotenv beautifulsoup4")
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

	company_collection = db["companies"]
	if company_collection.estimated_document_count() == 0:
		company_collection = db["companise"]
	agent_collection = db["profiles"]

	# Keep licenseNumber as the canonical uniqueness key as requested.
	# try:
	# 	agent_collection.drop_index("licenseNumber_1_companyId_1_title_1")
	# except Exception:
	# 	pass

	# agent_collection.create_index("licenseNumber", unique=True)

	companies = company_collection.find(
		{"28hse_link": {"$exists": True, "$ne": ""}},
		{"28hse_link": 1, "code": 1},
	)

	total_companies = 0
	total_agents_found = 0
	inserted = 0
	updated = 0
	failed_companies = 0

	for company in companies:
		link = clean_text(company.get("28hse_link"))
		company_id = company.get("_id")
		if not link:
			continue

		total_companies += 1

		try:
			html = fetch_html(link, headers=headers)
			agents = parse_agents_from_company_html(html)
			total_agents_found += len(agents)

			for agent in agents:
				agent["companyId"] = company_id
				agent["type"] = "agent"
				existing_agent = agent_collection.find_one({"licenseNumber": agent["licenseNumber"]})
				if not existing_agent:
					result = agent_collection.insert_one(agent)
					if result.inserted_id is not None:
						inserted += 1
				elif len(existing_agent.get("adminIds", [])) == 0:
					result = agent_collection.update_one(
						{"licenseNumber": agent["licenseNumber"]},
						{"$set": agent},
					)
					updated += result.modified_count

		except Exception as exc:
			failed_companies += 1
			print(f"Failed company link: {link} | error: {exc}")

		if total_companies % 20 == 0:
			print(
				f"Processed companies={total_companies}, agents_found={total_agents_found}, "
				f"inserted={inserted}, updated={updated}, failed_companies={failed_companies}"
			)

		time.sleep(0.2)

	print(
		f"Done. companies={total_companies}, agents_found={total_agents_found}, "
		f"inserted={inserted}, updated={updated}, failed_companies={failed_companies}"
	)

	mongo_client.close()


if __name__ == "__main__":
	main()
