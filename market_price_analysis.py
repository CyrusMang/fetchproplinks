import argparse
import os
import re
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
ARTIFACTS_FOLDER = os.getenv("ARTIFACTS_FOLDER", "artifacts")

DIR = os.path.dirname(os.path.abspath(__file__))
ARTIFACTS_DIR = os.path.join(DIR, ARTIFACTS_FOLDER)
REPORT_DIR = os.path.join(ARTIFACTS_DIR, "market_price_analysis")

DEFAULT_CURSOR_BATCH_SIZE = 200

# Common region labels that should not be used as district keys.
REGION_LABELS = {
	"香港島",
	"港島",
	"港島區",
	"港島沿線",
	"港島南岸",
	"九龍",
	"九龍區",
	"新界",
	"新界東",
	"新界西",
	"離島",
	"高級豪宅區",
}

# Variants observed in sources to a canonical district/building-area name.
DISTRICT_ALIAS_MAP = {
	"灣仔": "灣仔",
	"湾仔": "灣仔",
	"灣仔區": "灣仔",
	"港島灣仔": "灣仔",
	"港島東": "東區",
	"東區": "東區",
	"東": "東區",
	"銅鑼灣": "銅鑼灣",
	"跑馬地": "跑馬地",
	"西半山": "西半山",
	"上環": "上環",
	"中環": "中環",
	"天后": "天后",
	"炮台山": "炮台山",
	"鰂魚涌": "鰂魚涌",
	"筲箕灣": "筲箕灣",
	"柴灣": "柴灣",
	"西灣河": "西灣河",
	"西營盤": "西營盤",
	"大坑": "大坑",
	"薄扶林": "薄扶林",
	"堅尼地城": "堅尼地城",
	"黃竹坑": "黃竹坑",
	"香港仔": "香港仔",
	"北角": "北角",
	"旺角": "旺角",
	"油麻地": "油麻地",
	"尖沙咀": "尖沙咀",
	"佐敦": "佐敦",
	"何文田": "何文田",
	"土瓜灣": "土瓜灣",
	"紅磡": "紅磡",
	"深水埗": "深水埗",
	"觀塘": "觀塘",
	"九龍站": "九龍站",
	"奧運": "奧運",
	"油塘": "油塘",
	"荔枝角": "荔枝角",
	"大角咀": "大角咀",
	"九龍灣": "九龍灣",
	"馬鞍山": "馬鞍山",
	"沙田": "沙田",
	"大圍": "大圍",
	"將軍澳": "將軍澳",
	"坑口": "坑口",
	"寶琳": "寶琳",
	"康城": "康城",
	"屯門": "屯門",
	"元朗": "元朗",
	"天水圍": "天水圍",
	"粉嶺": "粉嶺",
	"上水": "上水",
	"大埔": "大埔",
	"青衣": "青衣",
	"荃灣": "荃灣",
	"東涌": "東涌",
	"葵芳": "葵芳",
	"油尖旺": "油尖旺",
	"油尖旺區": "油尖旺",
	"葵青": "葵青",
	"葵青區": "葵青",
}

SIMPLIFIED_CHAR_MAP = str.maketrans(
	{
		"湾": "灣",
		"区": "區",
		"鱼": "魚",
		"东": "東",
		"马": "馬",
	}
)

CANONICAL_DISTRICT_MATCHERS = [
	("灣仔", ["灣仔", "湾仔", "灣仔區", "wan chai"]),
	("銅鑼灣", ["銅鑼灣", "causeway bay"]),
	("跑馬地", ["跑馬地", "happy valley"]),
	("西半山", ["西半山", "mid-levels west"]),
	("上環", ["上環", "sheung wan"]),
	("中環", ["中環", "central"]),
	("天后", ["天后", "tin hau"]),
	("炮台山", ["炮台山", "fortress hill"]),
	("鰂魚涌", ["鰂魚涌", "quarry bay"]),
	("筲箕灣", ["筲箕灣", "shau kei wan"]),
	("柴灣", ["柴灣", "chai wan"]),
	("西灣河", ["西灣河", "sai wan ho"]),
	("西營盤", ["西營盤", "sai ying pun"]),
	("大坑", ["大坑", "tai hang"]),
	("薄扶林", ["薄扶林", "pok fu lam"]),
	("堅尼地城", ["堅尼地城", "kennedy town"]),
	("黃竹坑", ["黃竹坑", "wong chuk hang"]),
	("香港仔", ["香港仔", "aberdeen"]),
	("北角", ["北角", "north point"]),
	("東區", ["東區", "eastern district"]),
	("旺角", ["旺角", "mong kok"]),
	("油麻地", ["油麻地", "yau ma tei"]),
	("尖沙咀", ["尖沙咀", "tsim sha tsui"]),
	("佐敦", ["佐敦", "jordan"]),
	("何文田", ["何文田", "ho man tin"]),
	("土瓜灣", ["土瓜灣", "to kwa wan"]),
	("紅磡", ["紅磡", "hung hom"]),
	("深水埗", ["深水埗", "sham shui po"]),
	("觀塘", ["觀塘", "kwun tong"]),
	("九龍站", ["九龍站", "kowloon station"]),
	("奧運", ["奧運", "olympic"]),
	("油塘", ["油塘", "yau tong"]),
	("荔枝角", ["荔枝角", "lai chi kok"]),
	("大角咀", ["大角咀", "tai kok tsui"]),
	("九龍灣", ["九龍灣", "kowloon bay"]),
	("馬鞍山", ["馬鞍山", "ma on shan"]),
	("沙田", ["沙田", "sha tin"]),
	("大圍", ["大圍", "tai wai"]),
	("將軍澳", ["將軍澳", "tseung kwan o"]),
	("坑口", ["坑口", "hang hau"]),
	("寶琳", ["寶琳", "po lam"]),
	("康城", ["康城", "lohas park"]),
	("屯門", ["屯門", "tuen mun"]),
	("元朗", ["元朗", "yuen long"]),
	("天水圍", ["天水圍", "tin shui wai"]),
	("粉嶺", ["粉嶺", "fanling"]),
	("上水", ["上水", "sheung shui"]),
	("大埔", ["大埔", "tai po"]),
	("青衣", ["青衣", "tsing yi"]),
	("荃灣", ["荃灣", "tsuen wan"]),
	("東涌", ["東涌", "tung chung"]),
	("葵芳", ["葵芳", "kwai fong"]),
	("油尖旺", ["油尖旺", "油尖旺區", "yau tsim mong"]),
	("葵青", ["葵青", "葵青區", "kwai tsing"]),
]

HK_ISLAND_DISTRICTS = {
	"灣仔",
	"銅鑼灣",
	"跑馬地",
	"西半山",
	"上環",
	"中環",
	"天后",
	"炮台山",
	"鰂魚涌",
	"筲箕灣",
	"柴灣",
	"西灣河",
	"西營盤",
	"大坑",
	"薄扶林",
	"堅尼地城",
	"黃竹坑",
	"香港仔",
	"北角",
	"東區",
}

KOWLOON_DISTRICTS = {
	"旺角",
	"油麻地",
	"尖沙咀",
	"佐敦",
	"何文田",
	"土瓜灣",
	"紅磡",
	"深水埗",
	"觀塘",
	"九龍站",
	"奧運",
	"油塘",
	"荔枝角",
	"大角咀",
	"九龍灣",
	"油尖旺",
}

NT_DISTRICTS = {
	"馬鞍山",
	"沙田",
	"大圍",
	"將軍澳",
	"坑口",
	"寶琳",
	"康城",
	"屯門",
	"元朗",
	"天水圍",
	"粉嶺",
	"上水",
	"大埔",
	"青衣",
	"荃灣",
	"東涌",
	"葵芳",
	"葵青",
}

DISTRICT_I18N_MAP = {
	"灣仔": {"en": "Wan Chai", "zh_cn": "湾仔"},
	"銅鑼灣": {"en": "Causeway Bay", "zh_cn": "铜锣湾"},
	"跑馬地": {"en": "Happy Valley", "zh_cn": "跑马地"},
	"西半山": {"en": "Mid-Levels West", "zh_cn": "西半山"},
	"上環": {"en": "Sheung Wan", "zh_cn": "上环"},
	"中環": {"en": "Central", "zh_cn": "中环"},
	"天后": {"en": "Tin Hau", "zh_cn": "天后"},
	"炮台山": {"en": "Fortress Hill", "zh_cn": "炮台山"},
	"鰂魚涌": {"en": "Quarry Bay", "zh_cn": "鲗鱼涌"},
	"筲箕灣": {"en": "Shau Kei Wan", "zh_cn": "筲箕湾"},
	"柴灣": {"en": "Chai Wan", "zh_cn": "柴湾"},
	"西灣河": {"en": "Sai Wan Ho", "zh_cn": "西湾河"},
	"西營盤": {"en": "Sai Ying Pun", "zh_cn": "西营盘"},
	"大坑": {"en": "Tai Hang", "zh_cn": "大坑"},
	"薄扶林": {"en": "Pok Fu Lam", "zh_cn": "薄扶林"},
	"堅尼地城": {"en": "Kennedy Town", "zh_cn": "坚尼地城"},
	"黃竹坑": {"en": "Wong Chuk Hang", "zh_cn": "黄竹坑"},
	"香港仔": {"en": "Aberdeen", "zh_cn": "香港仔"},
	"北角": {"en": "North Point", "zh_cn": "北角"},
	"東區": {"en": "Eastern District", "zh_cn": "东区"},
	"旺角": {"en": "Mong Kok", "zh_cn": "旺角"},
	"油麻地": {"en": "Yau Ma Tei", "zh_cn": "油麻地"},
	"尖沙咀": {"en": "Tsim Sha Tsui", "zh_cn": "尖沙咀"},
	"佐敦": {"en": "Jordan", "zh_cn": "佐敦"},
	"何文田": {"en": "Ho Man Tin", "zh_cn": "何文田"},
	"土瓜灣": {"en": "To Kwa Wan", "zh_cn": "土瓜湾"},
	"紅磡": {"en": "Hung Hom", "zh_cn": "红磡"},
	"深水埗": {"en": "Sham Shui Po", "zh_cn": "深水埗"},
	"觀塘": {"en": "Kwun Tong", "zh_cn": "观塘"},
	"九龍站": {"en": "Kowloon Station", "zh_cn": "九龙站"},
	"奧運": {"en": "Olympic", "zh_cn": "奥运"},
	"油塘": {"en": "Yau Tong", "zh_cn": "油塘"},
	"荔枝角": {"en": "Lai Chi Kok", "zh_cn": "荔枝角"},
	"大角咀": {"en": "Tai Kok Tsui", "zh_cn": "大角咀"},
	"九龍灣": {"en": "Kowloon Bay", "zh_cn": "九龙湾"},
	"油尖旺": {"en": "Yau Tsim Mong", "zh_cn": "油尖旺"},
	"馬鞍山": {"en": "Ma On Shan", "zh_cn": "马鞍山"},
	"沙田": {"en": "Sha Tin", "zh_cn": "沙田"},
	"大圍": {"en": "Tai Wai", "zh_cn": "大围"},
	"將軍澳": {"en": "Tseung Kwan O", "zh_cn": "将军澳"},
	"坑口": {"en": "Hang Hau", "zh_cn": "坑口"},
	"寶琳": {"en": "Po Lam", "zh_cn": "宝琳"},
	"康城": {"en": "LOHAS Park", "zh_cn": "康城"},
	"屯門": {"en": "Tuen Mun", "zh_cn": "屯门"},
	"元朗": {"en": "Yuen Long", "zh_cn": "元朗"},
	"天水圍": {"en": "Tin Shui Wai", "zh_cn": "天水围"},
	"粉嶺": {"en": "Fanling", "zh_cn": "粉岭"},
	"上水": {"en": "Sheung Shui", "zh_cn": "上水"},
	"大埔": {"en": "Tai Po", "zh_cn": "大埔"},
	"青衣": {"en": "Tsing Yi", "zh_cn": "青衣"},
	"荃灣": {"en": "Tsuen Wan", "zh_cn": "荃湾"},
	"東涌": {"en": "Tung Chung", "zh_cn": "东涌"},
	"葵芳": {"en": "Kwai Fong", "zh_cn": "葵芳"},
	"葵青": {"en": "Kwai Tsing", "zh_cn": "葵青"},
	"Unknown": {"en": "Unknown", "zh_cn": "未知"},
}


def normalize_text(value):
	if value is None:
		return ""
	text = str(value).strip()
	return text if text else ""


def normalize_district_name(value):
	raw = normalize_text(value)
	if not raw:
		return "Unknown"

	text = raw.translate(SIMPLIFIED_CHAR_MAP)
	text_lower = text.lower()
	text = re.sub(r"\s+", " ", text)
	text = text.replace("，", ",")
	text = text.replace("、", "/")
	text = text.replace("｜", "|")
	text = text.replace("（", "(").replace("）", ")")

	# Remove decorative or bracketed suffixes frequently used in listing titles.
	text = re.sub(r"【[^】]*】", "", text).strip()
	text = re.sub(r"\([^)]*\)", "", text).strip()
	text_lower = text.lower()

	if text in REGION_LABELS or text_lower in {"hong kong island", "kowloon", "new territories"}:
		return "Unknown"

	# Direct phrase matching is more robust than token-only splitting.
	for canonical, patterns in CANONICAL_DISTRICT_MATCHERS:
		for pattern in patterns:
			if re.search(r"[A-Za-z]", pattern):
				if re.search(rf"\b{re.escape(pattern.lower())}\b", text_lower):
					return canonical
			else:
				if pattern in text:
					return canonical

	if text in DISTRICT_ALIAS_MAP:
		return DISTRICT_ALIAS_MAP[text]

	# Handle concatenated forms like 港島灣仔 or 香港島灣仔.
	for region in sorted(REGION_LABELS, key=len, reverse=True):
		if text.startswith(region):
			candidate = text[len(region):].strip(" ,/|-")
			# Only strip concatenated region prefixes when the remainder is a known district alias.
			if candidate in DISTRICT_ALIAS_MAP:
				text = candidate
			break

	tokens = [token.strip() for token in re.split(r"[,/|]", text) if token.strip()]
	if not tokens:
		compact_tokens = [token.strip() for token in text.split(" ") if token.strip()]
		if compact_tokens:
			tokens = compact_tokens
		else:
			return "Unknown"

	cleaned_tokens = []
	for token in tokens:
		if token in REGION_LABELS:
			continue
		for region in sorted(REGION_LABELS, key=len, reverse=True):
			for sep in [" ", ",", "/", "|", "-"]:
				prefix = f"{region}{sep}"
				suffix = f"{sep}{region}"
				if token.startswith(prefix):
					token = token[len(prefix):].strip(" ,/|-")
				if token.endswith(suffix):
					token = token[: -len(suffix)].strip(" ,/|-")
			if token == region:
				token = ""
				break
		token = re.sub(r"^[\-\s]+|[\-\s]+$", "", token)
		if not token:
			continue
		cleaned_tokens.append(token)

	if not cleaned_tokens:
		return "Unknown"

	if len(cleaned_tokens) > 1 and cleaned_tokens[0].endswith("區"):
		second_token = cleaned_tokens[1]
		if second_token not in REGION_LABELS:
			return DISTRICT_ALIAS_MAP.get(second_token, second_token)

	# Prefer canonical aliases first, then keep the first meaningful token.
	for token in cleaned_tokens:
		token = token.replace(" ", "")
		if token.endswith("區") and len(token) > 2:
			base_token = token[:-1]
			if base_token in DISTRICT_ALIAS_MAP:
				return DISTRICT_ALIAS_MAP[base_token]
		if token in DISTRICT_ALIAS_MAP:
			return DISTRICT_ALIAS_MAP[token]

	first_token = cleaned_tokens[0]
	if len(first_token) == 1 and re.search(r"[\u4e00-\u9fff]", first_token):
		return "Unknown"
	return DISTRICT_ALIAS_MAP.get(first_token, first_token)


def parse_number(value):
	if value is None:
		return None
	if isinstance(value, (int, float)):
		return float(value)
	if isinstance(value, str):
		match = re.search(r"\d+(?:\.\d+)?", value.replace(",", ""))
		if match:
			return float(match.group(0))
	return None


def bucket_sqft_price(price_per_sqft):
	if price_per_sqft is None:
		return "unknown"
	if price_per_sqft < 30:
		return "under_30"
	if price_per_sqft < 40:
		return "30_to_39_99"
	if price_per_sqft < 50:
		return "40_to_49_99"
	if price_per_sqft < 60:
		return "50_to_59_99"
	if price_per_sqft < 80:
		return "60_to_79_99"
	return "80_plus"


def price_level_from_median(median_price):
	if median_price is None:
		return "unknown"
	if median_price < 30:
		return "budget"
	if median_price < 40:
		return "affordable"
	if median_price < 50:
		return "mid_market"
	if median_price < 70:
		return "premium"
	return "luxury"


def get_district_name(extracted):
	return normalize_district_name(extracted.get("district"))


def get_building_name(extracted):
	return normalize_text(extracted.get("estate_or_building_name"))


def get_net_size_sqft(extracted):
	net_size = parse_number(extracted.get("net_size_sqft"))
	if net_size is None or net_size <= 0:
		return None
	return net_size


def get_price_per_sqft(extracted):
	direct_price = parse_number(extracted.get("price_sqft"))
	if direct_price is None:
		direct_price = parse_number(extracted.get("price_per_sqft"))
	if direct_price is not None:
		return direct_price

	rent_price = parse_number(extracted.get("rent_price"))
	net_size = parse_number(extracted.get("net_size_sqft"))
	if rent_price is None or net_size is None or net_size <= 0:
		return None

	return rent_price / net_size


def get_rent_price(extracted):
	rent_price = parse_number(extracted.get("rent_price"))
	if rent_price is None or rent_price <= 0:
		return None
	return rent_price


def get_bedroom_group(extracted):
	bedrooms = parse_number(extracted.get("number_of_bedrooms"))
	if bedrooms is None:
		return None
	count = int(bedrooms)
	if count <= 0:
		return "studio"
	if count >= 6:
		return "6+ bedrooms"
	return f"{count} bedrooms"


def bedroom_group_sort_key(group):
	if group == "studio":
		return 0
	if group == "6+ bedrooms":
		return 99
	match = re.match(r"^(\d+) bedrooms$", group)
	if match:
		return int(match.group(1))
	return 100


def create_report_directory():
	os.makedirs(REPORT_DIR, exist_ok=True)


def get_region_for_district(district):
	if district == "東涌":
		return "離島"
	if district in HK_ISLAND_DISTRICTS:
		return "香港島"
	if district in KOWLOON_DISTRICTS:
		return "九龍"
	if district in NT_DISTRICTS:
		return "新界"
	return "Unknown"


def get_i18n_name_for_district(district):
	i18n = DISTRICT_I18N_MAP.get(district)
	if i18n:
		return i18n
	return {
		"en": district,
		"zh_cn": district,
	}


def summarize_district(bucket):
	prices = bucket["price_per_sqft_values"]
	valid_prices = [price for price in prices if price is not None]
	if valid_prices:
		avg_price = sum(valid_prices) / len(valid_prices)
		median_price = statistics.median(valid_prices)
		min_price = min(valid_prices)
		max_price = max(valid_prices)
	else:
		avg_price = None
		median_price = None
		min_price = None
		max_price = None

	bucket_counts = bucket["price_buckets"]
	dominant_bucket = None
	if bucket_counts:
		dominant_bucket = bucket_counts.most_common(1)[0][0]

	building_reports = []
	for building_name, building_bucket in bucket["buildings"].items():
		building_prices = building_bucket["price_per_sqft_values"]
		building_sizes = building_bucket["net_size_sqft_values"]
		bedroom_rent_summary = []
		if building_prices:
			building_avg = sum(building_prices) / len(building_prices)
			building_median = statistics.median(building_prices)
		else:
			building_avg = None
			building_median = None

		if building_sizes:
			building_avg_size = sum(building_sizes) / len(building_sizes)
			building_median_size = statistics.median(building_sizes)
		else:
			building_avg_size = None
			building_median_size = None

		for bedroom_group, rents in building_bucket["bedroom_rent_values"].items():
			if not rents:
				continue
			avg_rent = sum(rents) / len(rents)
			bedroom_rent_summary.append(
				{
					"bedroom_group": bedroom_group,
					"property_count": len(rents),
					"avg_rent_price": round(avg_rent, 2),
				}
			)

		bedroom_rent_summary.sort(
			key=lambda item: (
				bedroom_group_sort_key(item["bedroom_group"]),
				item["bedroom_group"],
			)
		)

		building_reports.append(
			{
				"estate_or_building_name": building_name,
				"property_count": building_bucket["listing_count"],
				"priced_property_count": len(building_prices),
				"sized_property_count": len(building_sizes),
				"avg_sqft_price": round(building_avg, 2) if building_avg is not None else None,
				"median_sqft_price": round(building_median, 2) if building_median is not None else None,
				"avg_net_size_sqft": round(building_avg_size, 2) if building_avg_size is not None else None,
				"median_net_size_sqft": round(building_median_size, 2) if building_median_size is not None else None,
				"bedroom_rent_summary": bedroom_rent_summary,
			}
		)

	building_reports.sort(
		key=lambda item: (
			-item["property_count"],
			-(item["avg_sqft_price"] if item["avg_sqft_price"] is not None else -1),
			item["estate_or_building_name"],
		)
	)

	return {
		"district": bucket["district"],
		"listing_count": bucket["listing_count"],
		"priced_listing_count": len(valid_prices),
		"avg_sqft_price": round(avg_price, 2) if avg_price is not None else None,
		"median_sqft_price": round(median_price, 2) if median_price is not None else None,
		"min_sqft_price": round(min_price, 2) if min_price is not None else None,
		"max_sqft_price": round(max_price, 2) if max_price is not None else None,
		"pricing_level": price_level_from_median(median_price),
		"dominant_price_bucket": dominant_bucket,
		"price_buckets": dict(bucket_counts),
		"buildings": building_reports,
	}


def iter_properties(db, batch_size, types=None, limit=None):
	prop_filter = {
		"v1_extracted_data": {"$exists": True},
		"status": {"$ne": "archived"},
		"$or": [
			{"v1_extracted_data.price_sqft": {"$exists": True, "$ne": None}},
			{"v1_extracted_data.price_per_sqft": {"$exists": True, "$ne": None}},
			{
				"$and": [
					{"v1_extracted_data.rent_price": {"$exists": True, "$ne": None}},
					{"v1_extracted_data.net_size_sqft": {"$exists": True, "$ne": None, "$gt": 0}},
				]
			},
		],
	}
	if types:
		prop_filter["type"] = {"$in": types}

	cursor = db["props"].find(
		prop_filter,
		{
			"source_id": 1,
			"source_url": 1,
			"type": 1,
			"created_at": 1,
			"v1_extracted_data": 1,
		},
	).sort("created_at", -1).batch_size(batch_size)

	seen = 0
	for prop in cursor:
		yield prop
		seen += 1
		if limit and seen >= limit:
			break


def build_district_buckets(properties):
	buckets = defaultdict(
		lambda: {
			"district": None,
			"listing_count": 0,
			"price_per_sqft_values": [],
			"price_buckets": Counter(),
			"buildings": defaultdict(
				lambda: {
					"listing_count": 0,
					"price_per_sqft_values": [],
					"net_size_sqft_values": [],
					"bedroom_rent_values": defaultdict(list),
				}
			),
		}
	)

	total_count = 0
	priced_count = 0

	for prop in properties:
		total_count += 1
		extracted = prop.get("v1_extracted_data", {}) or {}
		district = get_district_name(extracted)
		building_name = get_building_name(extracted)
		bucket = buckets[district]
		bucket["district"] = district
		bucket["listing_count"] += 1
		building_bucket = None
		if building_name:
			building_bucket = bucket["buildings"][building_name]
			building_bucket["listing_count"] += 1

		price_per_sqft = get_price_per_sqft(extracted)
		rent_price = get_rent_price(extracted)
		bedroom_group = get_bedroom_group(extracted)
		net_size_sqft = get_net_size_sqft(extracted)
		if price_per_sqft is not None:
			bucket["price_per_sqft_values"].append(price_per_sqft)
			bucket["price_buckets"][bucket_sqft_price(price_per_sqft)] += 1
			if building_bucket is not None:
				building_bucket["price_per_sqft_values"].append(price_per_sqft)
			priced_count += 1

		if building_bucket is not None and rent_price is not None and bedroom_group is not None:
			building_bucket["bedroom_rent_values"][bedroom_group].append(rent_price)

		if net_size_sqft is not None:
			if building_bucket is not None:
				building_bucket["net_size_sqft_values"].append(net_size_sqft)

	district_reports = [summarize_district(bucket) for bucket in buckets.values()]
	district_reports = [item for item in district_reports if item["listing_count"] > 1]
	district_reports.sort(key=lambda item: (-item["listing_count"], item["district"]))

	overall_prices = [price for bucket in buckets.values() for price in bucket["price_per_sqft_values"] if price is not None]
	overall_avg = sum(overall_prices) / len(overall_prices) if overall_prices else None
	overall_median = statistics.median(overall_prices) if overall_prices else None

	report = {
		"generated_at": datetime.now().isoformat(timespec="seconds"),
		"total_listings": total_count,
		"priced_listings": priced_count,
		"district_count": len(district_reports),
		"overall_avg_sqft_price": round(overall_avg, 2) if overall_avg is not None else None,
		"overall_median_sqft_price": round(overall_median, 2) if overall_median is not None else None,
		"districts": district_reports,
	}
	return report


def render_district_markdown(district_report, region):
	lines = []
	lines.append(f"### {district_report['district']}")
	lines.append(f"- Region: {region}")
	lines.append(f"- Listing count: {district_report['listing_count']}")
	lines.append(f"- Priced listings: {district_report['priced_listing_count']}")
	if district_report["avg_sqft_price"] is not None:
		lines.append(f"- Average 呎價: HK${district_report['avg_sqft_price']:.2f}")
	if district_report["median_sqft_price"] is not None:
		lines.append(f"- Median 呎價: HK${district_report['median_sqft_price']:.2f}")
	lines.append(f"- Pricing level: {district_report['pricing_level']}")
	if district_report["dominant_price_bucket"]:
		lines.append(f"- Dominant price bucket: {district_report['dominant_price_bucket']}")

	price_buckets = district_report["price_buckets"]
	if price_buckets:
		bucket_text = ", ".join(f"{name}={count}" for name, count in sorted(price_buckets.items(), key=lambda item: (-item[1], item[0])))
		lines.append(f"- Price distribution: {bucket_text}")

	lines.append("- Estates:")
	for building in district_report["buildings"]:
		if building["avg_sqft_price"] is not None:
			lines.append(
				f"  - {building['estate_or_building_name']}: {building['property_count']} properties, "
				f"avg HK${building['avg_sqft_price']:.2f}/sqft"
			)
		else:
			lines.append(
				f"  - {building['estate_or_building_name']}: {building['property_count']} properties, no valid 呎價"
			)

	return "\n".join(lines).strip() + "\n"


def build_district_snapshot_document(district_report, report_generated_at):
	region = get_region_for_district(district_report["district"])
	i18n = get_i18n_name_for_district(district_report["district"])
	return {
		"district": district_report["district"],
		"district_name_en": i18n["en"],
		"district_name_zh_cn": i18n["zh_cn"],
		"canonical_names": {
			"zh_hk": district_report["district"],
			"zh_cn": i18n["zh_cn"],
			"en": i18n["en"],
		},
		"region": region,
		"updated_at": datetime.now(timezone.utc),
		"summary": {
			"listing_count": district_report["listing_count"],
			"avg_price_per_sqft": district_report["avg_sqft_price"],
			"median_price_per_sqft": district_report["median_sqft_price"],
			"pricing_level": district_report["pricing_level"],
			"dominant_price_bucket": district_report["dominant_price_bucket"],
			"price_distribution": district_report["price_buckets"],
		},
		"estates": [
			{
				"name": building["estate_or_building_name"],
				"listing_count": building["property_count"],
				"avg_price_per_sqft": building["avg_sqft_price"],
				"avg_net_size": building["avg_net_size_sqft"],
			}
			for building in district_report["buildings"]
		],
		"raw_markdown_report": render_district_markdown(district_report, region),
	}


def persist_report_to_db(db, report):
	collection = db["district_market_stats"]

	updated_count = 0
	for district_report in report["districts"]:
		document = build_district_snapshot_document(district_report, report["generated_at"])
		result = collection.replace_one(
			{"district": district_report["district"]},
			document,
			upsert=True,
		)
		if result.upserted_id is not None or result.matched_count >= 1:
			updated_count += 1

	return updated_count


def main():
	parser = argparse.ArgumentParser(
		description="Stream property data from MongoDB and generate a district rent price report."
	)
	parser.add_argument(
		"--types",
		nargs="*",
		default=None,
		help="Optional property types to include. If omitted, all non-archived rental listings are included.",
	)
	parser.add_argument(
		"--limit",
		type=int,
		default=0,
		help="Optional maximum number of properties to scan. 0 means no limit.",
	)
	parser.add_argument(
		"--cursor-batch-size",
		type=int,
		default=DEFAULT_CURSOR_BATCH_SIZE,
		help="MongoDB cursor batch size for streaming reads.",
	)
	parser.add_argument(
		"--output-dir",
		default=REPORT_DIR,
		help="Deprecated. Report files are no longer written; kept for backward compatibility.",
	)
	args = parser.parse_args()

	if not MONGODB_CONNECTION_STRING:
		print("Missing MONGODB_CONNECTION_STRING in environment.")
		return

	client = MongoClient(MONGODB_CONNECTION_STRING)
	db = client["prop_main"]

	try:
		properties = iter_properties(
			db,
			batch_size=args.cursor_batch_size,
			types=args.types,
			limit=args.limit or None,
		)
		report = build_district_buckets(properties)

		if report["total_listings"] == 0:
			print("No rental listings found for report generation.")
			return

		updated_count = persist_report_to_db(db, report)
		print(f"Saved {updated_count} district snapshot(s) to district_market_stats.")
	finally:
		client.close()


if __name__ == "__main__":
	main()
