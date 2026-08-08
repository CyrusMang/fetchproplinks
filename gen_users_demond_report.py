import argparse
import json
import os
import re
import statistics
from collections import Counter
from datetime import datetime

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
ARTIFACTS_FOLDER = os.getenv("ARTIFACTS_FOLDER", "artifacts")

DIR = os.path.dirname(os.path.abspath(__file__))
ARTIFACTS_DIR = os.path.join(DIR, ARTIFACTS_FOLDER)
REPORT_DIR = os.path.join(ARTIFACTS_DIR, "user_demand_insight")

DEFAULT_LIMIT = 0
DEFAULT_TOP_K = 20
DEFAULT_CURSOR_BATCH_SIZE = 300

NUMERIC_PREF_FIELDS = [
	"minBedrooms",
	"maxBedrooms",
	"minPrice",
	"maxPrice",
	"minSize",
	"maxSize",
	"minBuildingAge",
	"maxBuildingAge",
]

BOOLEAN_PREF_FIELDS = [
	"haveCar",
	"likeVillageHouse",
	"havePets",
	"needMaidRoom",
	"preferDirectOwnerListing",
]

LIST_PREF_FIELDS = ["districts", "buildingEstate"]


def number_or_none(value):
	try:
		if value is None:
			return None
		if isinstance(value, str):
			cleaned = value.strip().lower()
			if cleaned == "":
				return None
			cleaned = cleaned.replace(",", "").replace("$", "").replace("hkd", "")
			return float(cleaned)
		return float(value)
	except (TypeError, ValueError):
		return None


def normalize_token(value):
	if value is None:
		return ""
	text = str(value).strip()
	text = re.sub(r"\s+", " ", text)
	return text


def normalize_keyword(value):
	text = normalize_token(value).lower()
	return re.sub(r"\s+", " ", text)


def bool_or_none(value):
	if isinstance(value, bool):
		return value
	if isinstance(value, str):
		token = value.strip().lower()
		if token in ("1", "true", "yes", "y"):
			return True
		if token in ("0", "false", "no", "n"):
			return False
	return None


def summarize_numeric(values):
	if not values:
		return {
			"count": 0,
			"min": None,
			"max": None,
			"avg": None,
			"median": None,
			"p25": None,
			"p75": None,
		}

	sorted_values = sorted(values)
	return {
		"count": len(sorted_values),
		"min": sorted_values[0],
		"max": sorted_values[-1],
		"avg": round(sum(sorted_values) / len(sorted_values), 2),
		"median": round(statistics.median(sorted_values), 2),
		"p25": round(percentile(sorted_values, 25), 2),
		"p75": round(percentile(sorted_values, 75), 2),
	}


def percentile(sorted_values, p):
	if not sorted_values:
		return None
	if len(sorted_values) == 1:
		return sorted_values[0]
	rank = (len(sorted_values) - 1) * (p / 100)
	low = int(rank)
	high = min(low + 1, len(sorted_values) - 1)
	frac = rank - low
	return sorted_values[low] + (sorted_values[high] - sorted_values[low]) * frac


def budget_bucket(min_price, max_price):
	anchor = max_price if max_price is not None else min_price
	if anchor is None:
		return "unknown"
	if anchor < 10000:
		return "<10k"
	if anchor < 20000:
		return "10k-20k"
	if anchor < 30000:
		return "20k-30k"
	if anchor < 50000:
		return "30k-50k"
	if anchor < 80000:
		return "50k-80k"
	return ">=80k"


def iter_conversations(db, state=None, limit=0, batch_size=300):
	query = {}
	if state:
		query["state"] = {"$in": state}

	projection = {
		"threadId": 1,
		"state": 1,
		"updatedAt": 1,
		"createdAt": 1,
		"userPreferences": 1,
	}

	cursor = db["conversations-v2"].find(query, projection).sort("updatedAt", -1).batch_size(batch_size)
	if limit and limit > 0:
		cursor = cursor.limit(limit)
	for conv in cursor:
		yield conv


def build_demand_insight(conversations, top_k=20):
	total_conversations = 0
	with_preferences = 0
	with_query_text = 0

	field_presence = Counter()
	district_counter = Counter()
	estate_counter = Counter()
	query_keyword_counter = Counter()
	state_counter = Counter()
	budget_bucket_counter = Counter()

	numeric_values = {field: [] for field in NUMERIC_PREF_FIELDS}
	boolean_values = {field: Counter({"true": 0, "false": 0}) for field in BOOLEAN_PREF_FIELDS}

	for conv in conversations:
		total_conversations += 1
		state = normalize_token(conv.get("state") or "unknown")
		state_counter[state] += 1

		prefs = conv.get("userPreferences") or {}
		if not isinstance(prefs, dict) or not prefs:
			continue

		with_preferences += 1

		for field in LIST_PREF_FIELDS + NUMERIC_PREF_FIELDS + BOOLEAN_PREF_FIELDS + ["queryText"]:
			value = prefs.get(field)
			if value is not None and value != "" and value != []:
				field_presence[field] += 1

		query_text = normalize_token(prefs.get("queryText"))
		if query_text:
			with_query_text += 1
			for token in split_query_tokens(query_text):
				query_keyword_counter[token] += 1

		districts = prefs.get("districts") or []
		if isinstance(districts, list):
			for district in districts:
				token = normalize_token(district)
				if token:
					district_counter[token] += 1

		estates = prefs.get("buildingEstate") or []
		if isinstance(estates, list):
			for estate in estates:
				token = normalize_token(estate)
				if token:
					estate_counter[token] += 1

		min_price = number_or_none(prefs.get("minPrice"))
		max_price = number_or_none(prefs.get("maxPrice"))
		budget_bucket_counter[budget_bucket(min_price, max_price)] += 1

		for field in NUMERIC_PREF_FIELDS:
			value = number_or_none(prefs.get(field))
			if value is not None:
				numeric_values[field].append(value)

		for field in BOOLEAN_PREF_FIELDS:
			value = bool_or_none(prefs.get(field))
			if value is True:
				boolean_values[field]["true"] += 1
			elif value is False:
				boolean_values[field]["false"] += 1

	field_coverage = []
	denominator = with_preferences if with_preferences > 0 else 1
	for field in ["queryText"] + LIST_PREF_FIELDS + NUMERIC_PREF_FIELDS + BOOLEAN_PREF_FIELDS:
		count = field_presence.get(field, 0)
		field_coverage.append(
			{
				"field": field,
				"count": count,
				"coverage_ratio": round(count / denominator, 4),
			}
		)

	numeric_summary = {field: summarize_numeric(values) for field, values in numeric_values.items()}

	boolean_summary = {}
	for field, counts in boolean_values.items():
		total_answered = counts["true"] + counts["false"]
		ratio = round(counts["true"] / total_answered, 4) if total_answered else None
		boolean_summary[field] = {
			"true_count": counts["true"],
			"false_count": counts["false"],
			"answered_count": total_answered,
			"true_ratio": ratio,
		}

	return {
		"generated_at": datetime.now().isoformat(timespec="seconds"),
		"summary": {
			"total_conversations": total_conversations,
			"conversations_with_user_preferences": with_preferences,
			"preferences_coverage_ratio": round(with_preferences / total_conversations, 4) if total_conversations else 0,
			"conversations_with_query_text": with_query_text,
		},
		"state_distribution": counter_to_sorted_list(state_counter, top_k=50),
		"field_coverage": sorted(field_coverage, key=lambda x: (-x["coverage_ratio"], x["field"])),
		"district_demand_top": counter_to_sorted_list(district_counter, top_k=top_k),
		"estate_demand_top": counter_to_sorted_list(estate_counter, top_k=top_k),
		"query_keyword_top": counter_to_sorted_list(query_keyword_counter, top_k=top_k),
		"budget_distribution": counter_to_sorted_list(budget_bucket_counter, top_k=20),
		"numeric_preferences": numeric_summary,
		"boolean_preferences": boolean_summary,
	}


def split_query_tokens(query_text):
	tokens = re.split(r"[\s,，。.!?！？;；:/\\\-()\[\]{}]+", query_text)
	cleaned = []
	for token in tokens:
		normalized = normalize_keyword(token)
		if len(normalized) < 2:
			continue
		cleaned.append(normalized)
	return cleaned


def counter_to_sorted_list(counter_obj, top_k=20):
	items = sorted(counter_obj.items(), key=lambda x: (-x[1], x[0]))
	if top_k and top_k > 0:
		items = items[:top_k]
	return [{"value": key, "count": value} for key, value in items]


def render_markdown(report, state_filter, limit):
	summary = report["summary"]
	lines = []
	lines.append("# Users Demand Insight Report")
	lines.append("")
	lines.append(f"- Generated at: {report['generated_at']}")
	lines.append(f"- State filter: {', '.join(state_filter) if state_filter else 'none'}")
	lines.append(f"- Fetch limit: {limit if limit and limit > 0 else 'none'}")
	lines.append(f"- Total conversations: {summary['total_conversations']}")
	lines.append(f"- Conversations with userPreferences: {summary['conversations_with_user_preferences']}")
	lines.append(f"- userPreferences coverage: {summary['preferences_coverage_ratio'] * 100:.2f}%")
	lines.append("")

	lines.append("## State Distribution")
	for item in report["state_distribution"]:
		lines.append(f"- {item['value']}: {item['count']}")
	lines.append("")

	lines.append("## Field Coverage")
	for item in report["field_coverage"]:
		lines.append(f"- {item['field']}: {item['count']} ({item['coverage_ratio'] * 100:.2f}%)")
	lines.append("")

	lines.append("## Top District Demand")
	if report["district_demand_top"]:
		for item in report["district_demand_top"]:
			lines.append(f"- {item['value']}: {item['count']}")
	else:
		lines.append("- No district preference found.")
	lines.append("")

	lines.append("## Top Estate/Building Demand")
	if report["estate_demand_top"]:
		for item in report["estate_demand_top"]:
			lines.append(f"- {item['value']}: {item['count']}")
	else:
		lines.append("- No building/estate preference found.")
	lines.append("")

	lines.append("## Top Query Keywords")
	if report["query_keyword_top"]:
		for item in report["query_keyword_top"]:
			lines.append(f"- {item['value']}: {item['count']}")
	else:
		lines.append("- No query text keywords found.")
	lines.append("")

	lines.append("## Budget Distribution")
	for item in report["budget_distribution"]:
		lines.append(f"- {item['value']}: {item['count']}")
	lines.append("")

	lines.append("## Numeric Preference Summary")
	for field, stats in report["numeric_preferences"].items():
		lines.append(
			f"- {field}: count={stats['count']}, min={stats['min']}, p25={stats['p25']}, "
			f"median={stats['median']}, p75={stats['p75']}, max={stats['max']}, avg={stats['avg']}"
		)
	lines.append("")

	lines.append("## Boolean Preference Summary")
	for field, stats in report["boolean_preferences"].items():
		ratio = f"{stats['true_ratio'] * 100:.2f}%" if stats["true_ratio"] is not None else "N/A"
		lines.append(
			f"- {field}: true={stats['true_count']}, false={stats['false_count']}, "
			f"answered={stats['answered_count']}, true_ratio={ratio}"
		)

	return "\n".join(lines).strip() + "\n"


def write_report_files(report, output_dir, state_filter, limit):
	os.makedirs(output_dir, exist_ok=True)
	timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
	json_path = os.path.join(output_dir, f"users_demand_insight-{timestamp}.json")
	md_path = os.path.join(output_dir, f"users_demand_insight-{timestamp}.md")

	with open(json_path, "w", encoding="utf-8") as json_file:
		json.dump(report, json_file, ensure_ascii=False, indent=2)

	with open(md_path, "w", encoding="utf-8") as md_file:
		md_file.write(render_markdown(report, state_filter=state_filter, limit=limit))

	return json_path, md_path


def parse_args():
	parser = argparse.ArgumentParser(
		description="Fetch conversations-v2 userPreferences and generate users demand insight report."
	)
	parser.add_argument(
		"--state",
		nargs="*",
		default=["ACTIVE_TRACKING"],
		help="Conversation state filter. Use empty to include all states.",
	)
	parser.add_argument(
		"--limit",
		type=int,
		default=DEFAULT_LIMIT,
		help="Max number of conversations to read. 0 means no limit.",
	)
	parser.add_argument(
		"--top-k",
		type=int,
		default=DEFAULT_TOP_K,
		help="Top K items in rank-based sections.",
	)
	parser.add_argument(
		"--cursor-batch-size",
		type=int,
		default=DEFAULT_CURSOR_BATCH_SIZE,
		help="MongoDB cursor batch size.",
	)
	parser.add_argument(
		"--output-dir",
		default=REPORT_DIR,
		help="Directory where JSON/Markdown reports will be written.",
	)
	return parser.parse_args()


def main():
	args = parse_args()
	if not MONGODB_CONNECTION_STRING:
		print("Missing MONGODB_CONNECTION_STRING in environment.")
		return

	state_filter = [s for s in args.state if s]

	mongo_client = MongoClient(MONGODB_CONNECTION_STRING)
	db = mongo_client["prop_main"]

	try:
		conversations = iter_conversations(
			db,
			state=state_filter,
			limit=args.limit,
			batch_size=args.cursor_batch_size,
		)
		report = build_demand_insight(conversations, top_k=args.top_k)

		if report["summary"]["total_conversations"] == 0:
			print("No conversations found with current filters.")
			return

		json_path, md_path = write_report_files(
			report,
			output_dir=args.output_dir,
			state_filter=state_filter,
			limit=args.limit,
		)

		print(
			"Done. "
			f"total={report['summary']['total_conversations']}, "
			f"with_preferences={report['summary']['conversations_with_user_preferences']}."
		)
		print(f"JSON report written to: {json_path}")
		print(f"Markdown report written to: {md_path}")
	finally:
		mongo_client.close()


if __name__ == "__main__":
	main()
