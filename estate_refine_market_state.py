import argparse
import os
import re
import statistics
import atexit
import fcntl
from collections import Counter, defaultdict
from datetime import datetime

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

from market_price_analysis import (
  bedroom_group_sort_key,
  bucket_sqft_price,
  get_bedroom_group,
  get_building_name,
  get_district_name,
  get_net_size_sqft,
  get_price_per_sqft,
  get_rent_price,
  normalize_text,
  price_level_from_median,
)

load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
ARTIFACTS_FOLDER = os.getenv("ARTIFACTS_FOLDER", "artifacts")
LOCK_FILE_PATH = os.path.join(
  os.path.dirname(os.path.abspath(__file__)),
  ARTIFACTS_FOLDER,
  '.estate_refine_market_state.lock',
)

WRITE_BATCH_SIZE = 500
NAME_BOUNDARY = set(' 期第座苑閣()-/,.')


def acquire_lock(lock_file_path):
  os.makedirs(os.path.dirname(lock_file_path), exist_ok=True)
  lock_file = open(lock_file_path, 'w')
  try:
    fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
  except BlockingIOError:
    lock_file.close()
    return None

  atexit.register(lock_file.close)
  return lock_file


def normalize_match_key(value):
  text = normalize_text(value)
  text = re.sub(r'\s+', ' ', text).casefold()
  return text.strip()


def empty_stats():
  return {
    'listing_count': 0,
    'prices': [],
    'sizes': [],
    'buckets': Counter(),
    'bedroom_rents': defaultdict(list),
    'districts': Counter(),
  }


def remember_name(exact, prefix_buckets, raw_name, estate_id):
  key = normalize_match_key(raw_name)
  if len(key) < 2:
    return

  keys = [key]
  compact = re.sub(r'\s+', '', key)
  if compact != key and len(compact) >= 2:
    keys.append(compact)

  for index, name_key in enumerate(keys):
    ids = exact.get(name_key)
    if ids is None:
      ids = []
      exact[name_key] = ids
      if index == 0 and len(name_key) >= 3:
        prefix_buckets[name_key[:2]].append((name_key, ids))
    if estate_id not in ids:
      ids.append(estate_id)


def load_estate_index(db):
  exact = {}
  prefix_buckets = defaultdict(list)
  estate_count = 0
  for estate in db['estate'].find({}, {'name': 1, 'estateName': 1, 'phaseName': 1}):
    estate_count += 1
    for field in ('name', 'estateName', 'phaseName'):
      block = estate.get(field) or {}
      if not isinstance(block, dict):
        continue
      for lang in ('zh-hk', 'en'):
        remember_name(exact, prefix_buckets, block.get(lang), estate['_id'])
  return exact, prefix_buckets, estate_count


def load_building_estate_ids(db):
  mapping = {}
  for building in db['estate_buildings'].find(
    {'estate_id': {'$exists': True}},
    {'estate_id': 1},
  ):
    estate_id = building.get('estate_id')
    if estate_id is not None:
      mapping[building['_id']] = estate_id
  return mapping


def unique_match(ids):
  if ids and len(ids) == 1:
    return ids[0]
  return None


def match_prefix(prefix_buckets, key):
  if len(key) < 4:
    return None

  best_len = 0
  best_ids = []
  for estate_key, estate_ids in prefix_buckets.get(key[:2], []):
    if len(estate_key) < 3 or len(estate_key) >= len(key):
      continue
    if not key.startswith(estate_key):
      continue
    boundary = key[len(estate_key)]
    if boundary not in NAME_BOUNDARY and not boundary.isdigit():
      continue
    if len(estate_key) > best_len:
      best_len = len(estate_key)
      best_ids = list(estate_ids)
    elif len(estate_key) == best_len:
      for estate_id in estate_ids:
        if estate_id not in best_ids:
          best_ids.append(estate_id)

  return unique_match(best_ids)


def match_name(exact, prefix_buckets, raw_name):
  key = normalize_match_key(raw_name)
  if len(key) < 2:
    return None
  if key in exact:
    return unique_match(exact[key])

  compact = re.sub(r'\s+', '', key)
  if compact != key and compact in exact:
    return unique_match(exact[compact])
  return match_prefix(prefix_buckets, key)


def listing_names(prop):
  names = []
  address = prop.get('address') or {}
  for lang in ('zh-hk', 'en'):
    block = address.get(lang) or {}
    if isinstance(block, dict) and block.get('estate_name'):
      names.append(block['estate_name'])

  building_name = get_building_name(prop.get('v1_extracted_data') or {})
  if building_name:
    names.append(building_name)

  for lang in ('zh-hk', 'en'):
    block = address.get(lang) or {}
    if isinstance(block, dict) and block.get('building_name'):
      names.append(block['building_name'])
  return names


def resolve_estate_id(prop, building_to_estate, exact, prefix_buckets):
  address = prop.get('address') or {}
  building_id = address.get('building_estate_id')
  if building_id in building_to_estate:
    return building_to_estate[building_id]

  for name in listing_names(prop):
    estate_id = match_name(exact, prefix_buckets, name)
    if estate_id is not None:
      return estate_id
  return None


def iter_market_properties(db, batch_size, types=None, limit=None):
  prop_filter = {
    'v1_extracted_data': {'$exists': True},
    'status': {'$ne': 'archived'},
    '$or': [
      {'v1_extracted_data.price_sqft': {'$exists': True, '$ne': None}},
      {'v1_extracted_data.price_per_sqft': {'$exists': True, '$ne': None}},
      {
        '$and': [
          {'v1_extracted_data.rent_price': {'$exists': True, '$ne': None}},
          {'v1_extracted_data.net_size_sqft': {'$exists': True, '$ne': None, '$gt': 0}},
        ]
      },
    ],
  }
  if types:
    prop_filter['type'] = {'$in': types}

  cursor = db['props'].find(
    prop_filter,
    {
      'source_id': 1,
      'type': 1,
      'address': 1,
      'v1_extracted_data': 1,
    },
  ).sort('created_at', -1).batch_size(batch_size)

  seen = 0
  for prop in cursor:
    yield prop
    seen += 1
    if limit and seen >= limit:
      break


def add_listing(stats, extracted):
  stats['listing_count'] += 1
  district = get_district_name(extracted)
  if district and district != 'Unknown':
    stats['districts'][district] += 1

  price_per_sqft = get_price_per_sqft(extracted)
  if price_per_sqft is not None:
    stats['prices'].append(price_per_sqft)
    stats['buckets'][bucket_sqft_price(price_per_sqft)] += 1

  net_size = get_net_size_sqft(extracted)
  if net_size is not None:
    stats['sizes'].append(net_size)

  rent_price = get_rent_price(extracted)
  bedroom_group = get_bedroom_group(extracted)
  if rent_price is not None and bedroom_group is not None:
    stats['bedroom_rents'][bedroom_group].append(rent_price)


def price_summary(prices):
  if not prices:
    return None, None, None, None
  return (
    round(sum(prices) / len(prices), 2),
    round(statistics.median(prices), 2),
    round(min(prices), 2),
    round(max(prices), 2),
  )


def size_summary(sizes):
  if not sizes:
    return None, None
  return (
    round(sum(sizes) / len(sizes), 2),
    round(statistics.median(sizes), 2),
  )


def rent_by_bedroom(bedroom_rents):
  summary = []
  for bedroom_group, rents in bedroom_rents.items():
    if not rents:
      continue
    summary.append({
      'bedroom_group': bedroom_group,
      'property_count': len(rents),
      'avg_rent_price': round(sum(rents) / len(rents), 2),
    })
  summary.sort(key=lambda item: (
    bedroom_group_sort_key(item['bedroom_group']),
    item['bedroom_group'],
  ))
  return summary


def pick_district(districts):
  named = [
    (name, count)
    for name, count in districts.items()
    if name and name != 'Unknown'
  ]
  if not named:
    return None
  named.sort(key=lambda item: (-item[1], item[0]))
  return named[0][0]


def build_market_state(stats, district_stats, updated_at):
  prices = stats['prices'] if stats else []
  sizes = stats['sizes'] if stats else []
  buckets = stats['buckets'] if stats else Counter()
  bedroom_rents = stats['bedroom_rents'] if stats else {}
  districts = stats['districts'] if stats else Counter()

  avg_price, median_price, min_price, max_price = price_summary(prices)
  avg_size, median_size = size_summary(sizes)
  district_name = pick_district(districts)
  district_state = None
  vs_district = None
  if district_name and district_name in district_stats:
    district = district_stats[district_name]
    district_avg, district_median, _, _ = price_summary(district['prices'])
    district_state = {
      'name': district_name,
      'listing_count': district['listing_count'],
      'priced_property_count': len(district['prices']),
      'avg_price_per_sqft': district_avg,
      'median_price_per_sqft': district_median,
      'pricing_level': price_level_from_median(district_median),
    }
    if median_price is not None and district_median:
      vs_district = round((median_price - district_median) / district_median * 100, 1)

  return {
    'updated_at': updated_at,
    'listing_count': stats['listing_count'] if stats else 0,
    'priced_property_count': len(prices),
    'sized_property_count': len(sizes),
    'avg_price_per_sqft': avg_price,
    'median_price_per_sqft': median_price,
    'min_price_per_sqft': min_price,
    'max_price_per_sqft': max_price,
    'avg_net_size': avg_size,
    'median_net_size': median_size,
    'pricing_level': price_level_from_median(median_price),
    'dominant_price_bucket': buckets.most_common(1)[0][0] if buckets else None,
    'price_distribution': dict(buckets),
    'rent_by_bedroom': rent_by_bedroom(bedroom_rents),
    'district': district_state,
    'vs_district_median_pct': vs_district,
  }


def collect_market_stats(db, building_to_estate, exact, prefix_buckets, batch_size, types, limit):
  estate_stats = defaultdict(empty_stats)
  district_stats = defaultdict(empty_stats)
  scanned = 0
  matched = 0

  for prop in iter_market_properties(db, batch_size, types, limit):
    scanned += 1
    extracted = prop.get('v1_extracted_data') or {}
    add_listing(district_stats[get_district_name(extracted)], extracted)

    estate_id = resolve_estate_id(prop, building_to_estate, exact, prefix_buckets)
    if estate_id is not None:
      matched += 1
      add_listing(estate_stats[estate_id], extracted)

    if scanned % 5000 == 0:
      print(f"Scanned {scanned} listings, matched {matched}")

  return estate_stats, district_stats, scanned, matched


def write_market_states(db, estate_stats, district_stats):
  updated_at = datetime.now().timestamp()
  operations = []
  for estate_id, stats in estate_stats.items():
    operations.append(UpdateOne(
      {'_id': estate_id},
      {'$set': {
        'market_state': build_market_state(stats, district_stats, updated_at),
        'updated_at': updated_at,
      }},
    ))

  cleared = 0
  for estate in db['estate'].find({'market_state': {'$exists': True}}, {'_id': 1}):
    if estate['_id'] in estate_stats:
      continue
    cleared += 1
    operations.append(UpdateOne(
      {'_id': estate['_id']},
      {'$set': {
        'market_state': build_market_state(None, district_stats, updated_at),
        'updated_at': updated_at,
      }},
    ))

  for start in range(0, len(operations), WRITE_BATCH_SIZE):
    db['estate'].bulk_write(operations[start:start + WRITE_BATCH_SIZE], ordered=False)
  return len(estate_stats), cleared


def main():
  lock_file = acquire_lock(LOCK_FILE_PATH)
  if not lock_file:
    print("Another instance is running, skipping this execution.")
    return

  parser = argparse.ArgumentParser(
    description='Attach rent market stats from property listings onto estate documents.'
  )
  parser.add_argument(
    '--types',
    nargs='*',
    default=None,
    help='Optional property types to include. If omitted, the same listings as market_price_analysis are used.',
  )
  parser.add_argument(
    '--limit',
    type=int,
    default=0,
    help='Optional maximum number of listings to scan. 0 means no limit.',
  )
  parser.add_argument(
    '--cursor-batch-size',
    type=int,
    default=200,
    help='MongoDB cursor batch size for streaming reads.',
  )
  args = parser.parse_args()

  if not MONGODB_CONNECTION_STRING:
    print("Missing MONGODB_CONNECTION_STRING in environment.")
    return

  client = MongoClient(MONGODB_CONNECTION_STRING)
  db = client['prop_main']

  try:
    exact, prefix_buckets, estate_count = load_estate_index(db)
    building_to_estate = load_building_estate_ids(db)
    print(f"Indexed {estate_count} estates and {len(building_to_estate)} buildings.")

    estate_stats, district_stats, scanned, matched = collect_market_stats(
      db,
      building_to_estate,
      exact,
      prefix_buckets,
      args.cursor_batch_size,
      args.types,
      args.limit or None,
    )
    updated, cleared = write_market_states(db, estate_stats, district_stats)
    print(
      f"Finished. scanned={scanned}, matched={matched}, "
      f"estates_updated={updated}, estates_cleared={cleared}"
    )
  finally:
    client.close()


if __name__ == '__main__':
  main()
