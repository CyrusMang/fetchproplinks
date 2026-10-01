import argparse
import math
import os
import time
import atexit
import fcntl
from datetime import datetime

from dotenv import load_dotenv
from pymongo import MongoClient

from models.place import Place

load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
ARTIFACTS_FOLDER = os.getenv("ARTIFACTS_FOLDER", "artifacts")
LOCK_FILE_PATH = os.path.join(
  os.path.dirname(os.path.abspath(__file__)),
  ARTIFACTS_FOLDER,
  '.estate_refine.lock',
)

LANGUAGE_CODE = 'en'
PLACE_SEARCH_RADIUS_METERS = 1500
HOUSING_TYPES = {
  'apartment_building',
  'apartment_complex',
  'condominium_complex',
  'housing_complex',
}

# Google Places primary types, keyed by the amenity stored on the estate.
NEARBY_CATEGORIES = {
  'market': ['market'],
  'supermarket': ['supermarket'],
  'subway_station': ['subway_station'],
  'clinic': ['doctor'],
  'restaurant': ['restaurant'],
  'gym': ['gym'],
  'park': ['park'],
  'car_park': ['parking'],
  'mall': ['shopping_mall'],
}


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


def normalize_text(value):
  if not value:
    return ''
  return ' '.join(str(value).strip().lower().split())


def localized_text(value):
  if isinstance(value, dict):
    text = value.get('text')
    return text.strip() if isinstance(text, str) and text.strip() else None
  if isinstance(value, str) and value.strip():
    return value.strip()
  return None


def distance_meters(lat1, lng1, lat2, lng2):
  radius = 6371000
  phi1 = math.radians(lat1)
  phi2 = math.radians(lat2)
  dphi = math.radians(lat2 - lat1)
  dlambda = math.radians(lng2 - lng1)
  a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
  return 2 * radius * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def place_coordinates(data):
  location = data.get('location') or {}
  return location.get('latitude'), location.get('longitude')


def estate_label(estate):
  name = estate.get('name') or {}
  return name.get('en') or name.get('zh-hk') or str(estate.get('_id'))


def estate_search_query(estate):
  name = estate.get('name') or {}
  street = estate.get('street') or {}
  district = estate.get('district') or {}
  parts = [
    name.get('en') or name.get('zh-hk'),
    street.get('en') or street.get('zh-hk'),
    district.get('en') or district.get('zh-hk'),
    'Hong Kong',
  ]
  return ' '.join(part.strip() for part in parts if part and str(part).strip())


def estate_name_variants(estate):
  variants = []
  for field in ('name', 'estateName', 'phaseName'):
    value = estate.get(field) or {}
    if not isinstance(value, dict):
      continue
    for key in ('en', 'zh-hk'):
      text = normalize_text(value.get(key))
      if text and text not in variants:
        variants.append(text)
  return variants


def name_match_score(estate, display_name):
  place_name = normalize_text(display_name)
  if not place_name:
    return 0

  best = 0
  for variant in estate_name_variants(estate):
    if variant == place_name:
      best = max(best, 100)
    elif variant in place_name or place_name in variant:
      best = max(best, 80)
    else:
      variant_tokens = set(variant.split())
      place_tokens = set(place_name.split())
      if variant_tokens and place_tokens:
        overlap = len(variant_tokens & place_tokens) / len(variant_tokens)
        best = max(best, int(overlap * 60))
  return best


def is_housing_place(data):
  types = set(data.get('types') or [])
  primary = data.get('primaryType')
  if primary:
    types.add(primary)
  return bool(types & HOUSING_TYPES)


def place_distance(place, origin_lat, origin_lng):
  lat, lng = place_coordinates(place.data)
  if None in (origin_lat, origin_lng, lat, lng):
    return None
  return distance_meters(origin_lat, origin_lng, lat, lng)


def score_place(estate, place, origin_lat, origin_lng):
  data = place.data
  score = name_match_score(estate, localized_text(data.get('displayName')))
  if is_housing_place(data):
    score += 30

  distance = place_distance(place, origin_lat, origin_lng)
  if distance is not None:
    score += max(0, 30 * (1 - distance / PLACE_SEARCH_RADIUS_METERS))
  return score


def pick_estate_place(estate, places, origin_lat, origin_lng):
  if not places:
    return None

  housing_nearby = []
  for place in places:
    if not is_housing_place(place.data):
      continue
    distance = place_distance(place, origin_lat, origin_lng)
    if distance is None or distance <= PLACE_SEARCH_RADIUS_METERS:
      housing_nearby.append(place)

  pool = housing_nearby or places
  best = None
  best_score = 0
  for place in pool:
    score = score_place(estate, place, origin_lat, origin_lng)
    if score > best_score:
      best = place
      best_score = score

  if best is None or best_score < 45:
    return None
  return best


def search_estate_place(db, estate, latitude, longitude):
  query = estate_search_query(estate)
  if not query:
    return None

  places = Place.search(db, query, {
    'languageCode': LANGUAGE_CODE,
    'regionCode': 'HK',
    'locationBias': {
      'circle': {
        'center': {
          'latitude': latitude,
          'longitude': longitude,
        },
        'radius': PLACE_SEARCH_RADIUS_METERS,
      },
    },
  })
  return pick_estate_place(estate, places or [], latitude, longitude)


def search_nearby(db, latitude, longitude, radius, nearby_limit):
  nearby = {}
  center = {'latitude': latitude, 'longitude': longitude}
  for category, types in NEARBY_CATEGORIES.items():
    found = Place.nearby_search(
      db,
      center,
      float(radius),
      types,
      rank_preference='DISTANCE',
      language_code=LANGUAGE_CODE,
    )
    ranked = []
    for place in found or []:
      place_id = place.data.get('id')
      if not place_id:
        continue
      distance = place_distance(place, latitude, longitude)
      ranked.append((distance if distance is not None else 10 ** 9, place_id))
    ranked.sort(key=lambda item: item[0])
    nearby[category] = [place_id for _, place_id in ranked[:nearby_limit]]
    time.sleep(0.2)
  return nearby


def refine_estate(db, estate, radius, nearby_limit):
  geo = estate.get('geoLocation') or {}
  latitude = geo.get('latitude')
  longitude = geo.get('longitude')
  if latitude is None or longitude is None:
    raise ValueError('missing_geo')

  place = search_estate_place(db, estate, latitude, longitude)
  nearby = search_nearby(db, latitude, longitude, radius, nearby_limit)
  now = datetime.now().timestamp()

  db['estate'].update_one(
    {'_id': estate['_id']},
    {
      '$set': {
        'place_id': place.data.get('id') if place else None,
        'nearby': nearby,
        'nearbyRadiusMeters': radius,
        'refined_at': now,
        'updated_at': now,
      },
      '$unset': {
        'refine_error': '',
        'place': '',
      },
    },
  )
  counts = {category: len(places) for category, places in nearby.items()}
  print(f"Refined {estate_label(estate)}: place={'yes' if place else 'no'}, nearby={counts}")
  return True


def process_batch(db, batch_size, radius, nearby_limit, force, seen_ids):
  query = { "geoLocation": {"$exists": True} }
  if seen_ids:
    query['_id'] = {'$nin': list(seen_ids)}
  if not force:
    query['refined_at'] = {'$exists': False}
    query['refine_error'] = {'$exists': False}
  estates = list(db['estate'].find(query).sort('created_at', -1).limit(batch_size))
  if not estates:
    return 0, 0

  success_count = 0
  for estate in estates:
    seen_ids.add(estate['_id'])
    try:
      if refine_estate(db, estate, radius, nearby_limit):
        success_count += 1
    except Exception as error:
      db['estate'].update_one(
        {'_id': estate['_id']},
        {'$set': {
          'refine_error': str(error)[:500],
          'updated_at': datetime.now().timestamp(),
        }},
      )
      print(f"Error refining {estate_label(estate)}: {error}")
    time.sleep(0.5)
  return len(estates), success_count


def main():
  lock_file = acquire_lock(LOCK_FILE_PATH)
  if not lock_file:
    print("Another instance is running, skipping this execution.")
    return

  parser = argparse.ArgumentParser(description='Refine estate records with Google Place details and nearby amenities.')
  parser.add_argument('--batch-size', type=int, default=20, help='Number of estates to process per batch.')
  parser.add_argument('--max-batches', type=int, default=5, help='Max number of batches to run. 0 means run until no records remain.')
  parser.add_argument('--radius', type=float, default=800, help='Nearby search radius in meters.')
  parser.add_argument('--nearby-limit', type=int, default=8, help='Max nearby places stored per category.')
  parser.add_argument('--force', action='store_true', help='Reprocess estates that were already refined.')
  args = parser.parse_args()

  if args.radius <= 0 or args.radius > 50000:
    raise SystemExit('--radius must be between 0 and 50000 meters.')
  if args.nearby_limit < 1 or args.nearby_limit > 20:
    raise SystemExit('--nearby-limit must be between 1 and 20.')

  client = MongoClient(MONGODB_CONNECTION_STRING)
  db = client['prop_main']

  total_processed = 0
  total_success = 0
  batch_number = 0
  seen_ids = set()

  while True:
    if args.max_batches and batch_number >= args.max_batches:
      break

    processed_count, success_count = process_batch(
      db,
      args.batch_size,
      args.radius,
      args.nearby_limit,
      args.force,
      seen_ids,
    )
    if processed_count == 0:
      break

    batch_number += 1
    total_processed += processed_count
    total_success += success_count
    print(f"Batch {batch_number}: processed={processed_count}, success={success_count}")

  print(f"Finished. batches={batch_number}, processed={total_processed}, success={total_success}")
  client.close()


if __name__ == '__main__':
  main()
