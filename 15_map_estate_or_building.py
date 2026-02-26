import argparse
import os
import uuid
from datetime import datetime

from dotenv import load_dotenv
from pymongo import MongoClient

from models.estate_building import EstateBuilding
from models.place import Place

load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")

allow_types = [
  "apartment_building",
  "apartment_complex",
  "condominium_complex",
  "housing_complex",
  "service",
  "business_center",
  "premise",
  "establishment",
  "point_of_interest",
]


def normalize_text(value):
  if not value:
    return None
  text = str(value).strip()
  return text if text else None


def build_place_query(prop):
  prop_type = prop.get('type')
  extracted = prop.get('v1_extracted_data', {})
  estate_or_building_name = normalize_text(extracted.get('estate_or_building_name'))
  district = normalize_text(extracted.get('district'))

  if not estate_or_building_name:
    return None

  if district:
    return f"{prop_type} {estate_or_building_name}, {district}, Hong Kong"
  return f"{prop_type} {estate_or_building_name}, Hong Kong"


def pick_place(places, estate_or_building_name):
  if not places:
    return None

  search_name = (estate_or_building_name or '').lower()
  candidates = [place for place in places if not place.is_region() and place.data.get('primaryType') in allow_types]

  if search_name:
    for place in candidates:
      display_name = (place.data.get('displayName', {}).get('text', '') or '').lower()
      if display_name and display_name == search_name:
        return place

    for place in candidates:
      display_name = (place.data.get('displayName', {}).get('text', '') or '').lower()
      if display_name and search_name in display_name:
        return place

  return candidates[0]


def search_estate_place(db, prop):
  extracted = prop.get('v1_extracted_data', {})
  estate_or_building_name = normalize_text(extracted.get('estate_or_building_name'))
  if not estate_or_building_name:
    return None

  query = build_place_query(prop)
  if not query:
    return None

  try:
    places = Place.search(db, query, {
      'regionCode': 'hk',
      'languageCode': 'zh-HK',
    })
    place = pick_place(places, estate_or_building_name)
    if place:
      return place
  except Exception as error:
    print(f"Place search failed for query '{query}': {error}")

  return None


def create_or_get_estate_building(db, place):
  place_id = place.data.get('id')
  if not place_id:
    return None

  existing_building = EstateBuilding.get_by_placeid(db, place_id)
  if existing_building:
    return existing_building

  now = datetime.now().timestamp()
  display_name = place.data.get('displayName', {})
  location = place.data.get('location', {})

  name = {}
  name[display_name.get('languageCode', 'zh-HK')] = display_name.get('text')

  building_data = {
    'id': str(uuid.uuid4()),
    'place_id': place_id,
    'name': name,
    'formatted_address': place.data.get('formattedAddress'),
    'regions': place.regions(),
    'types': place.data.get('types', []),
    'primary_type': place.data.get('primaryType'),
    'location': {
      'latitude': location.get('latitude'),
      'longitude': location.get('longitude'),
    },
    'created_at': now,
    'updated_at': now,
  }
  return EstateBuilding.create(db, building_data)


def process_property(db, prop):
  source_id = prop.get('source_id')
  now = datetime.now().timestamp()

  place = search_estate_place(db, prop)
  if place:
    building = create_or_get_estate_building(db, place)
    if building:
      db['props'].update_one(
        {'source_id': source_id},
        {'$set': {
          'estate_building_id': building.data.get('id'),
          'estate_building_regions': building.data.get('regions', []),
          'updated_at': now,
        }}
      )
      print(f"Mapped {source_id} -> {building.data.get('id')}")
      return True
  
  db['props'].update_one(
    {'source_id': source_id},
    {'$set': {
      'estate_building_map_error': 'not_found',
      'updated_at': now,
    }}
  )
  print(f"Skip {source_id}: missing estate_or_building_name")
  return False


def process_batch(db, batch_size):
  props = list(db['props'].find({
    'status': "data_extracted",
    'estate_building_id': { '$exists': False },
    'estate_building_map_error': { '$exists': False },
  }).limit(batch_size))

  if not props:
    return 0, 0

  success_count = 0
  for prop in props:
    try:
      if process_property(db, prop):
        success_count += 1
    except Exception as error:
      source_id = prop.get('source_id')
      db['props'].update_one(
        {'source_id': source_id},
        {'$set': {
          'estate_building_map_error': 'unexpected_error',
          'updated_at': datetime.now().timestamp(),
        }}
      )
      print(f"Error processing {source_id}: {error}")
  return len(props), success_count


def main():
  parser = argparse.ArgumentParser(description='Map property records to estate/building records using Google Places.')
  parser.add_argument('--batch-size', type=int, default=5, help='Number of properties to process per batch.')
  parser.add_argument('--max-batches', type=int, default=1, help='Max number of batches to run. 0 means run until no records remain.')
  args = parser.parse_args()

  client = MongoClient(MONGODB_CONNECTION_STRING)
  db = client['prop_main']

  total_processed = 0
  total_success = 0
  batch_number = 0

  while True:
    if args.max_batches and batch_number >= args.max_batches:
      break

    processed_count, success_count = process_batch(db, args.batch_size)
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
