import argparse
import os
from datetime import datetime

from dotenv import load_dotenv
from pymongo import MongoClient

from models.estate_building import EstateBuilding
from models.place import Place

load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")


def normalize_text(value):
	if not value:
		return None
	text = str(value).strip()
	return text if text else None


def build_place_queries(prop):
	extracted = prop.get('v1_extracted_data', {})
	estate_or_building_name = normalize_text(extracted.get('estate_or_building_name'))
	district = normalize_text(extracted.get('district'))

	if not estate_or_building_name:
		return []

	queries = []
	if district:
		queries.append(f"{estate_or_building_name}, {district}, Hong Kong")
	queries.append(f"{estate_or_building_name}, Hong Kong")
	queries.append(estate_or_building_name)

	unique_queries = []
	for query in queries:
		if query not in unique_queries:
			unique_queries.append(query)
	return unique_queries


def pick_place(places, estate_or_building_name):
	if not places:
		return None

	search_name = (estate_or_building_name or '').lower()
	non_region_places = [place for place in places if not place.is_region()]
	candidates = non_region_places if non_region_places else places

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

	queries = build_place_queries(prop)
	for query in queries:
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

	building_data = {
		'id': place_id,
		'place_id': place_id,
		'name': display_name.get('text'),
		'formatted_address': place.data.get('formattedAddress'),
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
	extracted = prop.get('v1_extracted_data', {})
	estate_or_building_name = normalize_text(extracted.get('estate_or_building_name'))
	now = datetime.now().timestamp()

  if estate_or_building_name:
    place = search_estate_place(db, prop)
    if place:
      building = create_or_get_estate_building(db, place)
      if building:
        db['props'].update_one(
          {'source_id': source_id},
          {'$set': {
            'estate_building_id': building.data.get('id'),
            'status': 'mapped_to_estate_building',
            'updated_at': now,
          }}
        )
        print(f"Mapped {source_id} -> {building.data.get('id')}")
        return True
  
  db['props'].update_one(
    {'source_id': source_id},
    {'$set': {
      'status': 'estate_building_map_error',
      'updated_at': now,
    }}
  )
  print(f"Skip {source_id}: missing estate_or_building_name")
  return False


def process_batch(db, batch_size):
	props = list(db['props'].find({
		'status': 'data_extracted',
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
					'estate_building_map_error': str(error),
					'estate_building_map_updated_at': datetime.now().timestamp(),
				}}
			)
			print(f"Error processing {source_id}: {error}")
	return len(props), success_count


def main():
	parser = argparse.ArgumentParser(description='Map property records to estate/building records using Google Places.')
	parser.add_argument('--batch-size', type=int, default=50, help='Number of properties to process per batch.')
	parser.add_argument('--max-batches', type=int, default=0, help='Max number of batches to run. 0 means run until no records remain.')
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
