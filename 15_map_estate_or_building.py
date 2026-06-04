import argparse
import os
import time
import uuid
from datetime import datetime

import requests

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
    return ''
  text = str(value).strip()
  return text if text else ''


def pick_place(places, estate_or_building_name):
  return places[0]

def lookup_sub_district_by_geo_location(db, latitude, longitude):
  # {
  #   "_id": {
  #     "$oid": "6a1fa20533104a0ebe7b98e9"
  #   },
  #   "subDistrictName": "筲箕灣",
  #   "subDistrictNameEN": "Shau Kei Wan",
  #   "districtCode": "EST",
  #   "location": {
  #     "type": "Point",
  #     "coordinates": [
  #       114.2281847952,
  #       22.2791244542
  #     ]
  #   }
  # }

  if latitude is None or longitude is None:
    return None
  sub_district = db['sub_districts'].find_one({
    "location": {
      "$near": {
        "$geometry": {
          "type": "Point",
          "coordinates": [longitude, latitude]
        }
      }
    }
  })
  return sub_district

def search_estate_address(db, prop):
  extracted = prop.get('v1_extracted_data', {})
  estate_or_building_name = normalize_text(extracted.get('estate_or_building_name'))
  district = normalize_text(extracted.get('district'))
  if not estate_or_building_name and not district:
    print(f"no estate_or_building_name or district for property")
    return None
  q = f"{district} {estate_or_building_name}" if district else estate_or_building_name

  try:
    response = requests.get(
      'https://www.als.gov.hk/lookup',
      params={'q': q, 'n': 5},
      headers={
        'Accept': 'application/json',
        'Accept-Language': 'en,zh-Hant',
      },
      timeout=10,
    )
    response.raise_for_status()
    data = response.json()

    suggested = data.get('SuggestedAddress', [])
    if not suggested:
      print(f"no suggested address found for query '{q}'")
      return None

    best = suggested[0]
    premises = best.get('Address', {}).get('PremisesAddress', {})
    eng = premises.get('EngPremisesAddress', {})
    chi = premises.get('ChiPremisesAddress', {})
    geo = premises.get('GeospatialInformation', {})
    score = best.get('ValidationInformation', {}).get('Score')

    eng_street = eng.get('EngStreet', {}) or {}
    eng_estate = eng.get('EngEstate', {}) or {}

    chi_street = chi.get('ChiStreet', {}) or {}
    chi_estate = chi.get('ChiEstate', {}) or {}

    latitude = float(geo['Latitude']) if geo.get('Latitude') else None
    longitude = float(geo['Longitude']) if geo.get('Longitude') else None

    subdistrict = lookup_sub_district_by_geo_location(db, latitude, longitude)
    subdistrict_info = {
      'subdistrict_id': subdistrict['_id'],
      'name': subdistrict.get('subDistrictName'),
      'name_en': subdistrict.get('subDistrictNameEN'),
      'district_code': subdistrict.get('districtCode'),
      'latitude': latitude,
      'longitude': longitude,
    } if subdistrict else None

    if not estate_or_building_name or not score or score < 55:
      return {
        'en': {
          'district': (eng.get('EngDistrict') or {}).get('DcDistrict'),
          'region': eng.get('Region'),
        },
        'zh-hk': {
          'district': (chi.get('ChiDistrict') or {}).get('DcDistrict'),
          'region': chi.get('Region'),
        },
        'subdistrict': subdistrict_info,
        'score': float(score) if score else None,
        'source': 'als_gov_hk',
      }

    return {
      'geo_address': premises.get('GeoAddress'),
      'en': {
        'building_name': eng.get('BuildingName'),
        'estate_name': eng_estate.get('EstateName'),
        'street_name': eng_street.get('StreetName'),
        'street_no': eng_street.get('BuildingNoFrom'),
        'district': (eng.get('EngDistrict') or {}).get('DcDistrict'),
        'region': eng.get('Region'),
      },
      'zh-hk': {
        'building_name': chi.get('BuildingName'),
        'estate_name': chi_estate.get('EstateName'),
        'street_name': chi_street.get('StreetName'),
        'street_no': chi_street.get('BuildingNoFrom'), 
        'district': (chi.get('ChiDistrict') or {}).get('DcDistrict'),
        'region': chi.get('Region'),
      },
      'latitude': latitude,
      'longitude': longitude,
      'subdistrict': subdistrict_info,
      'score': float(score) if score else None,
      'source': 'als_gov_hk',
    }
  except Exception as error:
    print(f"Place search failed for query '{q}': {error}")

  return None


def process_property(db, prop):
  source_id = prop.get('source_id')
  now = datetime.now().timestamp()

  address = search_estate_address(db, prop)
  if address:
    db['props'].update_one(
      {'source_id': source_id},
      {'$set': {
        'address': address,
        'updated_at': now,
      }}
    )
    print(f"Mapped {source_id}")
    return True
  
  db['props'].update_one(
    {'source_id': source_id},
    {'$set': {
      'estate_building_map_error': 'not_found',
      'updated_at': now,
    }}
  )
  print(f"Skip {source_id}: no address found")
  return False


def process_batch(db, batch_size):
  props = list(db['props'].find({
    'v1_extracted_data': { '$exists': True },
    #'address.subdistrict': { '$exists': False },
    #"indexing_status": "indexed",
    'status': { '$ne': "archived" },
    'address': { '$exists': False },
    'estate_building_map_error': { '$exists': False },
  }).sort("created_at", -1).limit(batch_size))

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
    time.sleep(1)  # Add delay to avoid hitting API rate limits
  return len(props), success_count


def main():
  parser = argparse.ArgumentParser(description='Map property records to estate/building records using Google Places.')
  parser.add_argument('--batch-size', type=int, default=50, help='Number of properties to process per batch.')
  parser.add_argument('--max-batches', type=int, default=20, help='Max number of batches to run. 0 means run until no records remain.')
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
