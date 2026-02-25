import calendar
import os
import json
from datetime import datetime, timezone
from hashlib import sha256

from utils import google_place_api
from utils.azure_blob import upload

dir = os.path.dirname(os.path.abspath(__file__))

actions = {
    'autocomplete': google_place_api.autocomplete,
    'textSearch': google_place_api.text_search,
    'nearbySearch': google_place_api.nearby_search,
    'placeDetails': google_place_api.place_details,
    'placeImage': google_place_api.place_image,
}

region_types = [
    'locality',
    'sublocality',
    'postal_code',
    'country',
    'administrative_area_level_1',
    'administrative_area_level_2',
    'administrative_area_level_3'
]

class Place:
    def __init__(self, db, data):
        self.db = db
        self.data = data

    def brief(self):
        return {
            # 'photos': self.data.get('photos', []),
            'displayName': self.data.get('displayName'),
            'primaryType': self.data.get('primaryType'),
            'types': self.data.get('types', []),
            'formattedAddress': self.data.get('formattedAddress'),
            'businessStatus': self.data.get('businessStatus'),
            'priceLevel': self.data.get('priceLevel'),
            'priceRange': self.data.get('priceRange'),
            'rating': self.data.get('rating'),
            'userRatingCount': self.data.get('userRatingCount'),
            # 'websiteUri': self.data.get('websiteUri'),
            'editorialSummary': self.data.get('editorialSummary'),
            'generativeSummary': self.data.get('generativeSummary'),
            'goodForGroups': self.data.get('goodForGroups'),
            'neighborhoodSummary': self.data.get('neighborhoodSummary'),
            'reviews': [{
                'publishTime': review.get('publishTime'),
                'text': review.get('text'),
                'rating': review.get('rating'),
                'authorAttribution': {
                    'displayName': review.get('authorAttribution', {}).get('displayName'),
                },
            } for review in self.data.get('reviews', [])],
            'reviewSummary': self.data.get('reviewSummary'),
            'servesBreakfast': self.data.get('servesBreakfast'),
            'servesLunch': self.data.get('servesLunch'),
            'servesDinner': self.data.get('servesDinner'),
            'servesWine': self.data.get('servesWine'),
            'servesVegetarianFood': self.data.get('servesVegetarianFood'),
            'takeout': self.data.get('takeout'),
            'delivery': self.data.get('delivery'),
            'dineIn': self.data.get('dineIn'),
            'reservable': self.data.get('reservable'),
            'events': self.data.get('events', []),
            'currentOpeningHours': {
                'weekdayDescriptions': self.data.get('currentOpeningHours', {}).get('weekdayDescriptions', []),
            },
            'outdoorSeating': self.data.get('outdoorSeating'),
            'liveMusic': self.data.get('liveMusic'),
            'servesDessert': self.data.get('servesDessert'),
            'goodForChildren': self.data.get('goodForChildren'),
            'allowsDogs': self.data.get('allowsDogs'),
            'restroom': self.data.get('restroom'),
            'goodForWatchingSports': self.data.get('goodForWatchingSports'),
            'paymentOptions': self.data.get('paymentOptions', []),
            'accessibilityOptions': self.data.get('accessibilityOptions', []),
        }

    def is_region(self):
        return any(t in region_types for t in self.data.get('types', []))

    def regions(self):
        if not self.data.get('addressComponents'):
            return []
        return [c for c in self.data['addressComponents'] if c.get('types') and any(t in region_types for t in c['types'])]

    def update(self, data):
        update_data = { '$set': data }
        redownload = False
        if 'photo_blobs' not in self.data or len(self.data['photo_blobs']) < 1:
            update_data['$unset'] = { 'photo_blobs': '' }
            redownload = True
        self.db['places'].update_one({ 'id': self.data['id'] }, update_data)
        self.data = {**self.data, **data}
        # if redownload:
        #     self.download_photos()

    def download_photos(self):
        photo_blobs = []
        usage = Place._30daysUsages(self.db, 'placeImage')
        u = next((u['usage'] for u in usage if u['tier'] == 'normal'), 0)
        photos = self.data.get('photos', [])[:3]
        print('photo usage:', u)
        if u + len(photos) > 950:
            print('photo usage exceeded')
            return
        for photo in photos:
            name = photo.get('name')
            if not name:
                return None
            options = {
                'name': name,
                'heightPx': photo.get('heightPx', 500),
                'widthPx': photo.get('widthPx', 500),
            }
            existing = Place._existing(self.db, 'placeImage', options)
            if existing:
                photo_blobs.append(existing['blob_url'])
            else:
                response = actions['placeImage'](options)
                if response.status_code != 200:
                    break
                blob = upload('places', name, response.content, response.headers.get('content-type'))
                if blob:
                    key = f"placeImage-{json.dumps(options)}".encode(encoding = 'UTF-8', errors = 'strict')
                    hash = sha256(key).hexdigest()
                    self.db['place-requests'].insert_one({
                        'hash': hash,
                        'action': 'placeImage',
                        'args': [options],
                        'tier': 'normal',
                        'requestAt': datetime.now(),
                        'result': blob
                    })
                    photo_blobs.append(blob['blob_url'])
                else:
                    break
        self.db['places'].update_one({'id': self.data['id'] },
            {'$set': {'photo_blobs': photo_blobs}}
        )
        self.data['photo_blobs'] = photo_blobs

    def autocomplete(db, query, options={}):
        opt = {
            'input': query, 
            'includedPrimaryTypes': [options.get('includedType')] if options.get('includedType') else None,
            'includedRegionCodes': [options.get('regionCode')] if options.get('regionCode') else None,
        }
        existing = Place._existing(db, 'autocomplete', opt)
        if existing:
            return existing
        return Place._request(db, 'autocomplete', 'normal', opt)

    def nearby_search(db, location, radius, types=[]):
        opt = { 
            'locationRestriction': {
                'circle': {
                    'center': location,
                    'radius': radius,
                },
            },
            'includedPrimaryTypes': types,
        }
        existing = Place._existing(db, 'nearbySearch', opt)
        if existing:
            placeIds = [place['id'] for place in existing['places']]
            places = Place.find_by_ids(db, placeIds)
            return places
        usage = Place._30daysUsages(db, 'nearbySearch')
        with open(os.path.join(dir, '..', 'static', 'place-nearby-search-data-field.json')) as f:
            place_nearby_search_fields = json.load(f)
            fields = [
                *place_nearby_search_fields['pro']['fields'],
            ]
            tier = 'pro'
            enterprise_atmosphere_usage = next((u['usage'] for u in usage if u['tier'] == 'enterprise_atmosphere'), 0)
            enterprise_usage = next((u['usage'] for u in usage if u['tier'] == 'enterprise'), 0)
            if enterprise_atmosphere_usage < place_nearby_search_fields['enterprise_atmosphere']['free_cap']:
                tier = 'enterprise_atmosphere'
                fields.extend(
                    place_nearby_search_fields['enterprise']['fields'] +
                    place_nearby_search_fields['enterprise_atmosphere']['fields']
                )
            elif enterprise_usage < place_nearby_search_fields['enterprise']['free_cap']:
                tier = 'enterprise'
                fields.extend(
                    place_nearby_search_fields['enterprise']['fields']
                )
            result = Place._request(db, 'nearbySearch', tier, opt, fields)
            if not result or not result.get('places'):
                return []
            places = [Place.create_or_update(db, place) for place in result['places']]
            return places

    def search(db, query, options={}): 
        opt = {'textQuery': query, **options}
        existing = Place._existing(db, 'textSearch', opt)
        if existing:
            placeIds = [place['id'] for place in existing['places']]
            places = Place.find_by_ids(db, placeIds)
            return places
        usage = Place._30daysUsages(db, 'textSearch')
        print(f'usage: {usage}')
        with open(os.path.join(dir, '..', 'static', 'place-text-search-data-field.json')) as f:
            place_text_search_fields = json.load(f)
            fields = [
                *place_text_search_fields['essentials_id_only']['fields'],
            ]
            tier = None
            enterprise_atmosphere_usage = next((u['usage'] for u in usage if u['tier'] == 'enterprise_atmosphere'), 0)
            enterprise_usage = next((u['usage'] for u in usage if u['tier'] == 'enterprise'), 0)
            pro_usage = next((u['usage'] for u in usage if u['tier'] == 'pro'), 0)
            if enterprise_atmosphere_usage < place_text_search_fields['enterprise_atmosphere']['free_cap']:
                tier = 'enterprise_atmosphere'
                fields.extend(
                    place_text_search_fields['pro']['fields'] +
                    place_text_search_fields['enterprise']['fields'] +
                    place_text_search_fields['enterprise_atmosphere']['fields']
                )
            elif enterprise_usage < place_text_search_fields['enterprise']['free_cap']:
                tier = 'enterprise'
                fields.extend(
                    place_text_search_fields['pro']['fields'] +
                    place_text_search_fields['enterprise']['fields']
                )
            elif pro_usage < place_text_search_fields['pro']['free_cap']:
                tier = 'pro'
                fields.extend(
                    place_text_search_fields['pro']['fields']
                )
            if tier:
                result = Place._request(db, 'textSearch', tier, opt, fields)
                if not result or not result.get('places'):
                    return []
                places = [Place.create_or_update(db, place) for place in result['places']]
                return places
            else:
                result = Place.autocomplete(db, query, options)
                suggestions = result.get('suggestions', [])
                places = []
                for s in suggestions:
                    place_prediction = s.get('placePrediction', {})
                    place_id = place_prediction.get('placeId')
                    if place_id:
                        place = Place.details(db, place_id)
                        if place is not None:
                            places.append(place)
                return places

    def details(db, id):
        existing = Place.get_by_id(db, id)
        if existing:
            return existing
        usage = Place._30daysUsages(db, 'placeDetails')
        with open(os.path.join(dir, 'static', 'place-details-data-field.json')) as f:
            place_details_field = json.load(f)
            fields = [
                *place_details_field['essentials_id_only']['fields'],
                *place_details_field['essentials']['fields'],
                *place_details_field['pro']['fields'],
            ]
            tier = 'pro'
            enterprise_atmosphere_usage = next((u['usage'] for u in usage if u['tier'] == 'enterprise_atmosphere'), 0)
            enterprise_usage = next((u['usage'] for u in usage if u['tier'] == 'enterprise'), 0)
            if enterprise_atmosphere_usage < place_details_field['enterprise_atmosphere']['free_cap']:
                tier = 'enterprise_atmosphere'
                fields.extend(
                    place_details_field['enterprise']['fields'] +
                    place_details_field['enterprise_atmosphere']['fields']
                )
            elif enterprise_usage < place_details_field['enterprise']['free_cap']:
                tier = 'enterprise'
                fields.extend(
                    place_details_field['enterprise']['fields']
                )
            result = Place._request(db, 'placeDetails', tier, { 'place_id': id }, fields)
            if result:
                newPlace = Place.create(db, result)
                return newPlace
            return None

    def get_by_id(db, id):
        place = db['places'].find_one({ 'id': id })
        return Place(db, place) if place else None

    def find_by_ids(db, ids):
        places = db['places'].find({ 'id': { '$in': ids } })
        return [Place(db, data) for data in places]

    def create_or_update(db, data):
        place_id = data.get('id')
        if not place_id:
            raise ValueError("Missing place ID")
        existing_place = Place.get_by_id(db, place_id)
        if existing_place:
            existing_place.update(data)
            return existing_place
        else:
            return Place.create(db, data)

    def create(db, data):
        place_doc = db['places'].insert_one(data)
        place = Place(db, { '_id': place_doc.inserted_id, **data })
        # place.download_photos()
        return place

    def _30daysUsages(db, action):
        now = datetime.now(timezone.utc)
        from_date = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        year = from_date.year
        month = from_date.month
        _, num_days = calendar.monthrange(year, month)
        to = datetime(year, month, num_days, hour=23, minute=59, second=59, microsecond=999999)
        from_timestamp = int(from_date.timestamp())
        to_timestamp = int(to.timestamp())
        pipeline = [
            {
                '$match': {
                    'action': action,
                    'requestAt': {
                        '$gte': datetime.fromtimestamp(from_timestamp),
                        '$lt': datetime.fromtimestamp(to_timestamp)
                    }
                }
            },
            {
                '$group': {
                    '_id': '$tier',
                    'usages': {'$sum': 1},
                }
            }
        ]
        usages = list(db['place-requests'].aggregate(pipeline))
        return [{'tier': u['_id'], 'usage': u['usages']} for u in usages]

    def _existing(db, action, option):
        key = f"{action}-{json.dumps(option)}".encode(encoding = 'UTF-8', errors = 'strict')
        hash = sha256(key).hexdigest()
        existing_request = db['place-requests'].find_one({'hash': hash})
        if existing_request:
            return existing_request['result']
        return None

    def _newrequest(db, action, tier, *args):
        key = f"{action}-{json.dumps(args[0])}".encode(encoding = 'UTF-8', errors = 'strict')
        hash = sha256(key).hexdigest()
        result = actions[action](*args)
        db['place-requests'].insert_one({
            'hash': hash,
            'action': action,
            'args': args,
            'tier': tier,
            'requestAt': datetime.now(timezone.utc),
            'result': result
        })
        return result

    def _request(db, action, tier, *args):
        existing = Place._existing(db, action, args[0])
        if existing:
            return existing
        return Place._newrequest(db, action, tier, *args)