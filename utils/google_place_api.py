import os
import requests
from dotenv import load_dotenv

load_dotenv()

GOOGLE_KEY = os.getenv("GOOGLE_API_KEY")

ENDPOINT = 'https://places.googleapis.com/v1/places'

def autocomplete(options):
    url = f'{ENDPOINT}:autocomplete'
    headers = {
        'Content-Type': 'application/json',
        'X-Goog-Api-Key': GOOGLE_KEY
    }
    data = {
        **options,
    }
    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        return response.json()
    else:
        raise ValueError(f"Error: {response.status_code}, {response.text}")

def text_search(options, fields):
    url = f'{ENDPOINT}:searchText'
    headers = {
        'Content-Type': 'application/json',
        'X-Goog-Api-Key': GOOGLE_KEY,
        'X-Goog-FieldMask': ','.join(fields)
    }
    data = {
        **options,
        'pageSize': 20
    }
    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        return response.json()
    else:
        raise ValueError(f"Error: {response.status_code}, {response.text}")
    
def nearby_search(options, fields):
    url = f'{ENDPOINT}:nearbySearch'
    headers = {
        'Content-Type': 'application/json',
        'X-Goog-Api-Key': GOOGLE_KEY,
        'X-Goog-FieldMask': ','.join(fields)
    }
    data = {
        **options,
        'maxResultCount': 20
    }
    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        return response.json()
    else:
        raise ValueError(f"Error: {response.status_code}, {response.text}")

def place_details(options, fields):
    url = f'{ENDPOINT}/{options["place_id"]}'
    headers = {
        'Content-Type': 'application/json',
        'X-Goog-Api-Key': GOOGLE_KEY,
        'X-Goog-FieldMask': ','.join(fields)
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        raise ValueError(f"Error: {response.status_code}, {response.text}")

def place_image(options):
    name = options.get('name')
    max_h_px = min(500, options.get('heightPx', 500))
    max_w_px = min(500, options.get('widthPx', 500))
    url = f"https://places.googleapis.com/v1/{name}/media?maxHeightPx={max_h_px}&maxWidthPx={max_w_px}&key={GOOGLE_KEY}"
    print(url)
    response = requests.get(url)
    print(response.status_code)
    return response
