import os
import time
from urllib import response
from pymongo import MongoClient
from dotenv import load_dotenv
from azure.ai.contentunderstanding import ContentUnderstandingClient
from azure.ai.contentunderstanding.models import (
    ContentAnalyzerConfig, 
    ContentFieldSchema, 
    ContentFieldDefinition
)
from azure.core.credentials import AzureKeyCredential
import requests
from models.prop import Prop
from utils.azure_blob import upload

load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
AZURE_CONTENT_UNDERSTANDING_ENDPOINT = os.getenv("AZURE_CONTENT_UNDERSTANDING_ENDPOINT")
AZURE_CONTENT_UNDERSTANDING_KEY = os.getenv("AZURE_CONTENT_UNDERSTANDING_KEY")

client = ContentUnderstandingClient(AZURE_CONTENT_UNDERSTANDING_ENDPOINT, AzureKeyCredential(AZURE_CONTENT_UNDERSTANDING_KEY))

field_schema = ContentFieldSchema(
    fields={
        "image_description": ContentFieldDefinition(
            type="string", 
            method="generate", 
            description="A detailed description of the main subject."
        ),
        "is_indoor": ContentFieldDefinition(
            type="boolean",
            method="generate",
            description="Whether the photo is taken outdoors or indoors."
        ),
        "is_human_in_the_photo": ContentFieldDefinition(
            type="boolean",
            method="generate",
            description="Whether there is a human in the photo."
        ),
        "is_violating_policy": ContentFieldDefinition(
            type="boolean",
            method="generate",
            description="True if the image contains adult content, nudity, or graphic violence."
        ),
        "detected_objects": ContentFieldDefinition(
            type="array",
            method="generate",
            item_definition=ContentFieldDefinition(
                type="string",
                description="List of specific objects found in the image."
            )
        )
    }
)

batch_size = 10

def check_batch(collection, filter, skip=0, limit=batch_size):
    properties = collection.find(filter).sort("updated_at", 1).skip(skip).limit(limit)
    count = 0
    for prop in properties:
        count += 1
        image_links_to_analyze = prop.get('v1_extracted_data', {}).get('photo_urls', [])

        for photo in prop.get('image_links', []):
            if photo not in image_links_to_analyze:
                image_links_to_analyze.append(photo)

        if len(image_links_to_analyze) > 0:
            analyzer_id = f'{prop["source_id"]}-image-analyzer'
            config = ContentAnalyzerConfig(
                base_analyzer_id="prebuilt-image",
                field_schema=field_schema
            )

            print(f"Building analyzer: {analyzer_id}...")
            poller = client.begin_create_analyzer(analyzer_id=analyzer_id, config=config)
            poller.result() # Wait for the analyzer to be ready

            analyze_poller = client.begin_analyze_batch(
                analyzer_id=analyzer_id,
                content_urls=image_links_to_analyze
            )

            analysed_photos = []
            batch_result = analyze_poller.result()
            for entry in batch_result.values:
                if entry.fields:
                    desc = entry.fields.get("image_description")
                    is_indoor = entry.fields.get("is_indoor")
                    is_human_in_the_photo = entry.fields.get("is_human_in_the_photo")
                    is_violating_policy = entry.fields.get("is_violating_policy")
                    objects = entry.fields.get("detected_objects")
                    photo_data = {
                        "origin_url": entry.content_url,
                        "description": desc.value if desc else None,
                        "is_indoor": is_indoor.value if is_indoor else None,
                        "is_violating_policy": is_violating_policy.value if is_violating_policy else None,
                        "is_human_in_the_photo": is_human_in_the_photo.value if is_human_in_the_photo else None,
                        "objects": objects.value if objects else None,
                    }
                    if photo_data['is_indoor'] and not photo_data['is_violating_policy'] and not photo_data['is_human_in_the_photo']:
                        response = requests.get(photo_data['origin_url'])
                        if response.status_code == 200:
                            name = photo_data['origin_url'].split('/')[-1].split('?')[0]
                            photo_data['blob_url'] = upload('props', name, response.content, response.headers.get('content-type'))
                    analysed_photos.append(photo_data)
            collection.update_one(
                { 'source_id': prop['source_id'] },
                { 
                    '$set': { 
                        'status': "photo_analysed", 
                        'analysed_photos': analysed_photos 
                    }   
                }
            )
            print(f"Updated place {prop['source_id']} to photo_analysed due to missing photo URLs.")
    if count < limit:
        return False
    return True

def main():
    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client['prop_main']
    collection = db['props']

    f = {
        'status': "data_extracted",
    }
    skip = 0
    while True:
        if not check_batch(collection, f, skip=skip):
            break
        skip += batch_size
        time.sleep(2)  # Wait for page load
    print("Completed.")
    client.close()

if __name__ == '__main__':
   main()