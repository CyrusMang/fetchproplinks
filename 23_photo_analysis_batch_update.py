import os
import json
import requests
import cloudscraper
from openai import AzureOpenAI
from pymongo import MongoClient
from dotenv import load_dotenv
from utils.azure_blob import upload

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_ENDPOINT = os.getenv("OPENAI_API_ENDPOINT")
OPENAI_API_VERSION = os.getenv("OPENAI_API_VERSION")
MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
ARTIFACTS_FOLDER = os.getenv("ARTIFACTS_FOLDER")

dir = os.path.dirname(os.path.abspath(__file__))
artifacts = os.path.join(dir, ARTIFACTS_FOLDER)
folder = os.path.join(artifacts, 'photo_analysis')

scraper = cloudscraper.create_scraper()

def get_completed_batches(folder_path):
    """Get all completed batch files."""
    files = []
    for root, dirs, filenames in os.walk(folder_path):
        for filename in filenames:
            if filename.endswith('.json'):
                file_path = os.path.join(root, filename)
                with open(file_path, 'r', encoding='utf-8') as f:
                    batch_info = json.load(f)
                    if batch_info.get('status') == 'completed':
                        files.append(file_path)
    return files

def download_batch_results(client, batch_info, batch_code):
    """Download and save batch results."""
    output_file_id = batch_info.get('output_file_id')
    
    if not output_file_id:
        print("No output file ID found.")
        return None
    
    try:
        # Download the result file content
        file_response = client.files.content(output_file_id)
        
        # Save to results folder
        result_file_path = os.path.join(folder, 'results', f"batch-{batch_code}-result.jsonl")
        with open(result_file_path, 'w', encoding='utf-8') as f:
            f.write(file_response.text)
        
        print(f"Results saved to: {result_file_path}")
        return result_file_path
        
    except Exception as e:
        print(f"Error downloading results: {e}")
        return None

def process_photo_analysis_result(result_line, collection):
    """Process a single photo analysis result and update MongoDB."""
    try:
        data = json.loads(result_line)
        custom_id = data.get('custom_id')
        
        # Extract source_id from custom_id (format: photo-{source_id})
        source_id = custom_id.replace('photo-', '')
        
        # Get the response
        response = data.get('response', {})
        body = response.get('body', {})
        choices = body.get('choices', [])
        
        if not choices:
            print(f"No choices found for {source_id}")
            return False
        
        message = choices[0].get('message', {})
        content = message.get('content', '{}')
        
        # Parse the JSON response
        try:
            analysis_result = json.loads(content)
            
            # Handle both direct array and wrapped object responses
            if isinstance(analysis_result, dict) and 'photos' in analysis_result:
                photos = analysis_result['photos']
            elif isinstance(analysis_result, list):
                photos = analysis_result
            else:
                print(f"Unexpected response format for {source_id}")
                return False
            
            # Get property to match photo URLs
            prop = collection.find_one({'source_id': source_id})
            if not prop:
                print(f"Property not found: {source_id}")
                return False
            
            # Match analysis results with URLs
            analysed_photos = []
            
            for idx, photo_analysis in enumerate(photos):
                photo_url = photo_analysis.get('original_url')
                if photo_url:
                    
                    photo_data = {
                        'original_url': photo_url,
                        'description': photo_analysis.get('image_description', ''),
                        'is_indoor': photo_analysis.get('is_indoor', False),
                        'is_human_in_photo': photo_analysis.get('is_human_in_photo', False),
                        'is_violating_policy': photo_analysis.get('is_violating_policy', False),
                        'detected_objects': photo_analysis.get('detected_objects', []),
                        'quality_score': photo_analysis.get('quality_score', 0),
                        'room_type': photo_analysis.get('room_type', 'unknown')
                    }
                    
                    # Select high-quality indoor photos without policy violations or people
                    if (not photo_data['is_violating_policy'] and 
                        not photo_data['is_human_in_photo'] and
                        photo_data['quality_score'] > 40):
                        try:
                            response = scraper.get(photo_data['original_url'], stream=True)
                            response.raise_for_status()

                            name = photo_data['original_url'].split('/')[-1].split('?')[0]
                            blob_info = upload('props', name, response.content, response.headers.get('content-type'))
                            photo_data['blob_url'] = blob_info.get('blob_url')
                        except Exception as e:
                            print(f"Error downloading photo: {photo_data['original_url']} : {e}")
                    
                    analysed_photos.append(photo_data)
            
            analysed_photos.sort(key=lambda x: x['quality_score'], reverse=True)

            # Update MongoDB
            update_data = {
                'status': 'photo_analysed',
                'analysed_photos': analysed_photos,
            }
            
            collection.update_one(
                {'source_id': source_id},
                {'$set': update_data}
            )
            
            print(f"✓ Updated {source_id}: {len(analysed_photos)} analyzed")
            return True
            
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON response for {source_id}: {e}")
            return False
            
    except Exception as e:
        print(f"Error processing result: {e}")
        return False

def remove_file(file_path):
    try:
        os.remove(file_path)
        print(f"Removed file: {file_path}")
    except Exception as e:
        print(f"Error removing file {file_path}: {e}")

def main():
    # Initialize clients
    openai_client = AzureOpenAI(
        azure_endpoint=OPENAI_API_ENDPOINT,
        api_key=OPENAI_API_KEY,
        api_version=OPENAI_API_VERSION
    )
    
    mongo_client = MongoClient(MONGODB_CONNECTION_STRING)
    db = mongo_client['prop_main']
    collection = db['props']
    
    # Get completed batches
    completed_batches = get_completed_batches(os.path.join(folder, 'upload_batches'))
    
    if not completed_batches:
        print("No completed batches found.")
        print("Run 22_photo_analysis_batch_track.py to check batch status first.")
        return
    
    print(f"Found {len(completed_batches)} completed batch(es)\n")
    
    total_processed = 0
    total_succeeded = 0
    
    for batch_file_path in completed_batches:
        with open(batch_file_path, 'r', encoding='utf-8') as f:
            batch_info = json.load(f)
        
        batch_code = batch_file_path.split('/')[-1].replace('batch-', '').replace('.json', '')
        batch_id = batch_info['batch_id']
        
        print(f"Processing batch: {batch_id}")
        
        # Download results
        result_file_path = download_batch_results(openai_client, batch_info, batch_code)
        
        if not result_file_path:
            continue
        
        # Process each result
        with open(result_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    total_processed += 1
                    if process_photo_analysis_result(line, collection):
                        total_succeeded += 1
        
        print()
        remove_file(batch_file_path)
        #remove_file(result_file_path)
    
    print("=" * 50)
    print("SUMMARY")
    print("=" * 50)
    print(f"Total results processed: {total_processed}")
    print(f"Successfully updated: {total_succeeded}")
    print(f"Failed: {total_processed - total_succeeded}")
    
    mongo_client.close()

if __name__ == '__main__':
    main()
