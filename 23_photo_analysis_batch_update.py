import os
import json
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

def process_photo_analysis_result(result_line, photo_collection):
    """Process a single photo analysis result and update MongoDB."""
    try:
        data = json.loads(result_line)
        custom_id = data.get('custom_id')
        
        # Extract photo_id from custom_id (format: photo-{photo_id})
        photo_id = custom_id.replace('photo-', '')
        
        # Get the response
        response = data.get('response', {})
        body = response.get('body', {})
        choices = body.get('choices', [])
        
        if not choices:
            print(f"No choices found for {photo_id}")
            return False
        
        message = choices[0].get('message', {})
        content = message.get('content', '{}')
        
        # Parse the JSON response
        try:
            analysis_result = json.loads(content)
            
            # Get property to match photo URLs
            photo = photo_collection.find_one({'photo_id': photo_id})
            if not photo:
                print(f"Photo not found: {photo_id}")
                return False
                    
            # Select high-quality indoor photos without policy violations or people
            if (analysis_result['is_photo_of_property'] and
                not analysis_result['is_violating_policy'] and 
                not analysis_result['is_human_in_photo'] and
                analysis_result['quality_score'] > 40):
                try:
                    response = scraper.get(photo['photo_url'], stream=True)
                    response.raise_for_status()

                    name = photo['photo_url'].split('/')[-1].split('?')[0]
                    blob_info = upload('props', name, response.content, response.headers.get('content-type'))
                    analysis_result['blob_url'] = blob_info.get('blob_url')
                except Exception as e:
                    print(f"Error downloading photo: {photo['photo_url']} : {e}")
            
            # Update MongoDB
            update_data = {
                **analysis_result,
                'status': 'photo_analysed',
            }
            
            photo_collection.update_one(
                {'photo_id': photo_id},
                {'$set': update_data}
            )
            
            print(f"✓ Updated {photo['prop_source_id']}: 1 photo analyzed {photo_id}")
            return photo['prop_source_id']
            
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON response for {photo['prop_source_id']} - {photo_id}: {e}")
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
    photo_collection = db['prop_photos']
    
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
        source_ids = set()
        with open(result_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    total_processed += 1
                    source_id = process_photo_analysis_result(line, photo_collection)
                    if source_id:
                        source_ids.add(source_id)
                        total_succeeded += 1
        
        collection.update_many(
            { 'source_id': { '$in': list(source_ids) } },
            { '$set': { 'status': 'photo_analysed' } }
        )

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
