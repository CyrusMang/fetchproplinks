import os
import json
import requests
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
            
            photo_urls = prop.get('v1_extracted_data', {}).get('photo_urls', [])
            existing_links = prop.get('image_links', [])
            all_urls = photo_urls + [link for link in existing_links if link not in photo_urls]
            all_urls = all_urls[:20]  # Match the limit used in batch creation
            
            # Match analysis results with URLs
            analysed_photos = []
            selected_photos = []
            
            for idx, photo_analysis in enumerate(photos):
                if idx < len(all_urls):
                    photo_url = all_urls[idx]
                    
                    photo_data = {
                        'url': photo_url,
                        'description': photo_analysis.get('image_description', ''),
                        'is_indoor': photo_analysis.get('is_indoor', False),
                        'is_human_in_photo': photo_analysis.get('is_human_in_photo', False),
                        'is_violating_policy': photo_analysis.get('is_violating_policy', False),
                        'detected_objects': photo_analysis.get('detected_objects', []),
                        'quality_score': photo_analysis.get('quality_score', 0),
                        'room_type': photo_analysis.get('room_type', 'unknown')
                    }
                    
                    analysed_photos.append(photo_data)
                    
                    # Select high-quality indoor photos without policy violations or people
                    if (photo_data['is_indoor'] and 
                        not photo_data['is_violating_policy'] and 
                        not photo_data['is_human_in_photo'] and
                        photo_data['quality_score'] > 40):
                        selected_photos.append(photo_data)
            
            # Sort selected photos by quality score
            selected_photos.sort(key=lambda x: x['quality_score'], reverse=True)
            
            # Take top 10 or all if less than 10 qualify
            top_photos = selected_photos[:10]
            
            # If no photos meet criteria, take top 5 by quality score from all analyzed
            if len(top_photos) == 0:
                analysed_photos.sort(key=lambda x: x['quality_score'], reverse=True)
                top_photos = analysed_photos[:5]
            
            # Optionally download and upload to Azure Blob Storage
            downloaded_photos = []
            for photo in top_photos[:10]:  # Limit to 10
                try:
                    response = requests.get(photo['url'], timeout=10)
                    if response.status_code == 200:
                        name = photo['url'].split('/')[-1].split('?')[0]
                        blob_info = upload('props', name, response.content, 
                                         response.headers.get('content-type'))
                        photo['blob_url'] = blob_info.get('blob_url')
                        downloaded_photos.append(photo)
                except Exception as e:
                    print(f"Error downloading photo: {e}")
            
            # Update MongoDB
            update_data = {
                'status': 'photo_analysed',
                'photo_analysis_status': 'completed',
                'analysed_photos': analysed_photos,
                'selected_photos': top_photos,
                'downloaded_photos': downloaded_photos,
                'selected_photos_count': len(top_photos)
            }
            
            collection.update_one(
                {'source_id': source_id},
                {'$set': update_data}
            )
            
            print(f"✓ Updated {source_id}: {len(analysed_photos)} analyzed, {len(top_photos)} selected, {len(downloaded_photos)} downloaded")
            return True
            
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON response for {source_id}: {e}")
            return False
            
    except Exception as e:
        print(f"Error processing result: {e}")
        return False

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
    
    print("=" * 50)
    print("SUMMARY")
    print("=" * 50)
    print(f"Total results processed: {total_processed}")
    print(f"Successfully updated: {total_succeeded}")
    print(f"Failed: {total_processed - total_succeeded}")
    
    mongo_client.close()

if __name__ == '__main__':
    main()
