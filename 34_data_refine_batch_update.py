import os
import json
from datetime import datetime
from openai import AzureOpenAI
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_ENDPOINT = os.getenv("OPENAI_API_ENDPOINT")
OPENAI_API_VERSION = os.getenv("OPENAI_API_VERSION")
MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
ARTIFACTS_FOLDER = os.getenv("ARTIFACTS_FOLDER")

dir = os.path.dirname(os.path.abspath(__file__))
artifacts = os.path.join(dir, ARTIFACTS_FOLDER)
folder = os.path.join(artifacts, 'data_refine')

def get_all_files(folder_path):
    files = []
    for root, dirs, filenames in os.walk(folder_path):
        for filename in filenames:
            if filename.endswith('.json'):
                files.append(os.path.join(root, filename))
    return files

def remove_file(file_path):
    try:
        os.remove(file_path)
        print(f"Removed file: {file_path}")
    except Exception as e:
        print(f"Error removing file {file_path}: {e}")

def move_file(source, destination):
    try:
        os.rename(source, destination)
        print(f"Moved file: {source}")
    except Exception as e:
        print(f"Error moving file {source}: {e}")

def download_batch_results(openai_client, batch_info, batch_code):
    """Download batch results from OpenAI."""
    try:
        output_file_id = batch_info.get('output_file_id')
        if not output_file_id:
            print("No output file ID found. Batch may not be completed yet.")
            return None
        
        # Download the result file
        file_response = openai_client.files.content(output_file_id)
        
        # Save to results folder
        result_file_path = os.path.join(folder, 'results', f"batch-{batch_code}-result.jsonl")
        with open(result_file_path, 'wb') as f:
            f.write(file_response.content)
        
        print(f"Downloaded results to: {result_file_path}")
        return result_file_path
        
    except Exception as e:
        print(f"Error downloading batch results: {e}")
        return None

def process_refinement_result(line, collection):
    """Process a single refinement result and update MongoDB."""
    try:
        content = json.loads(line)
        
        if content.get('error'):
            raise Exception(f"Error in content: {content['error']}")
        
        custom_id = content.get('custom_id')
        if not custom_id:
            raise Exception(f"No custom_id found in content")
        
        source_id = custom_id.replace('refine-', '')
        
        # Extract the refinement result
        message = content['response']['body']['choices'][0]['message']
        refinement_result = json.loads(message['content'])
        
        # Update property with refined data
        update_data = {
            'v2_refined_data': refinement_result,
            'status': 'data_refined',
            'updated_at': datetime.now().timestamp()
        }
        
        collection.update_one(
            { 'source_id': source_id },
            { '$set': update_data }
        )
        
        district_matched = refinement_result.get('district_matched', 'N/A')
        print(f"✓ Updated {source_id}: District={district_matched}")
        return True
        
    except Exception as e:
        print(f"✗ Error processing result: {e}")
        # Try to mark error in database
        try:
            if source_id:
                collection.update_one(
                    { 'source_id': source_id },
                    { '$set': { 'v1_refine_data_error': str(e) } }
                )
        except:
            pass
        return False

def main():
    openai_client = AzureOpenAI(
        azure_endpoint = OPENAI_API_ENDPOINT,
        api_key = OPENAI_API_KEY,
        api_version = OPENAI_API_VERSION
    )
    
    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client['prop_main']
    collection = db['props']
    
    files = get_all_files(os.path.join(folder, 'upload_batches'))
    if not files:
        print("No batch files found to process.")
        return
    
    total_batches = len(files)
    total_processed = 0
    total_succeeded = 0
    
    print(f"Found {total_batches} batch(es) to process.\n")
    print("=" * 60)
    
    for file_path in files:
        with open(file_path, 'r') as f:
            batch_info = json.load(f)
        
        batch_code = batch_info['batch_code']
        batch_id = batch_info['batch_id']
        status = batch_info.get('status', 'unknown')
        
        if status != 'completed':
            print(f"Batch {batch_code} status: {status} - Skipping")
            print(f"Run 33_data_refine_batch_track.py to update status")
            print("-" * 60)
            continue
        
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
                    if process_refinement_result(line, collection):
                        total_succeeded += 1
        
        print()
        
        # Archive the batch tracking file
        remove_file(file_path)
    
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total batches processed: {total_batches}")
    print(f"Total results processed: {total_processed}")
    print(f"Successfully updated: {total_succeeded}")
    print(f"Failed: {total_processed - total_succeeded}")
    
    # Show statistics
    refined_count = collection.count_documents({'status': 'data_refined'})
    print(f"\nTotal properties with refined data: {refined_count}")
    
    client.close()

if __name__ == '__main__':
    main()
