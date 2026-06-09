"""
Recovery script: re-scans result JSONL files in results/ to fix prop_photos
stuck at 'batch_created' and props stuck at 'photo_analysing'.
Run this after 23_photo_analysis_batch_update.py has already run.
"""
import os
import json
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
ARTIFACTS_FOLDER = os.getenv("ARTIFACTS_FOLDER")

dir = os.path.dirname(os.path.abspath(__file__))
artifacts = os.path.join(dir, ARTIFACTS_FOLDER)
folder = os.path.join(artifacts, 'photo_analysis')
results_folder = os.path.join(folder, 'results')

def main():
    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client['prop_main']
    collection = db['props']
    photo_collection = db['prop_photos']

    # Find all prop_photos still stuck at batch_created
    stuck_photos = list(photo_collection.find({'status': 'batch_created'}))
    if not stuck_photos:
        print("No stuck photos found.")
        client.close()
        return

    print(f"Found {len(stuck_photos)} photo(s) stuck at 'batch_created'")

    # Build a set of photo_ids we need to resolve
    stuck_photo_ids = {p['photo_id'] for p in stuck_photos}

    # Scan all result files to find entries for stuck photos
    result_files = [
        os.path.join(results_folder, f)
        for f in os.listdir(results_folder)
        if f.endswith('.jsonl')
    ]
    print(f"Scanning {len(result_files)} result file(s)...\n")

    found_in_results = set()

    for result_file in result_files:
        with open(result_file, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    continue

                custom_id = data.get('custom_id', '')
                if not custom_id.startswith('photo-'):
                    continue

                photo_id = custom_id.replace('photo-', '')
                if photo_id not in stuck_photo_ids:
                    continue

                found_in_results.add(photo_id)
                response = data.get('response', {})
                status_code = response.get('status_code', 0)

                if status_code != 200:
                    error_info = response.get('body', {}).get('error', {})
                    print(f"  ✗ photo {photo_id}: API error {status_code} - {error_info.get('message', 'unknown')}")
                    photo_collection.update_one(
                        {'photo_id': photo_id},
                        {'$set': {'status': 'photo_analysis_failed', 'api_error': error_info}}
                    )
                else:
                    body = response.get('body', {})
                    choices = body.get('choices', [])
                    if not choices:
                        print(f"  ✗ photo {photo_id}: empty choices in response")
                        photo_collection.update_one(
                            {'photo_id': photo_id},
                            {'$set': {'status': 'photo_analysis_failed'}}
                        )
                    else:
                        content = choices[0].get('message', {}).get('content', '{}')
                        try:
                            analysis_result = json.loads(content)
                            photo_collection.update_one(
                                {'photo_id': photo_id},
                                {'$set': {**analysis_result, 'status': 'photo_analysed'}}
                            )
                            print(f"  ✓ photo {photo_id}: recovered and updated to photo_analysed")
                        except json.JSONDecodeError as e:
                            print(f"  ✗ photo {photo_id}: JSON parse error - {e}")
                            photo_collection.update_one(
                                {'photo_id': photo_id},
                                {'$set': {'status': 'photo_analysis_failed'}}
                            )

    # Photos not found in any result file = batch was never uploaded or result is missing
    not_found = stuck_photo_ids - found_in_results
    if not_found:
        print(f"\n{len(not_found)} photo(s) not found in any result file (batch may have been lost):")
        for photo_id in not_found:
            photo = photo_collection.find_one({'photo_id': photo_id})
            print(f"  - photo {photo_id} (prop: {photo.get('prop_source_id')}, batch: {photo.get('photo_analysis_batch_code')})")
        print("  -> Marking these as 'photo_analysis_missing' so props can be re-queued.")
        photo_collection.update_many(
            {'photo_id': {'$in': list(not_found)}},
            {'$set': {'status': 'photo_analysis_missing'}}
        )

    # Now fix prop statuses: any prop where ALL its photos are resolved (not batch_created)
    # can be moved out of photo_analysing
    stuck_source_ids = {p['prop_source_id'] for p in stuck_photos}
    updated_props = 0
    for source_id in stuck_source_ids:
        remaining = photo_collection.count_documents({
            'prop_source_id': source_id,
            'status': 'batch_created'
        })
        if remaining == 0:
            has_analysed = photo_collection.count_documents({
                'prop_source_id': source_id,
                'status': 'photo_analysed'
            })
            new_status = 'photo_analysed' if has_analysed > 0 else 'photo_analysis_failed'
            collection.update_one(
                {'source_id': source_id},
                {'$set': {'status': new_status}}
            )
            updated_props += 1

    print(f"\nUpdated {updated_props} prop(s) out of photo_analysing.")
    print("Done.")
    client.close()

if __name__ == '__main__':
    main()
