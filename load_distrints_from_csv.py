import csv
import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")

def load_districts_from_csv(csv_path):
    """
    Load district data from CSV file into MongoDB districts collection.
    
    CSV columns: OBJECTID,ID,CNAME,CNAME_S,ENAME,Shape__Area,Shape__Length
    """
    # Connect to MongoDB
    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client['prop_main']
    collection = db['districts']
    
    # Read CSV file
    with open(csv_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        
        inserted_count = 0
        updated_count = 0
        
        for row in reader:
            # Convert numeric fields
            document = {
                'id': int(row['ID']),
                'CNAME': row['CNAME'],
                'CNAME_S': row['CNAME_S'],
                'ENAME': row['ENAME'],
                'Shape__Area': float(row['Shape__Area']),
                'Shape__Length': float(row['Shape__Length'])
            }
            
            # Update or insert (upsert) based on ID
            result = collection.update_one(
                {'id': document['id']},
                {'$set': document},
                upsert=True
            )
            
            if result.upserted_id:
                inserted_count += 1
                print(f"Inserted: {document['CNAME']} ({document['ENAME']})")
            else:
                updated_count += 1
                print(f"Updated: {document['CNAME']} ({document['ENAME']})")
        
        print(f"\nCompleted!")
        print(f"Total inserted: {inserted_count}")
        print(f"Total updated: {updated_count}")
    
    client.close()

if __name__ == "__main__":
    # Specify your CSV file path
    csv_file_path = input("Enter CSV file path: ").strip()
    
    if not os.path.exists(csv_file_path):
        print(f"Error: File not found - {csv_file_path}")
    else:
        load_districts_from_csv(csv_file_path)
