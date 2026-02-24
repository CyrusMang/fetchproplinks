import os
import json
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")

batch_size = 50

def calculate_photo_quality_score(photo_collection, source_id):
    """Calculate average photo quality from analyzed photos."""
    photos = list(photo_collection.find({
        'prop_source_id': source_id,
        'status': 'photo_analysed',
        'is_photo_of_property': True,
        'is_violating_policy': False
    }))
    
    if not photos:
        return 0, 0
    
    total_quality = sum(p.get('quality_score', 0) for p in photos)
    avg_quality = total_quality / len(photos) if photos else 0
    
    return avg_quality, len(photos)

def calculate_completeness_score(extracted_data):
    """Calculate data completeness score based on filled fields."""
    important_fields = [
        'title', 'description', 'estate_or_building_name', 'district',
        'net_size_sqft', 'number_of_bedrooms', 'number_of_bathrooms',
        'features', 'contacts'
    ]
    
    filled_count = sum(1 for field in important_fields if extracted_data.get(field))
    completeness = (filled_count / len(important_fields)) * 100
    
    return completeness

def calculate_value_score(prop_data, extracted_data):
    """Calculate value score based on price, size, and location."""
    score = 50  # Base score
    
    # Price availability
    if extracted_data.get('rent_price') or extracted_data.get('sell_price'):
        score += 10
    
    # Size information
    if extracted_data.get('net_size_sqft') or extracted_data.get('gross_size_sqft'):
        score += 10
        
        # Price per sqft analysis (if available)
        size = extracted_data.get('net_size_sqft') or extracted_data.get('gross_size_sqft')
        price = extracted_data.get('rent_price') or extracted_data.get('sell_price')
        
        if size and price and size > 0:
            price_per_sqft = price / size
            # Reasonable price range check (HK market)
            if prop_data.get('post_type') == 'rent':
                # Rent: $20-80/sqft is reasonable
                if 20 <= price_per_sqft <= 80:
                    score += 10
                elif 10 <= price_per_sqft <= 100:
                    score += 5
            else:
                # Sale: $8000-20000/sqft is reasonable
                if 8000 <= price_per_sqft <= 20000:
                    score += 10
                elif 5000 <= price_per_sqft <= 30000:
                    score += 5
    
    # Location information
    if extracted_data.get('district'):
        score += 10
    
    # Features
    features = extracted_data.get('features', [])
    if len(features) > 0:
        score += min(len(features) * 2, 10)
    
    return min(score, 100)

def calculate_presentation_score(extracted_data, photo_quality, photo_count):
    """Calculate presentation score based on description, photos, and details."""
    score = 0
    
    # Description quality
    description = extracted_data.get('description', '')
    if description:
        desc_length = len(description)
        if desc_length > 500:
            score += 25
        elif desc_length > 200:
            score += 20
        elif desc_length > 100:
            score += 15
        else:
            score += 10
    
    # Photo quality and quantity
    if photo_count > 0:
        photo_score = min((photo_quality / 100) * 40, 40)
        score += photo_score
        
        # Bonus for having many photos
        if photo_count >= 10:
            score += 10
        elif photo_count >= 5:
            score += 5
    
    # Contact information
    contacts = extracted_data.get('contacts', [])
    if len(contacts) > 0:
        score += 10
    
    # Additional details
    if extracted_data.get('nearby_places') or extracted_data.get('transportation_options'):
        score += 10
    
    # Summary quality
    if extracted_data.get('summary'):
        score += 5
    
    return min(score, 100)

def evaluate_property(prop, photo_collection):
    """Evaluate a single property and calculate overall score."""
    
    extracted_data = prop.get('v1_extracted_data', {})
    source_id = prop.get('source_id')
    
    # Calculate individual scores
    photo_quality, photo_count = calculate_photo_quality_score(photo_collection, source_id)
    completeness = calculate_completeness_score(extracted_data)
    value_score = calculate_value_score(prop, extracted_data)
    presentation_score = calculate_presentation_score(extracted_data, photo_quality, photo_count)
    
    # Calculate weighted overall score
    overall_score = (
        completeness * 0.20 +      # 20% weight on data completeness
        value_score * 0.30 +        # 30% weight on value/price info
        presentation_score * 0.30 + # 30% weight on presentation
        photo_quality * 0.20        # 20% weight on photo quality
    )
    
    # Determine grade
    if overall_score >= 80:
        grade = "A"
    elif overall_score >= 70:
        grade = "B"
    elif overall_score >= 60:
        grade = "C"
    elif overall_score >= 50:
        grade = "D"
    else:
        grade = "F"
    
    evaluation = {
        'overall_score': round(overall_score, 2),
        'grade': grade,
        'completeness_score': round(completeness, 2),
        'value_score': round(value_score, 2),
        'presentation_score': round(presentation_score, 2),
        'photo_quality_score': round(photo_quality, 2),
        'photo_count': photo_count,
        'evaluated_at': datetime.now().timestamp()
    }
    
    return evaluation

def main():
    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client['prop_main']
    collection = db['props']
    photo_collection = db['prop_photos']
    
    # Find properties with status "photo_analysed"
    filter_query = {
        'status': 'photo_analysed',
        'v1_extracted_data': { '$exists': True }
    }
    
    total_count = collection.count_documents(filter_query)
    
    if total_count == 0:
        print("No properties found with status 'photo_analysed'.")
        client.close()
        return
    
    print(f"Found {total_count} properties to evaluate.")
    print("=" * 60)
    
    skip = 0
    processed = 0
    
    while skip < total_count:
        properties = collection.find(filter_query).skip(skip).limit(batch_size)
        
        for prop in properties:
            source_id = prop.get('source_id')
            
            try:
                # Evaluate the property
                evaluation = evaluate_property(prop, photo_collection)
                
                # Update property with evaluation and change status
                collection.update_one(
                    { 'source_id': source_id },
                    { 
                        '$set': { 
                            'evaluation': evaluation,
                            'status': 'evaluated',
                            'updated_at': datetime.now().timestamp()
                        }
                    }
                )
                
                processed += 1
                print(f"✓ [{processed}/{total_count}] {source_id}: "
                      f"Score={evaluation['overall_score']:.1f} "
                      f"Grade={evaluation['grade']} "
                      f"Photos={evaluation['photo_count']}")
                
            except Exception as e:
                print(f"✗ Error evaluating {source_id}: {e}")
                # Mark as error but continue
                collection.update_one(
                    { 'source_id': source_id },
                    { '$set': { 
                        'evaluation_error': str(e),
                        'updated_at': datetime.now().timestamp()
                    }}
                )
        
        skip += batch_size
    
    print("=" * 60)
    print(f"Evaluation completed. Processed {processed} properties.")
    
    # Print summary statistics
    print("\nGrade Distribution:")
    for grade in ['A', 'B', 'C', 'D', 'F']:
        count = collection.count_documents({'evaluation.grade': grade})
        if count > 0:
            print(f"  Grade {grade}: {count} properties")
    
    client.close()

if __name__ == '__main__':
    main()
