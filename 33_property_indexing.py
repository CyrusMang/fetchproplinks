import json
import os
from datetime import datetime

from dotenv import load_dotenv
from openai import AzureOpenAI
from pymongo import MongoClient

load_dotenv()

batch_size = 20

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_ENDPOINT = os.getenv("OPENAI_API_ENDPOINT")
OPENAI_API_VERSION = os.getenv("OPENAI_API_VERSION")

openai_client = AzureOpenAI(
    azure_endpoint=OPENAI_API_ENDPOINT,
    api_key=OPENAI_API_KEY,
    api_version=OPENAI_API_VERSION,
)
embedding_model = "text-embedding-3-large"


def get_embedding(text):
    embedding = openai_client.embeddings.create(
        input=[text], model=embedding_model
    ).data[0].embedding
    return embedding


def extract_indexing_text(prop):
    """Extract comprehensive text from property summary for embedding."""
    summary = prop.get("v1_summary_data", {})
    extracted = prop.get("v1_extracted_data", {})

    parts = []

    # Headlines and summaries (highest weight)
    if summary.get("headline"):
        parts.append(summary["headline"])
    if summary.get("executive_summary"):
        parts.append(summary["executive_summary"])

    # Key highlights and concerns
    highlights = summary.get("key_highlights", [])
    if highlights:
        parts.append("Highlights: " + " ".join(highlights))

    concerns = summary.get("possible_concerns", [])
    if concerns:
        parts.append("Considerations: " + " ".join(concerns))

    # Space and layout
    space_info = summary.get("layout_and_space", {})
    if space_info.get("space_comment"):
        parts.append(space_info["space_comment"])

    # Location and transport
    loc_info = summary.get("location_and_transport", {})
    if loc_info.get("location_comment"):
        parts.append(loc_info["location_comment"])
    district = extracted.get("district")
    if district:
        parts.append(f"District: {district}")
    nearby = extracted.get("nearby_places", [])
    if nearby:
        parts.append("Nearby: " + " ".join(nearby[:5]))
    transport = extracted.get("transportation_options", [])
    if transport:
        parts.append("Transport: " + " ".join(transport[:5]))

    # Price analysis
    price_info = summary.get("price_analysis", {})
    if price_info.get("value_comment"):
        parts.append(price_info["value_comment"])
    rent = extracted.get("rent_price")
    sell = extracted.get("sell_price")
    if rent:
        parts.append(f"Rent: {rent}")
    if sell:
        parts.append(f"Sell: {sell}")

    # Photo insights
    photo_info = summary.get("photo_insights", {})
    if photo_info.get("overall_condition"):
        parts.append(f"Condition: {photo_info['overall_condition']}")
    if photo_info.get("cleanliness_comment"):
        parts.append(photo_info["cleanliness_comment"])
    if photo_info.get("brightness_comment"):
        parts.append(photo_info["brightness_comment"])

    # Features and recommendations
    features = extracted.get("features", [])
    if features:
        parts.append("Features: " + " ".join(features[:10]))

    recommended = summary.get("recommended_for", [])
    if recommended:
        parts.append("Suited for: " + " ".join(recommended))

    # Building attributes
    bedrooms = extracted.get("number_of_bedrooms")
    bathrooms = extracted.get("number_of_bathrooms")
    if bedrooms:
        parts.append(f"{bedrooms} bedroom")
    if bathrooms:
        parts.append(f"{bathrooms} bathroom")
    building_age = extracted.get("building_age")
    if building_age:
        parts.append(f"Building age: {building_age} years")
    floor = extracted.get("floor")
    if floor:
        parts.append(f"Floor: {floor}")

    # Estate/building name
    estate = extracted.get("estate_or_building_name")
    if estate:
        parts.append(f"Estate: {estate}")

    combined_text = " ".join(parts)
    return combined_text.strip()


def main():
    if not MONGODB_CONNECTION_STRING:
        print("Missing MONGODB_CONNECTION_STRING in environment.")
        return
    if not OPENAI_API_KEY or not OPENAI_API_ENDPOINT or not OPENAI_API_VERSION:
        print("Missing OpenAI Azure configuration in environment.")
        return

    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client["prop_main"]
    prop_collection = db["props"]

    # Find properties with ready summaries that haven't been indexed yet
    props = prop_collection.find(
        {
            "v1_summary_data": {"$exists": True},
            "summary_status": "summary_ready",
            "property_embedding": {"$exists": False},
        }
    ).sort("updated_at", -1).limit(batch_size)

    processed = 0
    skipped = 0

    for prop in props:
        source_id = prop.get("source_id")

        try:
            # Extract text for indexing
            index_text = extract_indexing_text(prop)

            if not index_text or len(index_text) < 20:
                print(f"⊘ Skipped {source_id}: insufficient text for indexing")
                skipped += 1
                continue

            # Generate embedding
            embedding = get_embedding(index_text)

            # Update property with embedding and metadata
            prop_collection.update_one(
                {"source_id": source_id},
                {
                    "$set": {
                        "property_embedding": embedding,
                        "embedding_text_length": len(index_text),
                        "indexing_status": "indexed",
                        "indexed_at": datetime.now().timestamp(),
                    }
                },
            )
            processed += 1
            print(
                f"✓ Indexed {source_id} ({len(index_text)} chars, embedding_dim={len(embedding)})"
            )

        except Exception as e:
            skipped += 1
            prop_collection.update_one(
                {"source_id": source_id},
                {
                    "$set": {
                        "indexing_status": "indexing_failed",
                        "indexing_error": str(e),
                    }
                },
            )
            print(f"✗ Error indexing {source_id}: {e}")

    print("=" * 50)
    print("PROPERTY INDEXING SUMMARY")
    print("=" * 50)
    print(f"Successfully indexed: {processed}")
    print(f"Skipped/Failed: {skipped}")
    print(f"Total processed: {processed + skipped}")

    client.close()


if __name__ == "__main__":
    main()