import json
import os
from dotenv import load_dotenv
from pymongo import MongoClient, GEOSPHERE

load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
DB_NAME = "prop_main"
COLLECTION_NAME = "sub_districts"

NAME_FILE_PATH = "GeoName_PlaceName_20251106_gdb_PLACE_NAME_converted.geojson"
GEO_FILE_PATH = "GeoName_PlaceName_20251106_gdb_GEO_PLACE_NAME_converted.geojson"

def main():
    # 1. 檢查檔案是否存在
    if not os.path.exists(NAME_FILE_PATH) or not os.path.exists(GEO_FILE_PATH):
        print("❌ 錯誤：找不到 JSON 檔案，請檢查檔名和路徑是否正確。")
        return

    print("📖 正在讀取 JSON 數據...")
    with open(NAME_FILE_PATH, 'r', encoding='utf-8') as f:
        name_data = json.load(f)
    with open(GEO_FILE_PATH, 'r', encoding='utf-8') as f:
        geo_data = json.load(f)

    # 2. 首先建立 GEO_NAME_ID 的快速 Mapping Map
    print("🧩 正在解析地理坐標...")
    geo_map = {}
    for feature in geo_data.get('features', []):
        props = feature.get('properties', {})
        geometry = feature.get('geometry')
        
        # 關鍵篩選：只拿 'Settlement' (有人住的社區/城鎮地名)，過濾掉山頭或海面
        if geometry and props.get('PLACE_CLASS') == 'Settlement':
            geo_id = props.get('GEO_NAME_ID')
            coordinates = geometry.get('coordinates') # 格式為 [Lng, Lat]
            
            geo_map[geo_id] = {
                "district": props.get('DISTRICT'),
                "lng": coordinates[0],
                "lat": coordinates[1]
            }

    # 3. 拼合中英文地名，組成準備入庫的 Documents
    print("🏗️ 正在拼合社區數據...")
    documents_to_insert = []
    
    for feature in name_data.get('features', []):
        props = feature.get('properties', {})
        geo_id = props.get('GEO_NAME_ID')
        
        # 如果能在剛才篩選的地理 Map 裡面搵到對應
        if geo_id in geo_map:
            geo_info = geo_map[geo_id]
            
            # 建立符合 MongoDB GeoJSON 規範的結構
            doc = {
                "subDistrictName": props.get('NAME_TC'),   # 中文細分區名 (如: 旺角)
                "subDistrictNameEN": props.get('NAME_EN'), # 英文細分區名 (如: Mong Kok)
                "districtCode": geo_info['district'],       # 18區代碼 (如: YTM)
                "location": {
                    "type": "Point",
                    "coordinates": [geo_info['lng'], geo_info['lat']] # [經度, 緯度] ⚠️ 必須是這個順序
                }
            }
            documents_to_insert.append(doc)

    print(f"📊 拼合完成！共篩選出 {len(documents_to_insert)} 個有效社區中心點。")

    if not documents_to_insert:
        print("⚠️ 沒有找到符合條件的數據，取消入庫。")
        return

    # 4. 連接 MongoDB 並寫入數據
    print("🔌 正在連接 MongoDB...")
    try:
        client = MongoClient(MONGODB_CONNECTION_STRING)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        # 清空舊數據（防止重複執行時數據疊加）
        print("🗑️ 清空現有 Collection 數據...")
        collection.delete_many({})

        # 批量寫入
        print("💾 正在寫入數據到 MongoDB...")
        result = collection.insert_many(documents_to_insert)
        print(f"✅ 成功寫入 {len(result.inserted_ids)} 條數據！")

        # 🔥 建立 2dsphere 地理索引（Next.js 做 $near 查詢的關鍵）
        print("⚡ 正在建立 2dsphere 地理空間索引...")
        collection.create_index([("location", GEOSPHERE)])
        print("🚀 索引建立成功！數據庫準備就緒。")

    except Exception as e:
        print(f"❌ 數據庫操作失敗: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    main()