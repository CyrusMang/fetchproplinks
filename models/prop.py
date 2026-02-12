from datetime import datetime
import requests

from utils.azure_blob import upload


class Prop:
    def __init__(self, db, data):
        self.db = db
        self.data = data

    def get_by_id(db, id):
        prop = db['props'].find_one({ 'source_id': id })
        return Prop(db, prop) if prop else None
    
    def batch(db, skip, limit):
        props_cursor = db['props'].find({
            "type": "apartment",
            "v1_extracted_data": { '$exists': True },
        }).skip(skip).limit(limit)
        return [Prop(db, prop) for prop in props_cursor]

    def archive(self):
        self.db['prop_photos'].update_many(
            { 'prop_source_id': self.data['source_id'] },
            { '$set': { 'status': 'archived' } }
        )
        self.update({ 
            'status': 'archived',
            "updated_at": datetime.now().timestamp(),
        })

    # def download_photos(self):
    #     photo_with_blobs = []
    #     for photo in self.data.get('analysed_photos', []):
    #         response = requests.get(photo['origin_url'])
    #         if response.status_code != 200:
    #             break
    #         name = photo['origin_url'].split('/')[-1].split('?')[0]
    #         blob = upload('props', name, response.content, response.headers.get('content-type'))
    #         photo_with_blobs.append({
    #             **photo,
    #             "blob_url": blob,
    #         })
    #     self.update({'analysed_photos': photo_with_blobs, 'status': 'active'})

    def update(self, data):
        update_data = { '$set': data }
        self.db['props'].update_one({ 'source_id': self.data['source_id'] }, update_data)
        self.data = {**self.data, **data}

    def create(db, data):
        props_doc = db['props'].insert_one(data)
        prop = Prop(db, { '_id': props_doc.inserted_id, **data })
        return prop