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

    def download_photos(self):
        link_blobs = []
        links = self.data.get('image_links', [])
        for link in links:
            response = requests.get(link)
            if response.status_code != 200:
                break
            name = link.split('/')[-1].split('?')[0]
            blob = upload('props', name, response.content, response.headers.get('content-type'))
            link_blobs.append(blob)
        thumb_blobs = []
        thumb_links = self.data.get('thumb_links', [])
        for thumb_link in thumb_links:
            response = requests.get(thumb_link)
            if response.status_code != 200:
                break
            name = thumb_link.split('/')[-1].split('?')[0]
            blob = upload('props', name, response.content, response.headers.get('content-type'))
            thumb_blobs.append(blob)
        self.update({'image_links_downloaded': link_blobs, 'thumb_links_downloaded': thumb_blobs})
        self.data['image_links_downloaded'] = link_blobs
        self.data['thumb_links_downloaded'] = thumb_blobs

    def update(self, data):
        update_data = { '$set': data }
        self.db['props'].update_one({ 'source_id': self.data['source_id'] }, update_data)
        self.data = {**self.data, **data}

    def create(db, data):
        props_doc = db['props'].insert_one(data)
        prop = Prop(db, { '_id': props_doc.inserted_id, **data })
        return prop