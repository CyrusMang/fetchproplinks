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

    def update(self, data):
        update_data = { '$set': data }
        self.db['props'].update_one({ 'source_id': self.data['source_id'] }, update_data)
        self.data = {**self.data, **data}

    def create(db, data):
        props_doc = db['props'].insert_one(data)
        prop = Prop(db, { '_id': props_doc.inserted_id, **data })
        return prop