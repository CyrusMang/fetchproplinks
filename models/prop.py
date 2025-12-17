class Prop:
    def __init__(self, db, data):
        self.db = db
        self.data = data

    def get_by_id(db, id):
        prop = db['props'].find_one({ 'cid': id })
        return Prop(db, prop) if prop else None

    def update(self, data):
        update_data = { '$set': data }
        self.db['props'].update_one({ 'cid': self.data['cid'] }, update_data)
        self.data = {**self.data, **data}

    def create(db, data):
        props_doc = db['props'].insert_one(data)
        prop = Prop(db, { '_id': props_doc.inserted_id, **data })
        return prop