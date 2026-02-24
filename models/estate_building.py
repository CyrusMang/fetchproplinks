from datetime import datetime
import requests


class EstateBuilding:
    def __init__(self, db, data):
        self.db = db
        self.data = data

    def update(self, data):
        update_data = { '$set': data }
        self.db['estate_buildings'].update_one({ 'id': self.data['id'] }, update_data)
        self.data = {**self.data, **data}

    def search(db, query):
        pass

    def get_by_id(db, id):
        building = db['estate_buildings'].find_one({ 'id': id })
        return EstateBuilding(db, building) if building else None

    def get_by_placeid(db, placeid):
        building = db['estate_buildings'].find_one({ 'place_id': placeid })
        return EstateBuilding(db, building) if building else None

    def create(db, data):
        building_doc = db['estate_buildings'].insert_one(data)
        building = EstateBuilding(db, { '_id': building_doc.inserted_id, **data })
        return building