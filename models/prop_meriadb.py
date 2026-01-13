import json
import re

class PropMariaDB:
    def __init__(self, db, data):
        self.db = db
        self.data = data

    @staticmethod
    def get_by_id(db, id):
        cursor = db.cursor(dictionary=True, buffered=True)
        cursor.execute("SELECT * FROM properties WHERE channel_property_id = %s", (id,))
        result = cursor.fetchone()
        cursor.close()
        return PropMariaDB(db, result) if result else None

    def update(self, data):
        # Build SET clause dynamically
        columns = [
            'description_chi', 
            'tags_chi', 
            'listing_type',
            'building_chi',
            'price',
            'net_size',
            'price_sqft',
        ]
        v1_extracted_data = data.get('v1_extracted_data', {})
        post_type = 1 if data.get('post_type') == 'sell' else 2
        price = v1_extracted_data.get('rent_price', 0) if post_type == 2 else v1_extracted_data.get('sell_price', 0)
        size = v1_extracted_data.get('net_size', 0)
        values = [
            data.get('description_chi'),
            data.get('tags_chi'),
            post_type,
            v1_extracted_data.get('estate_or_building_name', ''),
            price,
            size,
            (price / size) if size and price else None,
        ]
        set_clause = []
        for key in columns:
            set_clause.append(f"{key} = %s")
        
        values.append(self.data['channel_property_id'])
        
        cursor = self.db.cursor()
        query = f"UPDATE properties SET {', '.join(set_clause)} WHERE channel_property_id = %s"
        cursor.execute(query, values)
        self.db.commit()
        cursor.close()
        
        self.data = {**self.data, **data}

    @staticmethod
    def create(db, data):
        cursor = db.cursor()

        channel_id = 1
        if data.get('source_channel') == 'squarefoot':
            channel_id = 2
        elif data.get('source_channel') == 'spacious':
            channel_id = 3

        cursor.execute("SELECT * FROM district_references WHERE channel_id = %s AND district LIKE %s", (channel_id, f"%{data.get('district')}%"))
        district = cursor.fetchone()

        # Prepare columns and values
        columns = [
            'channel_id',
            'channel_property_id', 
            'description_chi', 
            'property_type', 
            'tags_chi', 
            'listing_type',
            'building_chi',
            'price',
            'net_size',
            'gross_size',
            'bedroom',
            'bathroom',
            'price_sqft',
            'propx_district_id',
            'property_json',
        ]
        v1_extracted_data = data.get('v1_extracted_data', {})
        post_type = 1 if data.get('post_type') == 'sell' else 2
        price = v1_extracted_data.get('rent_price', 0) if post_type == 2 else v1_extracted_data.get('sell_price', 0)
        size = v1_extracted_data.get('net_size', 0)
        values = [
            channel_id,
            data.get('source_id'),
            data.get('description_chi'),
            data.get('type'),
            data.get('tags_chi'),
            post_type,
            v1_extracted_data.get('estate_or_building_name', ''),
            price,
            size,
            v1_extracted_data.get('gross_size', 0),
            v1_extracted_data.get('number_of_bedrooms', 0),
            v1_extracted_data.get('number_of_bathrooms', 0),
            (price / size) if size and price else None,
            district['district_id'] if district else None,
            json.dumps({ 'url': data.get('source_url') })
        ]

        for contact in data.get('contacts', []):
            cursor.execute("SELECT * FROM contacts WHERE contact_name_chi = %s", (contact.get('name'),))
            c = cursor.fetchone()
            contact_id = None
            if c:
                contact_id = contact['id']
            else:
                cursor.execute("INSERT INTO contacts (contact_name, agent_lic) VALUES (%s, %s)", (contact.get('phone'), contact.get('license_no')))
                contact_id = cursor.lastrowid
            for phone in contact.get('phones', []):
                if not phone:
                    continue
                cursor.execute("INSERT INTO phones (phone) VALUES (%s)", (phone,))
                phone_id = cursor.lastrowid
                cursor.execute("INSERT INTO contact_phones (contact_id, phone_id) VALUES (%s, %s)", (contact_id, phone_id))
            for wtsapp in contact.get('wtsapps', []):
                if not wtsapp:
                    continue
                p = r'(\d{3}\d{7})'
                match = re.search(p, wtsapp)
                if not match:
                    continue
                cursor.execute("INSERT INTO phones (phone, is_wtsapp) VALUES (%s, %s)", (match.group(0), True))
                phone_id = cursor.lastrowid
                cursor.execute("INSERT INTO contact_phones (contact_id, phone_id) VALUES (%s, %s)", (contact_id, phone_id))
        
        placeholders = []
        for _ in columns:
            placeholders.append('%s')
        
        query = f"INSERT INTO properties ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        cursor.execute(query, values)
        db.commit()
        
        prop_id = cursor.lastrowid
        cursor.close()
        
        prop = PropMariaDB(db, {'id': prop_id, **data})
        return prop

    @staticmethod
    def create_or_update(db, id, data):
        prop = PropMariaDB.get_by_id(db, id)
        if prop:
            prop.update(data)
            print(f"Updated prop {id}")
        else:
            prop = PropMariaDB.create(db, data)
            print(f"Created prop {id}")
        return prop