import re

class PropMariaDB:
    def __init__(self, db, data):
        self.db = db
        self.data = data

    @staticmethod
    def get_by_id(db, id):
        cursor = db.cursor(dictionary=True, buffered=True)
        cursor.execute("SELECT * FROM processed_properties WHERE source_id = %s", (id,))
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
            'price_from',
            'net_size',
            'price_sqft',
        ]
        v1_extracted_data = data.get('v1_extracted_data', {})
        post_type = 1 if data.get('post_type') == 'sell' else 2
        price = int(v1_extracted_data.get('rent_price', 0)) if post_type == 2 else int(v1_extracted_data.get('sell_price', 0))
        size = int(v1_extracted_data.get('net_size', 0))
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

        values.append(self.data['source_id'])
        
        cursor = self.db.cursor()
        query = f"UPDATE processed_properties SET {', '.join(set_clause)} WHERE source_id = %s"
        cursor.execute(query, values)
        self.db.commit()
        cursor.close()
        
        self.data = {**self.data, **data}

    @staticmethod
    def create(db, data):
        cursor = db.cursor(dictionary=True, buffered=True)

        channel_id = 1
        if data.get('source_channel') == 'squarefoot':
            channel_id = 2
        elif data.get('source_channel') == 'spacious':
            channel_id = 3

        v1_extracted_data = data.get('v1_extracted_data', {})
        district_keywords = v1_extracted_data.get('district', '').split(' ')
        cursor.execute("SELECT * FROM district_references WHERE channel_id = %s AND district LIKE %s OR district LIKE %s", (channel_id, f"%{district_keywords[0]}%", f"%{district_keywords[-1]}%"))
        #cursor.execute("SELECT * FROM district_references WHERE channel_id = %s AND district LIKE %s", (channel_id, f"%{v1_extracted_data.get('district')}%"))
        district = cursor.fetchone()

        if not district:
            raise Exception(f"District not found {data.get('source_id')} and district {v1_extracted_data.get('district')}")

        # Prepare columns and values
        columns = [
            'source_id', 
            'source_url', 
            'property_status',
            'description_chi', 
            'property_type', 
            'tags_chi', 
            'listing_type',
            'building_chi',
            'price_from',
            'net_size',
            'gross_size',
            'bedroom',
            'bathroom',
            'price_sqft',
            'propx_district_id',
        ]
        post_type = 1 if data.get('post_type') == 'sell' else 2
        price = int(v1_extracted_data.get('rent_price', 0)) if post_type == 2 else int(v1_extracted_data.get('sell_price', 0))
        size = int(v1_extracted_data.get('net_size', 0))
        values = [
            data.get('source_id'),
            data.get('source_url'),
            'Active',
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
            district['propx_district_id'] if district else None,
        ]

        contact_ids = []
        for contact in data.get('contacts', []):
            cursor.execute("SELECT * FROM contacts WHERE contact_name_chi = %s", (contact.get('name'),))
            c = cursor.fetchone()
            contact_id = None
            if c:
                contact_id = contact['contact_id']
            else:
                cursor.execute("INSERT INTO contacts (contact_name_chi, agent_lic) VALUES (%s, %s)", (contact.get('name'), contact.get('license_no')))
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
            contact_ids.append(contact_id)
        
        placeholders = []
        for _ in columns:
            placeholders.append('%s')
        
        query = f"INSERT INTO processed_properties ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        cursor.execute(query, values)
        prop_id = cursor.lastrowid

        for contact_id in contact_ids:
            cursor.execute("INSERT INTO property_contacts (property_id, contact_id) VALUES (%s, %s)", (prop_id, contact_id))
        db.commit()
        
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