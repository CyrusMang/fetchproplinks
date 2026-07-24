import uuid
import time
from utils.summarize_conversation import summarize_conversation
from models.archived_messages import ArchivedMessages


SUMMARIZE_BATCH_SIZE = 5
ARCHIVED_BATCH_SIZE = 50


class Conversation:
    def __init__(self, db, data):
        self.db = db
        self.data = data

    def meaningful_messages(self):
        def is_meaningful(msg):
            if msg['type'] in ('human', 'system'):
                return True
            elif msg['type'] == 'ai':
                return msg.get('content', '') != ''
            return False
        return [msg for msg in self.data.get('messages', []) if is_meaningful(msg)]

    def add_message(self, msg):
        message = {
            **msg,
            'id': str(uuid.uuid4()),
            'index': self.data['counter'],
            'createdAt': int(time.time()),
        }
        result = self.db['conversations-v2'].update_one(
            {'threadId': self.data['threadId'], 'counter': self.data['counter']},
            {
                '$push': {'messages': message},
                '$inc': {'counter': 1},
                '$set': {'status': 'active', 'updatedAt': int(time.time())},
            }
        )
        self.data['counter'] += 1
        if result.modified_count == 0:
            raise Exception('Failed to add message, database is busy')
        self.data.setdefault('messages', []).append(message)
        return message
