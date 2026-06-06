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

    def normalize(self):
        return {
            'code': self.data['code'],
            'summary': self.data.get('summary'),
            'counter': self.data['counter'],
            'preArchiveMessages': self.data.get('preArchiveMessages', []),
            'messages': self.data.get('messages', []),
            'createdAt': self.data['createdAt'],
        }

    def conversation_full(self):
        msgs = []
        for message in reversed(self.data.get('messages', [])):
            if message['type'] == 'human':
                msgs.insert(0, {
                    'type': message['type'],
                    'content': [{'type': 'text', 'text': str(message['content'])}],
                })
            elif message['type'] != 'ai_error':
                msgs.insert(0, message)
        if self.data.get('summary'):
            msgs.insert(0, {
                'type': 'system',
                'content': f"Summary of conversation earlier: {self.data['summary']}",
            })
        return msgs

    def conversation_brief(self):
        cmessages = self.meaningful_messages()
        msgs = []
        if self.data.get('summary'):
            msgs.append({
                'type': 'system',
                'content': f"Summary of conversation earlier: {self.data['summary']}",
            })
        for message in cmessages:
            msgs.append({
                'type': message['type'],
                'content': str(message['content']),
            })
        return msgs

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
        result = self.db['conversations'].update_one(
            {'code': self.data['code'], 'counter': self.data['counter']},
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

    def conversation_summary(self):
        cmessages = self.meaningful_messages()
        if len(cmessages) < (SUMMARIZE_BATCH_SIZE + 5):
            return
        diff = cmessages[SUMMARIZE_BATCH_SIZE]['index'] - self.data['messages'][0]['index']
        summarize_cmessages = cmessages[:SUMMARIZE_BATCH_SIZE]
        messages = self.data['messages'][:diff]
        summary = summarize_conversation(summarize_cmessages, self.data.get('summary'))
        self.db['conversations'].update_one(
            {'code': self.data['code']},
            [{
                '$set': {
                    'summary': summary,
                    'messages': {'$slice': ['$messages', {'$subtract': [diff, {'$size': '$messages'}]}]},
                    'preArchiveMessages': {'$concatArrays': ['$preArchiveMessages', messages]},
                    'updatedAt': int(time.time()),
                }
            }]
        )

    def archive_messages(self):
        if len(self.data.get('preArchiveMessages', [])) < ARCHIVED_BATCH_SIZE:
            return

        def callback(session):
            ArchivedMessages.create(self.db, self.data['code'], self.data['preArchiveMessages'], session=session)
            self.db['conversations'].update_one(
                {'code': self.data['code']},
                {
                    '$set': {
                        'preArchiveMessages': [],
                        'updatedAt': int(time.time()),
                    }
                },
                session=session,
            )

        with self.db.client.start_session() as session:
            session.with_transaction(callback)

    @staticmethod
    def find_by_user_id(db, user_id):
        result = db['conversations'].find({'userId': user_id})
        return [Conversation(db, doc) for doc in result]

    @staticmethod
    def get_by_user_id(db, user_id):
        result = db['conversations'].find_one({'userId': user_id})
        if not result:
            return None
        return Conversation(db, result)

    @staticmethod
    def get_by_code(db, code):
        result = db['conversations'].find_one({'code': code})
        if not result:
            return None
        return Conversation(db, result)
