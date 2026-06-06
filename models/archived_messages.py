import time


class ArchivedMessages:
    def __init__(self, data):
        self.data = data

    @staticmethod
    def find_by_range(db, code: str, from_index: int | None, to_index: int | None):
        query = {'code': code}
        if from_index is not None:
            query['fromIndex'] = {'$gte': from_index}
        if to_index is not None:
            query['toIndex'] = {'$lte': to_index}

        result = list(db['archived-messages'].find(query))
        messages = []
        for k, batch in enumerate(result):
            if k == 0 and from_index is not None:
                for message in batch['messages']:
                    if message['index'] >= from_index:
                        messages.append(message)
            elif k == len(result) - 1 and to_index is not None:
                for message in batch['messages']:
                    if message['index'] <= to_index:
                        messages.append(message)
            else:
                messages.extend(batch['messages'])
        return messages

    @staticmethod
    def create(db, code: str, messages: list, session=None):
        data = {
            'code': code,
            'messages': messages,
            'fromIndex': messages[0]['index'],
            'toIndex': messages[-1]['index'],
            'createdAt': int(time.time()),
        }
        kwargs = {'session': session} if session is not None else {}
        doc = db['archived-messages'].insert_one(data, **kwargs)
        return ArchivedMessages({'_id': doc.inserted_id, **data})
