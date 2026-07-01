import uuid
import time
from utils.summarize_conversation import summarize_conversation
from models.archived_messages import ArchivedMessages


CURATED_PREFIX = 'Property search curated results JSON:'
ROLLING_SUMMARY_TYPE = 'rolling_summary'


def _message_type(msg: dict) -> str:
    return str(msg.get('type', '')).strip().lower()


def _message_content_to_text(msg: dict) -> str:
    content = msg.get('data', {}).get('content', msg.get('content', ''))
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for part in content:
            if isinstance(part, str):
                parts.append(part)
            elif isinstance(part, dict):
                parts.append(str(part.get('text', '')))
        return '\n'.join(parts).strip()
    return ''


def _parse_rolling_summary_payload(text: str):
    import json

    try:
        parsed = json.loads(text)
        if parsed.get('type') != ROLLING_SUMMARY_TYPE:
            return None
        summary = parsed.get('summary')
        if not isinstance(summary, str) or summary.strip() == '':
            return None
        return {'type': ROLLING_SUMMARY_TYPE, 'summary': summary.strip()}
    except Exception:
        return None


def _build_rolling_summary_message(summary: str) -> dict:
    import json

    return {
        'type': 'system',
        'content': json.dumps({
            'type': ROLLING_SUMMARY_TYPE,
            'summary': summary,
        }),
    }


def _compact_persisted_messages(messages: list[dict]) -> list[dict]:
    latest_curated_index = -1
    for index in range(len(messages) - 1, -1, -1):
        message = messages[index]
        if _message_type(message) != 'system':
            continue
        if _message_content_to_text(message).startswith(CURATED_PREFIX):
            latest_curated_index = index
            break

    compacted = []
    for index, message in enumerate(messages):
        if _message_type(message) != 'system':
            compacted.append(message)
            continue
        text = _message_content_to_text(message)
        if not text.startswith(CURATED_PREFIX) or index == latest_curated_index:
            compacted.append(message)
    return compacted


def _remove_previous_summary_messages(messages: list[dict]) -> list[dict]:
    sanitized = []
    for message in messages:
        if _message_type(message) != 'system':
            sanitized.append(message)
            continue
        text = _message_content_to_text(message)
        if _parse_rolling_summary_payload(text) is None:
            sanitized.append(message)
    return sanitized


def _get_latest_summary_text(messages: list[dict]):
    for index in range(len(messages) - 1, -1, -1):
        message = messages[index]
        if _message_type(message) != 'system':
            continue
        payload = _parse_rolling_summary_payload(_message_content_to_text(message))
        if payload:
            return payload['summary']
    return None


def _get_latest_curated_index(messages: list[dict]) -> int:
    for index in range(len(messages) - 1, -1, -1):
        message = messages[index]
        if _message_type(message) != 'system':
            continue
        if _message_content_to_text(message).startswith(CURATED_PREFIX):
            return index
    return -1

class LanggraphThread:
    def __init__(self, db, data):
        self.db = db
        self.data = data

    def add_message(self, msg):
        now = int(time.time())
        update = {
            '$push': {'messages': msg},
            '$set': {
                'updatedAt': now,
            },
        }

        result = self.db['langgraph_v2_threads'].update_one({'_id': self.data['_id']}, update)
        if result.modified_count == 0:
            raise Exception('Failed to add message, database is busy')

        self.data.setdefault('messages', []).append(msg)
        self.data['updatedAt'] = now
        return msg

    def conversation_summary(self):
        messages = self.data.get('messages', [])
        if not messages:
            return

        compacted = _compact_persisted_messages(messages)
        previous_summary = _get_latest_summary_text(compacted)
        sanitized = _remove_previous_summary_messages(compacted)

        total_chars = sum(len(_message_content_to_text(message)) for message in sanitized)
        should_summarize = len(sanitized) > 12 or total_chars > 2800
        if not should_summarize:
            return

        keep_recent_count = 6
        if len(sanitized) <= keep_recent_count + 2:
            return

        recent_start_index = max(0, len(sanitized) - keep_recent_count)
        older = sanitized[:recent_start_index]
        recent = sanitized[recent_start_index:]

        latest_curated_index = _get_latest_curated_index(sanitized)
        latest_curated_message = sanitized[latest_curated_index] if latest_curated_index >= 0 else None

        older_dialogue_messages = []
        for message in older:
            msg_type = _message_type(message)
            if msg_type not in ('human', 'ai'):
                continue
            text = _message_content_to_text(message).strip()
            if text == '':
                continue
            role = 'user' if msg_type == 'human' else 'assistant'
            older_dialogue_messages.append({'role': role, 'content': text})

        if not older_dialogue_messages:
            return

        try:
            summary_text = summarize_conversation(older_dialogue_messages, previous_summary)
        except Exception:
            return

        if not isinstance(summary_text, str) or summary_text.strip() == '':
            return

        summary_text = summary_text.strip()
        should_pin_curated = latest_curated_message is not None and latest_curated_index < recent_start_index
        if should_pin_curated:
            next_messages = [_build_rolling_summary_message(summary_text), latest_curated_message, *recent]
        else:
            next_messages = [_build_rolling_summary_message(summary_text), *recent]

        now = int(time.time())
        self.db['langgraph_v2_threads'].update_one(
            {'_id': self.data['_id']},
            {
                '$set': {
                    'messages': next_messages,
                    'updatedAt': now,
                }
            }
        )
        self.data['messages'] = next_messages
        self.data['updatedAt'] = now

    @staticmethod
    def find_by_user_id(db, user_id):
        result = db['langgraph_v2_threads'].find({'userId': user_id})
        return [LanggraphThread(db, doc) for doc in result]

    @staticmethod
    def get_by_user_id(db, user_id):
        result = db['langgraph_v2_threads'].find_one({'userId': user_id})
        if not result:
            return None
        return LanggraphThread(db, result)
