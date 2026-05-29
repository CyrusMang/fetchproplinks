import os
import json
from datetime import datetime, timedelta
import requests
from dotenv import load_dotenv

load_dotenv()

CHATWOOT_BASE_URL = os.getenv("CHATWOOT_BASE_URL", "https://chatwoot.snailbutler.com")
CHATWOOT_ACCOUNT_ID = os.getenv("CHATWOOT_ACCOUNT_ID")
CHATWOOT_API_TOKEN = os.getenv("CHATWOOT_API_TOKEN")
CHATWOOT_INBOX_ID = os.getenv("CHATWOOT_INBOX_ID")   # WhatsApp inbox ID in Chatwoot

def chatwoot_headers():
    return {
        'api_access_token': CHATWOOT_API_TOKEN,
        'Content-Type': 'application/json',
    }

def find_chatwoot_contact(phone):
    """Search for a Chatwoot contact by phone number. Returns contact dict or None."""
    url = f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts/search"
    resp = requests.get(url, params={'q': phone, 'include_contacts': 'true'}, headers=chatwoot_headers(), timeout=15)
    if resp.status_code != 200:
        print(f"Contact search failed ({resp.status_code}): {resp.text}")
        return None
    data = resp.json()
    contacts = data.get('payload', [])
    return contacts[0] if contacts else None

def create_chatwoot_contact(phone):
    """Create a new Chatwoot contact with the given phone number."""
    url = f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts"
    payload = {'phone_number': phone}
    resp = requests.post(url, json=payload, headers=chatwoot_headers(), timeout=15)
    if resp.status_code not in (200, 201):
        print(f"Contact creation failed ({resp.status_code}): {resp.text}")
        return None
    return resp.json()

def get_or_create_contact(phone):
    contact = find_chatwoot_contact(phone)
    if not contact:
        print(f"Contact not found for {phone}, creating...")
        contact = create_chatwoot_contact(phone)
    return contact


def send_whatsapp_template(contact_id, lang, template_name, template_category, template_params):
    # Create a new conversation
    conv_url = f"{CHATWOOT_BASE_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations"
    conv_payload = {
        'inbox_id': int(CHATWOOT_INBOX_ID),
        'contact_id': contact_id,
        'message': {
            'template_params': {
                'name': template_name,
                'category': template_category,
                'language': lang,
                'processed_params': template_params,
            }
        },
    }
    resp = requests.post(conv_url, json=conv_payload, headers=chatwoot_headers(), timeout=15)
    if resp.status_code not in (200, 201):
        print(f"Conversation/template send failed ({resp.status_code}): {resp.text}")
        return False
    print(f"Template sent to contact_id={contact_id}")
    return True