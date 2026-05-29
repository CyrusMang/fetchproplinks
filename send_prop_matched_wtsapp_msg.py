import chatwoot_api_helpers

def send(rent_or_sell, phone, lang, total, props):
    contact = chatwoot_api_helpers.get_or_create_contact(phone)
    if not contact:
        print(f"Failed to get or create contact for {phone}")
        return False
    contact_id = contact.get('id')
    if not contact_id:
        print(f"Contact found but missing ID for {phone}")
        return False
    template_params = {
        'total': total,
    }
    selected_props = []  # Limit to top 5 properties
    for prop in props:
        extracted = prop.get('v1_extracted_data')
        summary = prop.get('v1_summary_data')
        if not extracted or not summary:
            return False
        price = extracted.get(f'{rent_or_sell}_price')
        if not price:
            return False
        selected_props.append(prop)
        if len(selected_props) >= 5:
            break
    for i, prop in enumerate(selected_props, start=1):
        extracted = prop.get('v1_extracted_data')
        summary = prop.get('v1_summary_data')
        size = extracted.get('net_size_sqft')
        price = extracted.get(f'{rent_or_sell}_price')
        template_params[f'prop_{i}_title'] = summary.get(f'headline_{lang}')
        template_params[f'prop_{i}_price'] = f"${price}"
        template_params[f'prop_{i}_size'] = f"{size} ft²" if size else "N/A"
        template_params[f'prop_{i}_link'] = prop.get('source_url')
    return chatwoot_api_helpers.send_whatsapp_template(
        contact_id, lang, f"new_prop_matched_{str(len(selected_props))}", 'UTILITY', template_params
    )