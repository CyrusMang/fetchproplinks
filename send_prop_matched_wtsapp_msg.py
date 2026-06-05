import chatwoot_api_helpers

templates = {
    1: ["new_prop_matched_1", ['zh-cn'], 'UTILITY'],
    2: ["new_prop_matched_2", ['zh-hk', 'zh-cn'], 'UTILITY'],
    3: ["new_prop_matched_3", ['en', 'zh-hk', 'zh-cn'], 'UTILITY'],
    4: ["new_prop_matched_4", ['zh-hk', 'zh-cn'], 'UTILITY'],
    # 5: ["new_prop_matched_5", ['en', 'zh-hk'], 'UTILITY'],
}

def get_right_num_of_props(num_props, lang):
    if num_props >= 4 or num_props < 1:
        raise ValueError(f"Invalid number of properties: {num_props}")
    template = templates[num_props]
    if lang not in template[1]:
        return get_template_id(num_props - 1, lang)
    return num_props

def get_template_and_props(props, lang):
    num_props = min(len(props), 4)
    try:
        right_num = get_right_num_of_props(num_props, lang)
        return [templates[right_num][0], templates[right_num][2], props[:right_num]]
    except ValueError:
        return None

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
    r = get_template_and_props(props, lang)
    if not r:
        print(f"No suitable template and language {lang}")
        return False
    template_name, template_category, selected_props = r

    for i, prop in enumerate(selected_props, start=1):
        extracted = prop.get('v1_extracted_data')
        summary = prop.get('v1_summary_data')
        size = extracted.get('net_size_sqft')
        price = extracted.get(f'{rent_or_sell}_price')
        template_params[f'prop_{i}_title'] = summary.get(f'headline_{lang.replace("-", "_")}')
        template_params[f'prop_{i}_price'] = f"${price}"
        template_params[f'prop_{i}_size'] = f"{size} ft²" if size else "N/A"
        template_params[f'prop_{i}_link'] = prop.get('source_url')
    
    print(f"phone: {phone}, lang: {lang}, total: {len(props)}, selected_props: {len(selected_props)}")
    return chatwoot_api_helpers.send_whatsapp_template(
        contact_id, lang, template_name, template_category, template_params
    )