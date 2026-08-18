import hashlib
import json
import re

from bs4 import BeautifulSoup

TWO_HIT_SOURCES = {'midland', 'house730'}


def _normalize_space(value):
    if value is None:
        return ''
    return re.sub(r'\s+', ' ', str(value)).strip()


def _normalize_price(value):
    text = _normalize_space(value)
    if not text:
        return ''

    text = text.replace('HK$', '').replace('$', '').replace(',', '')
    text = text.replace('港元', '').replace('/月', '').replace('每月', '')
    text = _normalize_space(text)
    return text


def _extract_title_text(soup):
    title_node = soup.select_one('title')
    if not title_node:
        return ''
    return _normalize_space(title_node.get_text(' ', strip=True))


def _extract_first_price_by_regex(text):
    cleaned = _normalize_space(text)
    if not cleaned:
        return ''

    patterns = [
        r'(?:HK\$|\$)\s*([0-9][0-9,]{2,})',
        r'([0-9][0-9,]{3,})\s*港元',
    ]
    for pattern in patterns:
        match = re.search(pattern, cleaned, flags=re.I)
        if match:
            return match.group(1)
    return ''


def _extract_price_from_ld_json(soup):
    for script in soup.select('script[type="application/ld+json"]'):
        raw = (script.string or script.get_text() or '').strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue

        items = payload if isinstance(payload, list) else [payload]
        for item in items:
            if not isinstance(item, dict):
                continue
            offers = item.get('offers')
            if isinstance(offers, dict):
                price = offers.get('price')
                if price is not None:
                    return str(price)
    return ''


def _extract_dates_from_ld_json(soup):
    posted = ''
    updated = ''
    for script in soup.select('script[type="application/ld+json"]'):
        raw = (script.string or script.get_text() or '').strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue

        items = payload if isinstance(payload, list) else [payload]
        for item in items:
            if not isinstance(item, dict):
                continue
            if not posted:
                posted = _normalize_space(item.get('datePosted') or item.get('datePublished'))
            if not updated:
                updated = _normalize_space(item.get('dateModified'))
            if posted and updated:
                return posted, updated
    return posted, updated


def _extract_28hse_snapshot(soup):
    snapshot = {
        'price_raw': '',
        'posted_date_raw': '',
        'updated_date_raw': '',
        'status_raw': 'active',
        'title_raw': _extract_title_text(soup),
        'confidence': 'low',
    }

    content_body_div = soup.select_one('.content_body .ten')
    if content_body_div:
        date_node = content_body_div.select_one('.propertyDate')
        if date_node:
            date_text = _normalize_space(date_node.get_text(' ', strip=True))
            date_parts = [part.strip() for part in date_text.split('|') if part.strip()]
            for part in date_parts:
                if part.startswith('刊登:'):
                    snapshot['posted_date_raw'] = _normalize_space(part.replace('刊登:', '', 1))
                elif part.startswith('更新:'):
                    snapshot['updated_date_raw'] = _normalize_space(part.replace('更新:', '', 1))

        for pair in content_body_div.select('table.tablePair tr'):
            key_node = pair.select_one('td.table_left')
            value_node = pair.select_one('.pairValue')
            if not key_node or not value_node:
                continue
            key_text = _normalize_space(key_node.get_text(' ', strip=True))
            if any(token in key_text for token in ['租金', '售價', '叫價']):
                snapshot['price_raw'] = _normalize_space(value_node.get_text(' ', strip=True))
                break

    if not snapshot['price_raw']:
        snapshot['price_raw'] = _extract_price_from_ld_json(soup)

    if not snapshot['posted_date_raw'] or not snapshot['updated_date_raw']:
        ld_posted, ld_updated = _extract_dates_from_ld_json(soup)
        if not snapshot['posted_date_raw']:
            snapshot['posted_date_raw'] = ld_posted
        if not snapshot['updated_date_raw']:
            snapshot['updated_date_raw'] = ld_updated

    if snapshot['price_raw'] and snapshot['updated_date_raw']:
        snapshot['confidence'] = 'high'
    elif snapshot['price_raw']:
        snapshot['confidence'] = 'medium'
    return snapshot


def _extract_house730_snapshot(soup):
    snapshot = {
        'price_raw': '',
        'posted_date_raw': '',
        'updated_date_raw': '',
        'status_raw': 'active',
        'title_raw': _extract_title_text(soup),
        'confidence': 'low',
    }

    detail = soup.select_one('#pc-services-detail') or soup

    # Try CSS hints before broad text regex.
    for node in detail.select('[class*="price" i], [data-testid*="price" i]'):
        text = _normalize_space(node.get_text(' ', strip=True))
        candidate = _extract_first_price_by_regex(text)
        if candidate:
            snapshot['price_raw'] = candidate
            break

    if not snapshot['price_raw']:
        snapshot['price_raw'] = _extract_first_price_by_regex(detail.get_text(' ', strip=True))

    if snapshot['price_raw']:
        snapshot['confidence'] = 'medium'
    return snapshot


def _extract_midland_snapshot(soup):
    snapshot = {
        'price_raw': '',
        'posted_date_raw': '',
        'updated_date_raw': '',
        'status_raw': 'active',
        'title_raw': _extract_title_text(soup),
        'confidence': 'low',
    }

    main = soup.select_one('main') or soup

    # Prioritize visible "price" blocks, then fallback to page text.
    for node in main.select('[class*="price" i], [class*="rent" i], [data-testid*="price" i]'):
        text = _normalize_space(node.get_text(' ', strip=True))
        candidate = _extract_first_price_by_regex(text)
        if candidate:
            snapshot['price_raw'] = candidate
            break

    if not snapshot['price_raw']:
        snapshot['price_raw'] = _extract_first_price_by_regex(main.get_text(' ', strip=True))

    if snapshot['price_raw']:
        snapshot['confidence'] = 'medium'
    return snapshot


def extract_monitor_snapshot(source_channel, html):
    soup = BeautifulSoup(html or '', 'lxml')
    if source_channel == '28hse':
        snapshot = _extract_28hse_snapshot(soup)
    elif source_channel == 'house730':
        snapshot = _extract_house730_snapshot(soup)
    elif source_channel == 'midland':
        snapshot = _extract_midland_snapshot(soup)
    else:
        snapshot = {
            'price_raw': _extract_price_from_ld_json(soup),
            'posted_date_raw': '',
            'updated_date_raw': '',
            'status_raw': 'active',
            'title_raw': _extract_title_text(soup),
            'confidence': 'low',
        }

    if not snapshot.get('posted_date_raw') or not snapshot.get('updated_date_raw'):
        ld_posted, ld_updated = _extract_dates_from_ld_json(soup)
        snapshot['posted_date_raw'] = snapshot.get('posted_date_raw') or ld_posted
        snapshot['updated_date_raw'] = snapshot.get('updated_date_raw') or ld_updated

    snapshot['price_norm'] = _normalize_price(snapshot['price_raw'])
    snapshot['posted_date_norm'] = _normalize_space(snapshot['posted_date_raw'])
    snapshot['updated_date_norm'] = _normalize_space(snapshot['updated_date_raw'])
    snapshot['title_norm'] = _normalize_space(snapshot.get('title_raw'))
    snapshot['confidence'] = snapshot.get('confidence', 'low')
    return snapshot


def build_monitor_fingerprint(source_id, source_channel, snapshot):
    common = [
        _normalize_space(source_id),
        _normalize_space(snapshot.get('status_raw')),
    ]

    if source_channel == '28hse':
        parts = common + [
            _normalize_space(snapshot.get('price_norm')),
            _normalize_space(snapshot.get('posted_date_norm')),
            _normalize_space(snapshot.get('updated_date_norm')),
        ]
    else:
        parts = common + [
            _normalize_space(snapshot.get('price_norm')),
            _normalize_space(snapshot.get('title_norm')),
        ]

    fingerprint_source = '|'.join(parts)
    return hashlib.sha1(fingerprint_source.encode('utf-8')).hexdigest()


def _apply_two_hit_confirmation(monitor, source_channel, next_fingerprint, reasons, now):
    if source_channel not in TWO_HIT_SOURCES:
        return True, False

    pending = dict(monitor.get('pending_change') or {})
    if pending.get('fingerprint') == next_fingerprint:
        seen_count = int(pending.get('seen_count', 1) or 1) + 1
    else:
        seen_count = 1

    pending = {
        'fingerprint': next_fingerprint,
        'reasons': reasons,
        'first_seen_at': pending.get('first_seen_at', now) if pending.get('fingerprint') == next_fingerprint else now,
        'last_seen_at': now,
        'seen_count': seen_count,
    }
    monitor['pending_change'] = pending

    if seen_count >= 2:
        monitor['pending_change'] = None
        return True, False
    return False, True


def build_monitor_update(prop, snapshot, now):
    monitor = dict(prop.get('source_monitor') or {})
    source_channel = prop.get('source_channel')
    previous_fingerprint = _normalize_space(monitor.get('fingerprint'))
    next_fingerprint = build_monitor_fingerprint(prop.get('source_id'), source_channel, snapshot)

    change_reasons = []
    if previous_fingerprint and previous_fingerprint != next_fingerprint:
        if _normalize_space(monitor.get('price_raw')) != _normalize_space(snapshot.get('price_raw')):
            change_reasons.append('price_changed')
        if _normalize_space(monitor.get('posted_date_raw')) != _normalize_space(snapshot.get('posted_date_raw')):
            change_reasons.append('posted_date_changed')
        if _normalize_space(monitor.get('updated_date_raw')) != _normalize_space(snapshot.get('updated_date_raw')):
            change_reasons.append('updated_date_changed')
        if _normalize_space(monitor.get('status_raw')) != _normalize_space(snapshot.get('status_raw')):
            change_reasons.append('status_changed')
        if _normalize_space(monitor.get('title_raw')) != _normalize_space(snapshot.get('title_raw')):
            change_reasons.append('title_changed')
        if not change_reasons:
            change_reasons.append('fingerprint_changed')
    elif not previous_fingerprint:
        change_reasons.append('initial_monitor')

    monitor.update({
        'price_raw': snapshot.get('price_raw', ''),
        'price_norm': snapshot.get('price_norm', ''),
        'posted_date_raw': snapshot.get('posted_date_raw', ''),
        'posted_date_norm': snapshot.get('posted_date_norm', ''),
        'updated_date_raw': snapshot.get('updated_date_raw', ''),
        'updated_date_norm': snapshot.get('updated_date_norm', ''),
        'status_raw': snapshot.get('status_raw', 'active'),
        'title_raw': snapshot.get('title_raw', ''),
        'title_norm': snapshot.get('title_norm', ''),
        'confidence': snapshot.get('confidence', 'low'),
        'fingerprint': next_fingerprint,
        'last_checked_at': now,
    })

    if not monitor.get('first_checked_at'):
        monitor['first_checked_at'] = now

    changed = bool(change_reasons) and change_reasons != ['initial_monitor']
    pending = False
    if changed:
        confirmed = snapshot.get('confidence') == 'high'
        if not confirmed:
            confirmed, pending = _apply_two_hit_confirmation(
                monitor,
                source_channel,
                next_fingerprint,
                change_reasons,
                now,
            )
        else:
            monitor['pending_change'] = None

        if not confirmed:
            changed = False

    if changed:
        monitor['last_changed_at'] = now
        monitor['change_count'] = int(monitor.get('change_count', 0) or 0) + 1
    elif not pending:
        monitor['pending_change'] = None

    update_data = {
        'source_monitor': monitor,
        'reviewed_at': now,
        'monitor_change_pending': pending,
    }

    if changed:
        update_data['reextract_needed'] = True
        update_data['reextract_reason'] = ','.join(change_reasons)
    elif pending:
        update_data['reextract_candidate'] = True
        update_data['reextract_reason'] = f"pending:{','.join(change_reasons)}"
    elif prop.get('reextract_needed') is None:
        update_data['reextract_needed'] = False

    return update_data, change_reasons