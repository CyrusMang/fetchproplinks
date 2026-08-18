import requests
import time
from datetime import datetime
from selenium.webdriver.common.by import By
from models.prop import Prop
from reviewers.monitor import extract_monitor_snapshot, build_monitor_update

# sign_message = '有關資料可能已被移除或隱藏'

def review(db, driver, prop):
    response = requests.get(prop['source_url'], allow_redirects=False, timeout=15)
    if response.status_code != 200:
        Prop(db, prop).archive()
        print(f"Archived place {prop['source_id']} due to inaccessible URL.")
        return
    driver.get(prop['source_url'])

    time.sleep(1) 
    current_url = driver.current_url
    still_accessible = False
    if current_url == prop['source_url'] or current_url == prop['source_url'] + "/":
        try:
            error_page = driver.find_element(By.CSS_SELECTOR, '.error .header')
            if error_page:
                still_accessible = False
        except:
            still_accessible = True
    else:
        still_accessible = False

    if not still_accessible:
        Prop(db, prop).archive()
        print(f"Archived place {prop['source_id']} due to inaccessible URL.")
        return

    now = datetime.now().timestamp()
    snapshot = extract_monitor_snapshot(prop['source_channel'], response.text)
    update_data, reasons = build_monitor_update(prop, snapshot, now)
    Prop(db, prop).update(update_data)

    if update_data.get('monitor_change_pending'):
        print(f"Place {prop['source_id']} change candidate: {','.join(reasons)}")
    elif reasons and reasons != ['initial_monitor']:
        print(f"Place {prop['source_id']} changed: {','.join(reasons)}")
    else:
        print(f"Place {prop['source_id']} is still accessible.")
