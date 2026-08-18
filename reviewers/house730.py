import time
from datetime import datetime
from selenium.webdriver.common.by import By
from models.prop import Prop
from reviewers.monitor import extract_monitor_snapshot, build_monitor_update

sign_message = ['樓盤已過期', '此樓盤已被隱藏']

def review(db, driver, prop):
    try:
        driver.get(prop['source_url'])

        time.sleep(1) 
        current_url = driver.current_url
        still_accessible = False
        if current_url == prop['source_url'] or current_url == prop['source_url'] + "/":
            try:
                error_page = driver.find_element(By.CSS_SELECTOR, '.detail-error-page')
                if error_page:
                    still_accessible = False
            except:
                still_accessible = True
        else:
            still_accessible = False

        try:
            content_body_div = driver.find_element(By.ID, 'pc-services-detail')
            if any(msg in content_body_div.get_attribute('outerHTML') for msg in sign_message):
                still_accessible = False
        except:
            still_accessible = False
    
        if not still_accessible:
            Prop(db, prop).archive()
            print(f"Archived place {prop['source_id']} due to inaccessible URL.")
        else:
            now = datetime.now().timestamp()
            snapshot = extract_monitor_snapshot(prop['source_channel'], driver.page_source)
            update_data, reasons = build_monitor_update(prop, snapshot, now)
            Prop(db, prop).update(update_data)
            if update_data.get('monitor_change_pending'):
                print(f"Place {prop['source_id']} change candidate: {','.join(reasons)}")
            elif reasons and reasons != ['initial_monitor']:
                print(f"Place {prop['source_id']} changed: {','.join(reasons)}")
            else:
                print(f"Place {prop['source_id']} is still accessible.")
    except:
        print(f"Webdriver Error, skip {prop['source_id']}.")