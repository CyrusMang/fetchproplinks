import time
from datetime import datetime
from selenium.webdriver.common.by import By
from models.prop import Prop

def review(db, driver, prop):
    try:
        driver.get(prop['source_url'])

        time.sleep(3) 
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
    
        if not still_accessible:
            Prop(db, prop).archive()
            print(f"Archived place {prop['source_id']} due to inaccessible URL.")
        else:
            Prop(db, prop).update({'updated_at': datetime.now().timestamp()})
            print(f"Place {prop['source_id']} is still accessible.")
    except:
        print(f"Webdriver Error, skip {prop['source_id']}.")