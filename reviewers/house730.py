import time
from datetime import datetime
from selenium.webdriver.common.by import By
from models.prop import Prop

def review(db, driver, prop):
    still_accessible = False
    driver.get(prop['source_url'])

    time.sleep(3) 
    try:
        current_url = driver.current_url
        if current_url == prop['source_url'] or current_url == prop['source_url'] + "/":
            try:
                error_page = driver.find_element(By.CSS_SELECTOR, '.detail-error-page')
                if error_page:
                    still_accessible = False
            except:
                still_accessible = True
        else:
            still_accessible = False
    except:
        still_accessible = False
    
    if not still_accessible:
        Prop(db, prop).update({
            'status': "archived", 
            "updated_at": datetime.now().timestamp(), 
        })
        print(f"Archived place {prop['source_id']} due to inaccessible URL.")
    else:
        print(f"Place {prop['source_id']} is still accessible.")