import requests
from datetime import datetime
from models.prop import Prop
from reviewers.monitor import extract_monitor_snapshot, build_monitor_update

def review(db, driver, prop):
    response = requests.get(prop['source_url'], allow_redirects=False, timeout=15)
    if response.status_code != 200:
        Prop(db, prop).archive()
        print(f"Archived place {prop['source_id']} due to inaccessible URL.")
    else:
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