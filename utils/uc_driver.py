import threading
import undetected_chromedriver as uc

_driver_lock = threading.Lock()


def create_uc_driver(*, options=None, version_main=None, use_subprocess=True):
    with _driver_lock:
        return uc.Chrome(
            options=options,
            use_subprocess=use_subprocess,
            version_main=version_main,
        )
