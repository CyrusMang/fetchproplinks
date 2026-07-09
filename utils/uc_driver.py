import re
import subprocess
import threading
from shutil import which

import undetected_chromedriver as uc

_driver_lock = threading.Lock()


def _detect_chrome_version_main():
    candidates = [
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary",
        "google-chrome",
        "google-chrome-stable",
        "chromium",
        "chromium-browser",
    ]

    for candidate in candidates:
        browser_path = candidate if candidate.startswith("/") else which(candidate)
        if not browser_path:
            continue
        try:
            completed = subprocess.run(
                [browser_path, "--version"],
                check=True,
                capture_output=True,
                text=True,
            )
        except Exception:
            continue

        match = re.search(r"(\d+)\.", completed.stdout)
        if match:
            return int(match.group(1))

    return None


def create_uc_driver(*, options=None, version_main=None, use_subprocess=True):
    resolved_version_main = _detect_chrome_version_main()
    if resolved_version_main is None:
        resolved_version_main = version_main

    with _driver_lock:
        return uc.Chrome(
            options=options,
            use_subprocess=use_subprocess,
            version_main=resolved_version_main,
        )
