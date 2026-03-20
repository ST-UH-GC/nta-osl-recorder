import json
import os
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import state

RECORD_DIR = os.environ.get("RECORD_DIR", "/data")
INTERVAL   = 30
_TZ        = ZoneInfo("Europe/Oslo")


def _record_path() -> str | None:
    local = datetime.fromtimestamp(time.time(), tz=_TZ)
    wd, hr = local.weekday(), local.hour  # 0=Mon … 6=Sun
    if wd == 6:                           # Sunday all day
        return os.path.join(RECORD_DIR, "sunday.jsonl")
    return None                           # outside window — idle


def _record_loop() -> None:
    os.makedirs(RECORD_DIR, exist_ok=True)
    print(f"[Recorder] Starting (every {INTERVAL}s, recording Sundays)")
    while True:
        time.sleep(INTERVAL)
        try:
            path = _record_path()
            if not path:
                continue
            vehicles = state.get_all_vehicles()
            if not vehicles:
                continue
            compact = {
                vid: [round(v["lat"], 6), round(v["lon"], 6), v["type"], v["route"]]
                for vid, v in vehicles.items()
            }
            with open(path, "a") as f:
                f.write(json.dumps({"t": int(time.time()), "v": compact}) + "\n")
        except Exception as e:
            print(f"[Recorder] {e}")


def start_recorder() -> None:
    threading.Thread(target=_record_loop, daemon=True).start()
