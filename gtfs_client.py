"""
Fetches Oslo Entur VehiclePositions GTFS-RT feed every 30 s.

Endpoint:
  https://api.entur.io/realtime/v1/gtfs-rt/vehicle-positions
  No auth required. ET-Client-Name header identifies the client.

Vehicle type classification (by NeTEx route_id operator prefix):
  RUT:Line:1-5   → metro   (T-bane lines 1–5)
  RUT:Line:11-19 → tram    (Trikk lines 11–19)
  VYX / GOA / FLT / NSB → train
  NOR / FRO / TID / WF   → ferry
  everything else        → bus

Bounding box covers Greater Oslo (Asker → Lillestrøm, Nesodden → Nittedal).
All types are recorded; the replay viewer's checkbox filters handle "no buses".
"""

import gzip
import threading
import time
import urllib.request
import urllib.error

from google.transit import gtfs_realtime_pb2
import state

INTERVAL = 30

# Greater Oslo bounding box
LAT_MIN, LAT_MAX = 59.70, 60.10
LON_MIN, LON_MAX = 10.30, 11.10

FEED_URL = "https://api.entur.io/realtime/v1/gtfs-rt/vehicle-positions"
CLIENT_NAME = "nta-osl-recorder"


def _classify(route_id: str) -> str:
    """Return vehicle type based on NeTEx route_id operator and line number."""
    if not route_id or ":" not in route_id:
        return "bus"

    parts = route_id.split(":")
    if len(parts) < 3:
        return "bus"

    operator = parts[0]   # e.g. "RUT", "VYX", "GOA"
    line_part = parts[2]  # e.g. "1", "11", "20", "1_453"

    # Trains: Vy, Go-Ahead, Flytoget, SJ Nord
    if operator in ("VYX", "GOA", "FLT", "NSB", "SJN"):
        return "train"

    # Ferries: Norled and similar water operators
    if operator in ("NOR", "FRO", "TID", "WF", "SKY"):
        return "ferry"

    # Ruter (Oslo city transit)
    if operator == "RUT":
        line_str = line_part.split("_")[0]
        try:
            line_num = int(line_str)
        except ValueError:
            return "bus"
        if 1 <= line_num <= 5:
            return "metro"
        if 11 <= line_num <= 19:
            return "tram"
        return "bus"

    return "bus"


def _fetch_and_update() -> int:
    try:
        req = urllib.request.Request(
            FEED_URL,
            headers={
                "ET-Client-Name": CLIENT_NAME,
                "Accept-Encoding": "gzip",
            },
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as e:
        print(f"[GTFS] HTTP {e.code}: {e.reason}")
        return 0
    except Exception as e:
        print(f"[GTFS] Fetch error: {e}")
        return 0

    # Decompress if gzip
    if raw[:2] == b'\x1f\x8b':
        raw = gzip.decompress(raw)

    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        feed.ParseFromString(raw)
    except Exception as e:
        print(f"[GTFS] Parse error: {e}")
        return 0

    seen: set = set()

    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        veh = entity.vehicle
        if not veh.HasField("position"):
            continue

        lat = veh.position.latitude
        lon = veh.position.longitude

        if lat == 0.0 and lon == 0.0:
            continue
        if not (LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX):
            continue

        vid = veh.vehicle.id or entity.id
        if not vid:
            continue

        route_id = veh.trip.route_id or ""
        vtype = _classify(route_id)
        route = route_id.split(":")[-1].split("_")[0] if route_id else ""

        seen.add(vid)
        state.update_vehicle(vid, {
            "lat":   round(lat, 6),
            "lon":   round(lon, 6),
            "type":  vtype,
            "route": route,
        })

    # Remove vehicles gone from feed
    for gone in set(state.get_all_vehicles().keys()) - seen:
        state.remove_vehicle(gone)

    return len(seen)


def _poll_loop() -> None:
    print(f"[GTFS] Starting poll loop (every {INTERVAL}s)")
    backoff = 5
    while True:
        try:
            n = _fetch_and_update()
            print(f"[GTFS] {n} vehicles in state")
            backoff = 5
        except Exception as e:
            print(f"[GTFS] Unexpected error: {e} — retry in {backoff}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue
        time.sleep(INTERVAL)


def start_gtfs_thread() -> None:
    threading.Thread(target=_poll_loop, daemon=True).start()
