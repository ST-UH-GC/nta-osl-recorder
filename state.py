import threading

_lock = threading.Lock()
_vehicles: dict = {}


def update_vehicle(vid: str, data: dict) -> None:
    with _lock:
        _vehicles[vid] = data


def remove_vehicle(vid: str) -> None:
    with _lock:
        _vehicles.pop(vid, None)


def get_all_vehicles() -> dict:
    with _lock:
        return dict(_vehicles)
