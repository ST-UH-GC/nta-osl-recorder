import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse

import gtfs_client
import recorder
import state

RECORD_DIR = os.environ.get("RECORD_DIR", "/data")


@asynccontextmanager
async def lifespan(app: FastAPI):
    gtfs_client.start_gtfs_thread()
    recorder.start_recorder()
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/")
def root():
    return {"status": "ok", "city": "oslo"}


@app.get("/health")
def health():
    vehicles = state.get_all_vehicles()
    return JSONResponse({"vehicles": len(vehicles)})


def _latest_recording():
    try:
        files = [f for f in os.listdir(RECORD_DIR) if f.endswith(".jsonl")]
        if not files:
            return None
        return os.path.join(RECORD_DIR, max(files, key=lambda f: os.path.getmtime(os.path.join(RECORD_DIR, f))))
    except Exception:
        return None


@app.get("/latest.jsonl")
def latest():
    path = _latest_recording()
    if not path:
        return JSONResponse({"error": "no recording yet"}, status_code=404)
    return FileResponse(path, media_type="application/x-ndjson", filename=os.path.basename(path))
