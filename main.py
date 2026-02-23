# main.py
from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import os
import uuid
import json
import threading

from sorter.external_merge_sort import external_merge_sort

app = FastAPI(title="External Merge Sort Service")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")

INPUT_DIR = os.path.join(BASE_DIR, "storage", "input")
OUTPUT_DIR = os.path.join(BASE_DIR, "storage", "output")

os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# in-memory job tracking
jobs = {}           # job_id -> status string
job_files = {}      # job_id -> input filename path
steps_storage = {}  # job_id -> steps list


@app.get("/")
def homepage():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


# ---------------- API: upload only (do not start sort) ----------------
@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        job_id = str(uuid.uuid4())
        input_filename = f"{job_id}_{file.filename}"
        input_path = os.path.join(INPUT_DIR, input_filename)

        # stream save
        with open(input_path, "wb") as buffer:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                buffer.write(chunk)

        jobs[job_id] = "uploaded"
        job_files[job_id] = input_path

        return {
            "job_id": job_id,
            "file_name": file.filename
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------- API: start sorting (provide k and block_size) ----------------
def _run_sort_thread(job_id, input_path, output_path, block_size, k):
    try:
        jobs[job_id] = "processing"
        # run external_merge_sort with given params
        steps = external_merge_sort(input_path, output_path, block_size=block_size, k=k)

        # store steps in memory and persist
        steps_storage[job_id] = steps
        with open(os.path.join(OUTPUT_DIR, f"steps_{job_id}.json"), "w", encoding="utf-8") as f:
            json.dump({"steps": steps}, f, indent=2)

        jobs[job_id] = "done"
    except Exception as e:
        jobs[job_id] = f"error: {str(e)}"


@app.post("/start/{job_id}")
def start_sort(job_id: str, block_size: int = Form(None), k: int = Form(None)):
    """
    Start sorting a previously uploaded file.
    client should POST form fields 'block_size' and 'k' (both optional).
    """
    if job_id not in job_files:
        raise HTTPException(status_code=404, detail="Job not found or file not uploaded")

    input_path = job_files[job_id]
    output_path = os.path.join(OUTPUT_DIR, f"sorted_{job_id}.bin")

    # set queued status and start thread
    jobs[job_id] = "queued"

    t = threading.Thread(
        target=_run_sort_thread,
        args=(job_id, input_path, output_path, block_size, k),
        daemon=True
    )
    t.start()

    return {"message": "sort_started", "job_id": job_id}


@app.get("/status/{job_id}")
def check_status(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"job_id": job_id, "status": jobs[job_id]}


@app.get("/steps/{job_id}")
def get_steps(job_id: str):
    # try in-memory then on-disk
    if job_id in steps_storage:
        return JSONResponse(content={"steps": steps_storage[job_id]})

    steps_file = os.path.join(OUTPUT_DIR, f"steps_{job_id}.json")
    if os.path.exists(steps_file):
        try:
            with open(steps_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            steps_storage[job_id] = data["steps"]
            return JSONResponse(content=data)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to read steps file: {e}")

    raise HTTPException(status_code=404, detail="Steps not ready")


@app.get("/download/{job_id}")
def download_file(job_id: str):
    file_path = os.path.join(OUTPUT_DIR, f"sorted_{job_id}.bin")
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not ready yet")
    return FileResponse(path=file_path, filename="sorted.bin", media_type="application/octet-stream")