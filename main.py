from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import os
import uuid
import json

from sorter.external_merge_sort import external_merge_sort

app = FastAPI()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")

INPUT_DIR = os.path.join(BASE_DIR, "storage", "input")
OUTPUT_DIR = os.path.join(BASE_DIR, "storage", "output")

os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

jobs = {}
steps_storage = {}

@app.get("/")
def homepage():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))

# ================= RUN SORT =================
def run_sort(job_id, input_path, output_path):
    try:
        jobs[job_id] = "processing"

        steps = external_merge_sort(input_path, output_path)
        steps_storage[job_id] = steps

        with open(os.path.join(OUTPUT_DIR, f"steps_{job_id}.json"), "w") as f:
            json.dump({"steps": steps}, f)

        jobs[job_id] = "done"

    except Exception as e:
        jobs[job_id] = f"error: {e}"

@app.post("/sort")
async def sort_file(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    job_id = str(uuid.uuid4())

    input_path = os.path.join(INPUT_DIR, f"{job_id}.bin")
    output_path = os.path.join(OUTPUT_DIR, f"sorted_{job_id}.bin")

    with open(input_path, "wb") as buffer:
        while True:
            chunk = await file.read(1024*1024)
            if not chunk:
                break
            buffer.write(chunk)

    jobs[job_id] = "queued"
    background_tasks.add_task(run_sort, job_id, input_path, output_path)

    return {"job_id": job_id}

@app.get("/status/{job_id}")
def status(job_id: str):
    return {"status": jobs.get(job_id, "processing")}

# ⭐⭐⭐ QUAN TRỌNG: TRẢ META
@app.get("/steps/{job_id}")
def get_steps(job_id: str):

    if job_id not in steps_storage:
        raise HTTPException(404, "steps not ready")

    steps = steps_storage[job_id]
    meta = steps[0] if steps and steps[0]["type"] == "meta" else None

    return JSONResponse({
        "steps": steps,
        "meta": meta
    })

@app.get("/download/{job_id}")
def download(job_id: str):
    file_path = os.path.join(OUTPUT_DIR, f"sorted_{job_id}.bin")
    if not os.path.exists(file_path):
        raise HTTPException(404, "file not ready")

    return FileResponse(file_path, filename="sorted.bin")