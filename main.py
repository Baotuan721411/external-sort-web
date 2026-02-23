from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import os
import uuid
import json

from sorter.external_merge_sort import external_merge_sort

app = FastAPI(title="External Merge Sort Service")

# ================= PATH CONFIG =================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")

INPUT_DIR = os.path.join(BASE_DIR, "storage", "input")
OUTPUT_DIR = os.path.join(BASE_DIR, "storage", "output")

os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# ================= IN-MEMORY STORAGE =================
jobs = {}
steps_storage = {}

# ================= HOMEPAGE =================
@app.get("/")
def homepage():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))

# ================= RUN SORT (BACKGROUND) =================
def run_sort(job_id: str, input_path: str, output_path: str):
    global jobs, steps_storage

    try:
        jobs[job_id] = "processing"

        # external_merge_sort MUST return list of steps
        steps = external_merge_sort(input_path, output_path)

        # lưu vào memory
        steps_storage[job_id] = steps

        # ghi ra file để persistent
        steps_file = os.path.join(OUTPUT_DIR, f"steps_{job_id}.json")
        with open(steps_file, "w", encoding="utf-8") as f:
            json.dump({"steps": steps}, f, indent=2)

        jobs[job_id] = "done"

    except Exception as e:
        jobs[job_id] = f"error: {str(e)}"

# ================= UPLOAD =================
@app.post("/sort")
async def sort_file(background_tasks: BackgroundTasks, file: UploadFile = File(...)):

    try:
        job_id = str(uuid.uuid4())

        input_filename = f"{job_id}_{file.filename}"
        output_filename = f"sorted_{job_id}.bin"

        input_path = os.path.join(INPUT_DIR, input_filename)
        output_path = os.path.join(OUTPUT_DIR, output_filename)

        # lưu file upload (stream)
        with open(input_path, "wb") as buffer:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                buffer.write(chunk)

        jobs[job_id] = "queued"

        # chạy nền
        background_tasks.add_task(run_sort, job_id, input_path, output_path)

        return {
            "message": "Upload successful",
            "job_id": job_id,
            "status_url": f"/status/{job_id}",
            "steps_url": f"/steps/{job_id}",
            "download_url": f"/download/{job_id}"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ================= CHECK STATUS =================
@app.get("/status/{job_id}")
def check_status(job_id: str):

    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    return {
        "job_id": job_id,
        "status": jobs[job_id]
    }

# ================= GET STEPS =================
@app.get("/steps/{job_id}")
def get_steps(job_id: str):

    # nếu có trong RAM
    if job_id in steps_storage:
        return JSONResponse(content={"steps": steps_storage[job_id]})

    # nếu không có trong RAM, thử đọc file
    steps_file = os.path.join(OUTPUT_DIR, f"steps_{job_id}.json")

    if os.path.exists(steps_file):
        try:
            with open(steps_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            # cache lại vào memory
            steps_storage[job_id] = data["steps"]

            return JSONResponse(content=data)

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to read steps file: {e}")

    raise HTTPException(status_code=404, detail="Steps not ready")

# ================= DOWNLOAD =================
@app.get("/download/{job_id}")
def download_file(job_id: str):

    output_filename = f"sorted_{job_id}.bin"
    file_path = os.path.join(OUTPUT_DIR, output_filename)

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not ready yet")

    return FileResponse(
        path=file_path,
        filename="sorted.bin",
        media_type="application/octet-stream"
    )