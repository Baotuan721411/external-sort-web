# main.py
import os
import uuid
import json
import threading
from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sorter.external_merge_sort import external_merge_sort, external_merge_sort_visualize

app = FastAPI(title="External Merge Sort Service")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")
INPUT_DIR  = os.path.join(BASE_DIR, "storage", "input")
OUTPUT_DIR = os.path.join(BASE_DIR, "storage", "output")

os.makedirs(INPUT_DIR,  exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(STATIC_DIR, exist_ok=True)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# job tracking
jobs = {}   # job_id -> "queued" | "processing" | "done" | "error: ..."


# ── Homepage ──────────────────────────────────────────────────
@app.get("/")
def homepage():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


# ── Background sort thật ──────────────────────────────────────
def run_sort(job_id: str, input_path: str, output_path: str):
    try:
        jobs[job_id] = "processing"
        external_merge_sort(input_path, output_path)

        # cleanup input sau khi sort xong
        try:
            os.remove(input_path)
        except Exception:
            pass

        jobs[job_id] = "done"
    except Exception as e:
        jobs[job_id] = f"error: {str(e)}"
        try:
            os.remove(input_path)
        except Exception:
            pass


# ── /sort/visualize — endpoint chính ─────────────────────────
@app.post("/sort/visualize")
async def sort_visualize(file: UploadFile = File(...)):
    """
    Upload file .bin MỘT LẦN DUY NHẤT.
    - Lưu file vào disk
    - Chạy visualize ngay (sample 300 số) → trả steps JSON
    - Đồng thời khởi động sort thật ở background thread
    - Trả về job_id để frontend poll /status/{job_id} và /download/{job_id}
    """
    job_id      = str(uuid.uuid4())
    input_path  = os.path.join(INPUT_DIR,  f"{job_id}_{file.filename}")
    output_path = os.path.join(OUTPUT_DIR, f"sorted_{job_id}.bin")

    # 1. Lưu file upload vào disk (1 lần duy nhất)
    try:
        with open(input_path, "wb") as buf:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                buf.write(chunk)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {e}")

    # 2. Khởi động sort thật ở background (dùng file vừa lưu)
    jobs[job_id] = "queued"
    t = threading.Thread(
        target=run_sort,
        args=(job_id, input_path, output_path),
        daemon=True
    )
    t.start()

    # 3. Chạy visualize ngay trên cùng file (sample nhỏ, nhanh)
    try:
        steps = external_merge_sort_visualize(input_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Visualize error: {e}")

    # 4. Tìm meta và params
    meta   = next((s for s in steps if s.get("type") == "meta"),          None)
    params = next((s for s in steps if s.get("type") == "params_chosen"), None)

    return JSONResponse(content={
        "steps":        steps,
        "meta":         meta,
        "job_id":       job_id,
        "status_url":   f"/status/{job_id}",
        "download_url": f"/download/{job_id}",
        "sample_size":  params.get("visualize_sample_size") if params else None,
        "total_steps":  len(steps),
    })


# ── /sort — giữ lại để backward compat ───────────────────────
@app.post("/sort")
async def sort_file(file: UploadFile = File(...)):
    job_id      = str(uuid.uuid4())
    input_path  = os.path.join(INPUT_DIR,  f"{job_id}_{file.filename}")
    output_path = os.path.join(OUTPUT_DIR, f"sorted_{job_id}.bin")

    with open(input_path, "wb") as buf:
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break
            buf.write(chunk)

    jobs[job_id] = "queued"
    t = threading.Thread(
        target=run_sort,
        args=(job_id, input_path, output_path),
        daemon=True
    )
    t.start()

    return {
        "job_id":       job_id,
        "status_url":   f"/status/{job_id}",
        "download_url": f"/download/{job_id}",
    }


# ── Status & Download ─────────────────────────────────────────
@app.get("/status/{job_id}")
def check_status(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"job_id": job_id, "status": jobs[job_id]}


@app.get("/download/{job_id}")
def download_file(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    status = jobs[job_id]
    if status in ("processing", "queued"):
        raise HTTPException(status_code=202, detail="Still processing")
    if status.startswith("error"):
        raise HTTPException(status_code=500, detail=status)

    file_path = os.path.join(OUTPUT_DIR, f"sorted_{job_id}.bin")
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Output file not found")

    return FileResponse(
        path=file_path,
        filename="sorted.bin",
        media_type="application/octet-stream"
    )