# main.py
import os
import uuid
import threading
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sorter.external_merge_sort import external_merge_sort, external_merge_sort_visualize

app = FastAPI(title="External Merge Sort Service")

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")
INPUT_DIR  = os.path.join(BASE_DIR, "storage", "input")
OUTPUT_DIR = os.path.join(BASE_DIR, "storage", "output")

os.makedirs(INPUT_DIR,  exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(STATIC_DIR, exist_ok=True)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

jobs = {}  # job_id -> "queued" | "processing" | "done" | "error: ..."


# ── Homepage ──────────────────────────────────────────────────
@app.get("/")
def homepage():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


# ── Background sort ───────────────────────────────────────────
def run_sort(job_id: str, input_path: str, output_path: str):
    try:
        jobs[job_id] = "processing"
        external_merge_sort(input_path, output_path)
        jobs[job_id] = "done"
    except Exception as e:
        jobs[job_id] = f"error: {str(e)}"
    finally:
        # Xóa input sau khi sort xong (dù thành công hay lỗi)
        try:
            os.remove(input_path)
        except Exception:
            pass


# ── /sort/visualize ───────────────────────────────────────────
@app.post("/sort/visualize")
async def sort_visualize(file: UploadFile = File(...)):
    """
    Upload file 1 lần duy nhất.
    1. Lưu file vào disk
    2. Spawn background thread sort thật
    3. Chạy visualize ngay (sample 300 số, ~nhanh)
    4. Trả về steps JSON + job_id để frontend poll download
    """
    job_id      = str(uuid.uuid4())
    input_path  = os.path.join(INPUT_DIR,  f"{job_id}_{file.filename}")
    output_path = os.path.join(OUTPUT_DIR, f"sorted_{job_id}.bin")

    # 1. Lưu file
    try:
        with open(input_path, "wb") as buf:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                buf.write(chunk)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {e}")

    # 2. Sort thật ở background — đọc file gốc, không bị ảnh hưởng bởi visualize
    jobs[job_id] = "queued"
    threading.Thread(
        target=run_sort,
        args=(job_id, input_path, output_path),
        daemon=True
    ).start()

    # 3. Visualize ngay — dùng viz_work_dir riêng, không đụng file gốc
    try:
        steps = external_merge_sort_visualize(input_path)
    except Exception as e:
        # Visualize lỗi không cancel sort thật
        raise HTTPException(status_code=500, detail=f"Visualize error: {e}")

    # 4. Trả về
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


# ── /sort (backward compat) ───────────────────────────────────
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
    threading.Thread(
        target=run_sort,
        args=(job_id, input_path, output_path),
        daemon=True
    ).start()

    return {"job_id": job_id,
            "status_url":   f"/status/{job_id}",
            "download_url": f"/download/{job_id}"}


# ── Status ────────────────────────────────────────────────────
@app.get("/status/{job_id}")
def check_status(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"job_id": job_id, "status": jobs[job_id]}


# ── Download ──────────────────────────────────────────────────
@app.get("/download/{job_id}")
def download_file(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    status = jobs[job_id]
    if status in ("queued", "processing"):
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