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
INPUT_DIR = os.path.join(BASE_DIR, "storage", "input")
OUTPUT_DIR = os.path.join(BASE_DIR, "storage", "output")

os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(STATIC_DIR, exist_ok=True)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# in-memory job tracking
jobs = {}           # job_id -> status string
steps_storage = {}  # job_id -> steps list (chỉ giữ tạm, xóa sau khi ghi file)


@app.get("/")
def homepage():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


# ──────────────────────────────────────────────────────────────
# SORT THẬT (giữ nguyên như cũ, chạy background)
# ──────────────────────────────────────────────────────────────

def run_sort(job_id: str, input_path: str, output_path: str):
    try:
        jobs[job_id] = "processing"
        steps = external_merge_sort(input_path, output_path)

        # ghi steps ra file
        steps_file = os.path.join(OUTPUT_DIR, f"steps_{job_id}.json")
        with open(steps_file, "w", encoding="utf-8") as f:
            json.dump({"steps": steps}, f)

        # FIX: không giữ steps trong RAM sau khi đã ghi file
        # steps_storage chỉ dùng để trả về nhanh nếu vẫn còn trong session
        # xóa sau 5 phút (đơn giản: xóa luôn ở đây, đọc từ file khi cần)
        if job_id in steps_storage:
            del steps_storage[job_id]

        # cleanup input file sau khi sort xong
        try:
            os.remove(input_path)
        except Exception:
            pass

        jobs[job_id] = "done"
    except Exception as e:
        jobs[job_id] = f"error: {str(e)}"


@app.post("/sort")
async def sort_file(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    """
    Upload file .bin (doubles) và sort thật sự.
    Trả job_id để poll status và download kết quả.
    """
    job_id = str(uuid.uuid4())
    input_filename = f"{job_id}_{file.filename}"
    output_filename = f"sorted_{job_id}.bin"
    input_path = os.path.join(INPUT_DIR, input_filename)
    output_path = os.path.join(OUTPUT_DIR, output_filename)

    with open(input_path, "wb") as buffer:
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break
            buffer.write(chunk)

    jobs[job_id] = "queued"
    t = threading.Thread(target=run_sort, args=(job_id, input_path, output_path), daemon=True)
    t.start()

    return {
        "job_id": job_id,
        "status_url": f"/status/{job_id}",
        "steps_url": f"/steps/{job_id}",
        "download_url": f"/download/{job_id}"
    }


@app.get("/status/{job_id}")
def check_status(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"job_id": job_id, "status": jobs[job_id]}


@app.get("/steps/{job_id}")
def get_steps(job_id: str):
    # đọc từ file (không giữ trong RAM nữa)
    steps_file = os.path.join(OUTPUT_DIR, f"steps_{job_id}.json")
    if os.path.exists(steps_file):
        with open(steps_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        steps = data["steps"]
        meta = steps[0] if steps and isinstance(steps[0], dict) and steps[0].get("type") == "meta" else None
        return JSONResponse(content={"steps": steps, "meta": meta})

    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    if jobs[job_id] == "processing":
        raise HTTPException(status_code=202, detail="Still processing")

    raise HTTPException(status_code=404, detail="Steps not ready")


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


# ──────────────────────────────────────────────────────────────
# SORT VISUALIZE (mới) — trả steps ngay, không cần background job
# ──────────────────────────────────────────────────────────────

@app.post("/sort/visualize")
async def sort_visualize(file: UploadFile = File(...)):
    """
    Upload file .bin, tự động sample xuống tối đa 300 số,
    chạy sort trên sample đó và trả về toàn bộ steps JSON ngay lập tức.

    Dùng cho frontend visualizer — không cần poll status.
    Response: { steps: [...], meta: {...}, sample_size: N }
    """
    # lưu file tạm
    tmp_id = str(uuid.uuid4())
    tmp_input = os.path.join(INPUT_DIR, f"viz_{tmp_id}.bin")

    try:
        with open(tmp_input, "wb") as buffer:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                buffer.write(chunk)

        # chạy sort visualize (sample + sort + sinh steps)
        steps = external_merge_sort_visualize(tmp_input)

        # tìm meta
        meta = next((s for s in steps if s.get("type") == "meta"), None)
        params = next((s for s in steps if s.get("type") == "params_chosen"), None)
        sample_size = params.get("visualize_sample_size") if params else None

        return JSONResponse(content={
            "steps": steps,
            "meta": meta,
            "sample_size": sample_size,
            "total_steps": len(steps)
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Visualize error: {str(e)}")

    finally:
        # cleanup file tạm
        try:
            os.remove(tmp_input)
        except Exception:
            pass
