from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
import os
import uuid
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from sorter.external_merge_sort import external_merge_sort


app = FastAPI(title="External Merge Sort Service")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

INPUT_DIR = os.path.join(BASE_DIR, "storage", "input")
OUTPUT_DIR = os.path.join(BASE_DIR, "storage", "output")

os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# lưu trạng thái job
jobs = {}
@app.get("/", response_class=HTMLResponse)
def homepage():
    index_path = os.path.join(STATIC_DIR, "index.html")
    with open(index_path, "r", encoding="utf-8") as f:
        return f.read()




# ================= RUN SORT =================
def run_sort(job_id: str, input_path: str, output_path: str):
    try:
        jobs[job_id] = "processing"
        external_merge_sort(input_path, output_path)
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

        # lưu file upload (stream, không ăn RAM)
        with open(input_path, "wb") as buffer:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                buffer.write(chunk)

        # tạo job
        jobs[job_id] = "queued"

        # chạy sort nền
        background_tasks.add_task(run_sort, job_id, input_path, output_path)

        return {
            "message": "Upload successful",
            "job_id": job_id,
            "status_url": f"/status/{job_id}",
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
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)