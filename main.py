import os
import shutil
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from sorter.external_merge_sort import external_merge_sort

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_PATH = "input.bin"
OUTPUT_PATH = "output/sorted.bin"

# upload + sort
@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):

    with open(UPLOAD_PATH, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    steps = external_merge_sort(UPLOAD_PATH, OUTPUT_PATH)

    return JSONResponse(content=steps)

# download sorted file
@app.get("/download")
def download_file():
    if not os.path.exists(OUTPUT_PATH):
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(
        OUTPUT_PATH,
        media_type="application/octet-stream",
        filename="sorted.bin"
    )