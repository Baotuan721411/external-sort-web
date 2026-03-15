# External Sort App

Ứng dụng sắp xếp bên ngoài (External Merge Sort) sử dụng FastAPI.

## Cách sử dụng

### 1. Clone repository từ GitHub

```bash
git clone https://github.com/Baotuan721411/external-sort-web.git
cd external_sort_app
```


### 2. Cài đặt dependencies

```bash
pip install -r requirements.txt
```

### 3. Chạy ứng dụng ở local

```bash
py -m uvicorn main:app --reload
```

Ứng dụng sẽ chạy trên `http://127.0.0.1:8000`.

### 4. Truy cập ứng dụng

Mở trình duyệt và truy cập `http://127.0.0.1:8000` để sử dụng giao diện web.

## Tính năng

- Upload file để sắp xếp
- Sắp xếp bên ngoài với merge sort
- Hiển thị tiến trình và kết quả