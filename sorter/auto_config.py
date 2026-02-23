# auto_config.py
import os
import math
import psutil


def choose_k(file_size):
    """
    chọn k dựa trên kích thước file
    """
    MB = 1024 * 1024

    if file_size < 50 * MB:
        return 6
    elif file_size < 200 * MB:
        return 8
    elif file_size < 500 * MB:
        return 12
    elif file_size < 2 * 1024 * MB:
        return 16
    else:
        return 24


def auto_tune_params(input_file, ram_ratio=0.5):
    """
    Tự động chọn block_size và k dựa vào:
    - RAM server (để tránh OOM nếu cần)
    - kích thước file (để chọn k)
    """

    # ===== 1. RAM khả dụng =====
    mem = psutil.virtual_memory()
    available_mem = int(mem.available * ram_ratio)

    # ===== 2. kích thước file =====
    file_size = os.path.getsize(input_file)
    N = file_size // 8   # số lượng double

    # ===== 3. chọn k =====
    k = choose_k(file_size)

    # ===== 4. block_size tối đa do RAM cho phép =====
    # đảm bảo (k+1) buffers có thể nằm trong mem_ratio * available RAM
    max_block = max(1, available_mem // (8 * (k + 1)))

    # ===== 5. block_size cần để đạt ~2 pass =====
    # target để giảm số pass (heuristic)
    target_block = max(1, math.ceil(N / (k * k)))

    # ===== 6. chọn block_size an toàn =====
    block_size = int(min(max_block, target_block))

    # tránh quá nhỏ (I/O rất chậm)
    MIN_BLOCK = 4096
    block_size = max(block_size, MIN_BLOCK)

    # ===== 7. ước lượng số pass (info only) =====
    runs = max(1, math.ceil(N / block_size))
    passes = math.ceil(math.log(runs, k)) if runs > 1 else 1

    return block_size, k