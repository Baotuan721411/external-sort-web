import os
import math
import psutil


def choose_k(available_mem):
    """
    chọn k dựa trên RAM server
    không nên quá lớn vì heap + file handle sẽ chậm
    """

    MB = 1024 * 1024

    if available_mem < 256 * MB:
        return 8
    elif available_mem < 512 * MB:
        return 16
    elif available_mem < 2 * 1024 * MB:
        return 32
    elif available_mem < 8 * 1024 * MB:
        return 64
    else:
        return 96


def auto_tune_params(input_file, ram_ratio=0.5):
    """
    Tự động chọn block_size và k dựa vào:
    - RAM server
    - kích thước file
    """

    # ===== 1. RAM khả dụng =====
    mem = psutil.virtual_memory()
    available_mem = int(mem.available * ram_ratio)

    # ===== 2. kích thước file =====
    file_size = os.path.getsize(input_file)
    N = file_size // 8   # số lượng double

    # ===== 3. chọn k =====
    k = choose_k(available_mem)

    # ===== 4. block_size tối đa do RAM cho phép =====
    max_block = available_mem // (8 * (k + 1))

    # ===== 5. block_size cần để đạt ~2 pass =====
    target_block = math.ceil(N / (k * k))

    # ===== 6. chọn block_size an toàn =====
    block_size = min(max_block, target_block)

    # tránh quá nhỏ (I/O rất chậm)
    MIN_BLOCK = 4096
    block_size = max(block_size, MIN_BLOCK)

    # ===== 7. ước lượng số pass =====
    runs = max(1, math.ceil(N / block_size))
    passes = math.ceil(math.log(runs, k)) if runs > 1 else 1

    print("------ AUTO CONFIG ------")
    print(f"File size: {file_size/1024/1024:.2f} MB")
    print(f"Available RAM: {available_mem/1024/1024:.2f} MB")
    print(f"k-way merge: {k}")
    print(f"block_size: {block_size} doubles ({block_size*8/1024/1024:.2f} MB)")
    print(f"Initial runs: {runs}")
    print(f"Estimated passes: {passes}")
    print("-------------------------")

    return block_size, k
 