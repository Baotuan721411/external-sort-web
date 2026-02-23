import os
import math


def choose_k(file_size):
    """
    k cho visualisation (không phải performance)
    """

    MB = 1024 * 1024

    if file_size < 5 * MB:
        return 4
    elif file_size < 20 * MB:
        return 5
    elif file_size < 100 * MB:
        return 6
    elif file_size < 500 * MB:
        return 7
    else:
        return 8


def choose_visual_block_size(N, k):
    """
    chọn block_size để:
    - nhìn rõ buffer
    - heap thay đổi liên tục
    - không lag browser
    """

    # ta muốn khoảng 10–18 runs để animation đẹp
    target_runs = 14

    block_size = math.ceil(N / target_runs)

    # giới hạn để UI vẽ được
    MIN_BLOCK = 32
    MAX_BLOCK = 80

    block_size = max(MIN_BLOCK, min(block_size, MAX_BLOCK))

    return block_size


def auto_tune_params(input_file):
    """
    Auto config cho visualizer
    """

    file_size = os.path.getsize(input_file)
    N = file_size // 8  # số lượng double

    # chọn k
    k = choose_k(file_size)

    # block_size cho animation
    block_size = choose_visual_block_size(N, k)

    return block_size, k