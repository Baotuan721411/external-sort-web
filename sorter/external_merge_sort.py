# sorter/external_merge_sort.py
import struct
import os
import heapq
import tempfile
import shutil
from .auto_config import auto_tune_params

INPUT_PREVIEW_AHEAD   = 8
PREVIEW_LIMIT         = 120
HEAP_SNAPSHOT_LIMIT   = 40
INPUT_BUFFER_SAMPLE   = 18
INPUT_BUFFER_LOG_FREQ = 10

VISUALIZE_MAX_NUMBERS  = 300
VISUALIZE_HEAP_LOG_EVERY = 1


class ExternalMergeSorter:
    def __init__(self, input_file, block_size=None, k=None, visualize=False):
        self.input_file = input_file
        self.block_size = block_size
        self.k          = k
        self.visualize  = visualize
        self.work_dir   = tempfile.mkdtemp(prefix="extsort_")
        self.runs       = []
        self.steps      = []

    def cleanup(self):
        shutil.rmtree(self.work_dir, ignore_errors=True)

    # ── helpers ───────────────────────────────────────────────
    def _read_block(self, f):
        chunk = f.read(8 * self.block_size)
        if not chunk:
            return None
        return list(struct.unpack(f"{len(chunk)//8}d", chunk))

    def _read_double(self, f):
        data = f.read(8)
        if not data:
            return None
        return struct.unpack("d", data)[0]

    def _peek_next(self, f, n=INPUT_PREVIEW_AHEAD):
        pos  = f.tell()
        data = f.read(8 * n)
        nums = list(struct.unpack(f"{len(data)//8}d", data)) if data else []
        f.seek(pos)
        return nums

    def _sample(self, arr, limit=PREVIEW_LIMIT):
        if not arr:
            return []
        n = len(arr)
        if n <= limit:
            return arr
        step = n / limit
        res, i = [], 0.0
        while int(i) < n:
            res.append(arr[int(i)])
            i += step
        return res[:limit]

    def _sample_small(self, arr, limit=INPUT_BUFFER_SAMPLE):
        return arr[:limit] if arr else []

    # ── phase 1: run generation ───────────────────────────────
    def generate_runs(self):
        self.runs  = []
        run_id     = 0
        try:
            size = os.path.getsize(self.input_file)
        except Exception:
            size = None

        self.steps.append({"type": "file_info",
                            "file_name": os.path.basename(self.input_file),
                            "file_size": size})

        with open(self.input_file, "rb") as f:
            while True:
                nums = self._read_block(f)
                if nums is None:
                    break
                self.steps.append({"type": "read_block", "run_index": run_id,
                                    "sample": self._sample_small(nums), "count": len(nums)})
                nums.sort()
                run_path = os.path.join(self.work_dir, f"run_{run_id}.bin")
                with open(run_path, "wb") as out:
                    out.write(struct.pack(f"{len(nums)}d", *nums))
                self.steps.append({"type": "sort_block", "run_index": run_id,
                                    "sorted_sample": self._sample_small(nums), "count": len(nums)})
                self.runs.append(run_path)
                run_id += 1

        self.steps.insert(0, {"type": "meta", "block_size": self.block_size,
                               "k": self.k, "runs_count": len(self.runs)})

    # ── K-way merge ───────────────────────────────────────────
    def merge_k_runs(self, input_runs, output_run, pass_id=None, group_id=None):
        files         = [open(r, "rb") for r in input_runs]
        heap          = []
        heap_op_count = 0

        for i, f in enumerate(files):
            num = self._read_double(f)
            if num is not None:
                heapq.heappush(heap, (num, i))
                heap_op_count += 1
                if self.visualize:
                    self.steps.append({"type": "heap_push", "pass_id": pass_id,
                                        "group_id": group_id, "value": num, "src": i,
                                        "heap_snapshot": [list(x) for x in sorted(heap)[:HEAP_SNAPSHOT_LIMIT]]})

        input_buffers = [self._peek_next(f) for f in files]
        self.steps.append({"type": "merge_start", "pass_id": pass_id, "group_id": group_id,
                            "inputs": [os.path.basename(p) for p in input_runs],
                            "input_buffers": [self._sample_small(b) for b in input_buffers],
                            "initial_heap": [list(x) for x in sorted(heap)[:HEAP_SNAPSHOT_LIMIT]]})

        merged_count = 0
        with open(output_run, "wb") as out:
            while heap:
                value, src_idx = heapq.heappop(heap)
                heap_op_count += 1

                if self.visualize and heap_op_count % VISUALIZE_HEAP_LOG_EVERY == 0:
                    self.steps.append({"type": "heap_pop", "pass_id": pass_id,
                                        "group_id": group_id, "value": value, "src": src_idx,
                                        "heap_snapshot": [list(x) for x in sorted(heap)[:HEAP_SNAPSHOT_LIMIT]]})

                out.write(struct.pack("d", value))
                merged_count += 1

                log_freq = 1 if self.visualize else INPUT_BUFFER_LOG_FREQ
                if merged_count % log_freq == 0:
                    self.steps.append({"type": "output_emit", "pass_id": pass_id,
                                        "group_id": group_id, "value": value,
                                        "emitted_count": merged_count})

                nxt = self._read_double(files[src_idx])
                if nxt is not None:
                    heapq.heappush(heap, (nxt, src_idx))
                    heap_op_count += 1
                    if self.visualize and heap_op_count % VISUALIZE_HEAP_LOG_EVERY == 0:
                        self.steps.append({"type": "heap_push", "pass_id": pass_id,
                                            "group_id": group_id, "value": nxt, "src": src_idx,
                                            "heap_snapshot": [list(x) for x in sorted(heap)[:HEAP_SNAPSHOT_LIMIT]]})

                input_buffers[src_idx] = self._peek_next(files[src_idx])
                if merged_count % INPUT_BUFFER_LOG_FREQ == 0:
                    self.steps.append({"type": "input_buffer_update", "pass_id": pass_id,
                                        "group_id": group_id,
                                        "input_buffers": [self._sample_small(b) for b in input_buffers],
                                        "consumed_count": merged_count})

        for f in files:
            f.close()
        self.steps.append({"type": "merge_end", "pass_id": pass_id,
                            "group_id": group_id, "merged_count": merged_count})

    # ── merge pass ────────────────────────────────────────────
    def merge_pass(self, runs, pass_id):
        new_runs, i, group_id = [], 0, 0
        while i < len(runs):
            group   = runs[i:i + self.k]
            out_run = os.path.join(self.work_dir, f"pass{pass_id}_run{group_id}.bin")
            self.merge_k_runs(group, out_run, pass_id=pass_id, group_id=group_id)
            new_runs.append(out_run)
            i        += self.k
            group_id += 1
        return new_runs

    # ── multi-pass ────────────────────────────────────────────
    def multi_pass_merge(self):
        current = list(self.runs)
        pass_id = 0
        while len(current) > 1:
            self.steps.append({"type": "pass_info", "pass_id": pass_id,
                                "runs_before": len(current), "k": self.k})
            current = self.merge_pass(current, pass_id)
            pass_id += 1
        return current[0] if current else None

    # ── sort thật ─────────────────────────────────────────────
    def sort(self, output_file):
        try:
            if self.block_size is None or self.k is None:
                b, kk = auto_tune_params(self.input_file)
                if self.block_size is None:
                    self.block_size = b
                if self.k is None:
                    self.k = kk

            self.steps.append({"type": "params_chosen",
                                "block_size": self.block_size, "k": self.k})
            self.generate_runs()
            final = self.multi_pass_merge()

            out_dir = os.path.dirname(output_file)
            if out_dir:
                os.makedirs(out_dir, exist_ok=True)

            if final is None:
                open(output_file, "wb").close()
            else:
                if os.path.exists(output_file):
                    os.remove(output_file)
                shutil.move(final, output_file)

            final_preview = []
            try:
                with open(output_file, "rb") as f:
                    for _ in range(PREVIEW_LIMIT):
                        data = f.read(8)
                        if not data:
                            break
                        final_preview.append(struct.unpack("d", data)[0])
            except Exception:
                pass

            self.steps.append({"type": "finished",
                                "final_preview": self._sample(final_preview)})
        finally:
            self.cleanup()

    # ── sort visualize (HOÀN TOÀN TÁCH BIỆT với sort thật) ───
    def sort_visualize(self):
        """
        - Dùng viz_work_dir RIÊNG, không đụng self.work_dir
        - KHÔNG đè self.input_file → run_sort vẫn dùng được file gốc
        - finally chỉ xóa viz_work_dir, không ảnh hưởng sort thật
        """
        viz_work_dir = tempfile.mkdtemp(prefix="extsort_viz_")
        try:
            # 1. Sample tối đa VISUALIZE_MAX_NUMBERS số từ file gốc
            sampled = []
            with open(self.input_file, "rb") as f:
                while len(sampled) < VISUALIZE_MAX_NUMBERS:
                    data = f.read(8)
                    if not data:
                        break
                    sampled.append(struct.unpack("d", data)[0])
            actual_count = len(sampled)

            viz_input = os.path.join(viz_work_dir, "_sample.bin")
            with open(viz_input, "wb") as f:
                f.write(struct.pack(f"{actual_count}d", *sampled))

            # 2. Params: dựa trên file GỐC (kích thước thật) nhưng giới hạn cho visualize
            b, kk      = auto_tune_params(self.input_file)
            viz_block  = self.block_size if self.block_size else min(b, max(10, actual_count // 4))
            viz_k      = self.k          if self.k          else min(kk, 4)

            self.steps.append({"type": "params_chosen",
                                "block_size": viz_block, "k": viz_k,
                                "visualize_sample_size": actual_count,
                                "original_file_size": os.path.getsize(self.input_file)})

            # 3. Generate runs từ sample — tất cả trong viz_work_dir
            runs   = []
            run_id = 0
            self.steps.append({"type": "file_info",
                                "file_name": os.path.basename(self.input_file),
                                "file_size": os.path.getsize(self.input_file)})

            with open(viz_input, "rb") as f:
                while True:
                    chunk = f.read(8 * viz_block)
                    if not chunk:
                        break
                    nums = list(struct.unpack(f"{len(chunk)//8}d", chunk))
                    self.steps.append({"type": "read_block", "run_index": run_id,
                                        "sample": self._sample_small(nums), "count": len(nums)})
                    nums.sort()
                    run_path = os.path.join(viz_work_dir, f"run_{run_id}.bin")
                    with open(run_path, "wb") as out:
                        out.write(struct.pack(f"{len(nums)}d", *nums))
                    self.steps.append({"type": "sort_block", "run_index": run_id,
                                        "sorted_sample": self._sample_small(nums), "count": len(nums)})
                    runs.append(run_path)
                    run_id += 1

            self.steps.insert(0, {"type": "meta", "block_size": viz_block,
                                   "k": viz_k, "runs_count": len(runs)})

            # 4. Merge passes — hoàn toàn trong viz_work_dir
            current = list(runs)
            pass_id = 0
            # lưu k gốc, tạm dùng viz_k
            orig_k, self.k = self.k, viz_k
            while len(current) > 1:
                self.steps.append({"type": "pass_info", "pass_id": pass_id,
                                    "runs_before": len(current), "k": viz_k})
                new_runs, group_id, i = [], 0, 0
                while i < len(current):
                    group   = current[i:i + viz_k]
                    out_run = os.path.join(viz_work_dir, f"pass{pass_id}_run{group_id}.bin")
                    self.merge_k_runs(group, out_run, pass_id=pass_id, group_id=group_id)
                    new_runs.append(out_run)
                    i        += viz_k
                    group_id += 1
                current = new_runs
                pass_id += 1
            self.k = orig_k  # khôi phục k gốc

            # 5. Final preview
            final_preview = []
            if current:
                try:
                    with open(current[0], "rb") as f:
                        while True:
                            data = f.read(8)
                            if not data:
                                break
                            final_preview.append(struct.unpack("d", data)[0])
                except Exception:
                    pass
            self.steps.append({"type": "finished",
                                "final_preview": final_preview[:PREVIEW_LIMIT]})

        finally:
            # CHỈ xóa viz_work_dir — file gốc và self.work_dir không bị đụng
            shutil.rmtree(viz_work_dir, ignore_errors=True)

        return self.steps

    def get_steps(self):
        return self.steps


# ── wrapper functions ──────────────────────────────────────────
def external_merge_sort(input_path, output_path, block_size=None, k=None):
    """Sort thật — trả file .bin"""
    sorter = ExternalMergeSorter(input_path, block_size=block_size, k=k, visualize=False)
    sorter.sort(output_path)
    return sorter.get_steps()


def external_merge_sort_visualize(input_path, block_size=None, k=None):
    """Sort visualize — sample nhỏ, log chi tiết, không output file"""
    sorter = ExternalMergeSorter(input_path, block_size=block_size, k=k, visualize=True)
    return sorter.sort_visualize()