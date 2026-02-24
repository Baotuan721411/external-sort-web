# sorter/external_merge_sort.py
import struct
import os
import heapq
import tempfile
import shutil
from .auto_config import auto_tune_params

# Tune these for visualization vs payload size
INPUT_PREVIEW_AHEAD = 8       # peek this many values for input-buffer preview
PREVIEW_LIMIT = 120           # trimming final previews
HEAP_SNAPSHOT_LIMIT = 40      # how many heap items to include in snapshot
INPUT_BUFFER_SAMPLE = 18      # how many items to draw per buffer
INPUT_BUFFER_LOG_FREQ = 10    # log input_buffer_update every N outputs

# ── Visualize mode caps ──────────────────────────────────────────────────────
VISUALIZE_MAX_NUMBERS = 300   # sample input xuống tối đa N số khi visualize
VISUALIZE_HEAP_LOG_EVERY = 1  # log mỗi N heap ops (tăng lên để giảm steps)


class ExternalMergeSorter:
    def __init__(self, input_file, block_size=None, k=None, visualize=False):
        self.input_file = input_file
        self.block_size = block_size
        self.k = k
        self.visualize = visualize  # ← chế độ visualize

        self.work_dir = tempfile.mkdtemp(prefix="extsort_")
        self.runs = []
        self.steps = []

    def cleanup(self):
        shutil.rmtree(self.work_dir, ignore_errors=True)

    # ---------- helpers ----------
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
        pos = f.tell()
        data = f.read(8 * n)
        nums = []
        if data:
            nums = list(struct.unpack(f"{len(data)//8}d", data))
        f.seek(pos)
        return nums

    def _sample(self, arr, limit=PREVIEW_LIMIT):
        if not arr:
            return []
        n = len(arr)
        if n <= limit:
            return arr
        step = n / limit
        res = []
        i = 0.0
        while int(i) < n:
            res.append(arr[int(i)])
            i += step
        return res[:limit]

    def _sample_small(self, arr, limit=INPUT_BUFFER_SAMPLE):
        if not arr:
            return []
        return arr[:limit]

    # ---------- NEW: sample input file để visualize ----------
    def _sample_input_for_visualize(self):
        """
        Đọc tối đa VISUALIZE_MAX_NUMBERS doubles từ input,
        ghi vào file tạm, trả về path file tạm đó.
        """
        sampled = []
        with open(self.input_file, "rb") as f:
            while len(sampled) < VISUALIZE_MAX_NUMBERS:
                data = f.read(8)
                if not data:
                    break
                sampled.append(struct.unpack("d", data)[0])

        tmp_path = os.path.join(self.work_dir, "_visualize_input.bin")
        with open(tmp_path, "wb") as f:
            f.write(struct.pack(f"{len(sampled)}d", *sampled))

        return tmp_path, len(sampled)

    # ---------- phase 1: run generation ----------
    def generate_runs(self):
        self.runs = []
        run_id = 0

        try:
            size = os.path.getsize(self.input_file)
        except Exception:
            size = None

        self.steps.append({
            "type": "file_info",
            "file_name": os.path.basename(self.input_file),
            "file_size": size
        })

        with open(self.input_file, "rb") as f:
            while True:
                nums = self._read_block(f)
                if nums is None:
                    break

                self.steps.append({
                    "type": "read_block",
                    "run_index": run_id,
                    "sample": self._sample_small(nums, limit=INPUT_BUFFER_SAMPLE),
                    "count": len(nums)
                })

                nums.sort()

                run_path = os.path.join(self.work_dir, f"run_{run_id}.bin")
                with open(run_path, "wb") as out:
                    out.write(struct.pack(f"{len(nums)}d", *nums))

                self.steps.append({
                    "type": "sort_block",
                    "run_index": run_id,
                    "sorted_sample": self._sample_small(nums, limit=INPUT_BUFFER_SAMPLE),
                    "count": len(nums)
                })

                self.runs.append(run_path)
                run_id += 1

        self.steps.insert(0, {
            "type": "meta",
            "block_size": self.block_size,
            "k": self.k,
            "runs_count": len(self.runs)
        })

    # ---------- K-way merge ----------
    def merge_k_runs(self, input_runs, output_run, pass_id=None, group_id=None):
        files = [open(r, "rb") for r in input_runs]
        heap = []
        heap_op_count = 0  # ← đếm để throttle logging

        for i, f in enumerate(files):
            num = self._read_double(f)
            if num is not None:
                heapq.heappush(heap, (num, i))
                heap_op_count += 1
                # Trong visualize mode: log mỗi push ban đầu
                # Trong production mode: bỏ qua hoàn toàn
                if self.visualize:
                    self.steps.append({
                        "type": "heap_push",
                        "pass_id": pass_id,
                        "group_id": group_id,
                        "value": num,
                        "src": i,
                        "heap_snapshot": [list(x) for x in sorted(heap)[:HEAP_SNAPSHOT_LIMIT]]
                    })

        input_buffers = [self._peek_next(f, n=INPUT_PREVIEW_AHEAD) for f in files]

        self.steps.append({
            "type": "merge_start",
            "pass_id": pass_id,
            "group_id": group_id,
            "inputs": [os.path.basename(p) for p in input_runs],
            "input_buffers": [self._sample_small(b) for b in input_buffers],
            "initial_heap": [list(x) for x in sorted(heap)[:HEAP_SNAPSHOT_LIMIT]]
        })

        merged_count = 0

        with open(output_run, "wb") as out:
            while heap:
                value, src_idx = heapq.heappop(heap)
                heap_op_count += 1

                # ── heap_pop: chỉ log trong visualize mode ──
                if self.visualize and heap_op_count % VISUALIZE_HEAP_LOG_EVERY == 0:
                    self.steps.append({
                        "type": "heap_pop",
                        "pass_id": pass_id,
                        "group_id": group_id,
                        "value": value,
                        "src": src_idx,
                        "heap_snapshot": [list(x) for x in sorted(heap)[:HEAP_SNAPSHOT_LIMIT]]
                    })

                out.write(struct.pack("d", value))
                merged_count += 1

                # ── output_emit: log trong cả 2 mode nhưng throttle ──
                log_freq = 1 if self.visualize else INPUT_BUFFER_LOG_FREQ
                if merged_count % log_freq == 0:
                    self.steps.append({
                        "type": "output_emit",
                        "pass_id": pass_id,
                        "group_id": group_id,
                        "value": value,
                        "emitted_count": merged_count
                    })

                nxt = self._read_double(files[src_idx])
                if nxt is not None:
                    heapq.heappush(heap, (nxt, src_idx))
                    heap_op_count += 1
                    if self.visualize and heap_op_count % VISUALIZE_HEAP_LOG_EVERY == 0:
                        self.steps.append({
                            "type": "heap_push",
                            "pass_id": pass_id,
                            "group_id": group_id,
                            "value": nxt,
                            "src": src_idx,
                            "heap_snapshot": [list(x) for x in sorted(heap)[:HEAP_SNAPSHOT_LIMIT]]
                        })

                input_buffers[src_idx] = self._peek_next(files[src_idx], n=INPUT_PREVIEW_AHEAD)

                if merged_count % INPUT_BUFFER_LOG_FREQ == 0:
                    self.steps.append({
                        "type": "input_buffer_update",
                        "pass_id": pass_id,
                        "group_id": group_id,
                        "input_buffers": [self._sample_small(b) for b in input_buffers],
                        "consumed_count": merged_count
                    })

        for f in files:
            f.close()

        self.steps.append({
            "type": "merge_end",
            "pass_id": pass_id,
            "group_id": group_id,
            "merged_count": merged_count
        })

    # ---------- one merge pass ----------
    def merge_pass(self, runs, pass_id):
        new_runs = []
        i = 0
        group_id = 0
        while i < len(runs):
            group = runs[i:i + self.k]
            out_run = os.path.join(self.work_dir, f"pass{pass_id}_run{group_id}.bin")
            self.merge_k_runs(group, out_run, pass_id=pass_id, group_id=group_id)
            new_runs.append(out_run)
            i += self.k
            group_id += 1
        return new_runs

    # ---------- multi-pass merge ----------
    def multi_pass_merge(self):
        current = list(self.runs)
        pass_id = 0
        while len(current) > 1:
            self.steps.append({
                "type": "pass_info",
                "pass_id": pass_id,
                "runs_before": len(current),
                "k": self.k
            })
            current = self.merge_pass(current, pass_id)
            pass_id += 1
        return current[0] if current else None

    # ---------- main pipeline ----------
    def sort(self, output_file):
        try:
            if self.block_size is None or self.k is None:
                b, kk = auto_tune_params(self.input_file)
                if self.block_size is None:
                    self.block_size = b
                if self.k is None:
                    self.k = kk

            self.steps.append({
                "type": "params_chosen",
                "block_size": self.block_size,
                "k": self.k
            })

            self.generate_runs()
            final = self.multi_pass_merge()

            os.makedirs(os.path.dirname(output_file), exist_ok=True)
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
                final_preview = []

            self.steps.append({
                "type": "finished",
                "final_preview": self._sample(final_preview, limit=PREVIEW_LIMIT)
            })

        finally:
            self.cleanup()

    # ---------- NEW: visualize pipeline (sort trên sample nhỏ) ----------
    def sort_visualize(self):
        """
        Sort trên sample nhỏ (tối đa VISUALIZE_MAX_NUMBERS số).
        KHÔNG ghi output file thật.
        Trả về steps để frontend visualize.
        """
        try:
            # 1. Sample input
            tmp_input, actual_count = self._sample_input_for_visualize()
            self.input_file = tmp_input  # trỏ sorter vào file sample

            # 2. Auto-tune params cho data nhỏ
            if self.block_size is None or self.k is None:
                b, kk = auto_tune_params(self.input_file)
                # Với visualize, giới hạn block_size nhỏ để có nhiều runs hơn
                # → dễ thấy quá trình merge hơn
                if self.block_size is None:
                    self.block_size = min(b, max(10, actual_count // 4))
                if self.k is None:
                    self.k = min(kk, 4)  # k nhỏ → dễ visualize hơn

            self.steps.append({
                "type": "params_chosen",
                "block_size": self.block_size,
                "k": self.k,
                "visualize_sample_size": actual_count
            })

            # 3. Generate runs
            self.generate_runs()

            # 4. Merge passes
            current = list(self.runs)
            pass_id = 0
            while len(current) > 1:
                self.steps.append({
                    "type": "pass_info",
                    "pass_id": pass_id,
                    "runs_before": len(current),
                    "k": self.k
                })
                new_runs = []
                group_id = 0
                i = 0
                while i < len(current):
                    group = current[i:i + self.k]
                    out_run = os.path.join(self.work_dir, f"pass{pass_id}_run{group_id}.bin")
                    self.merge_k_runs(group, out_run, pass_id=pass_id, group_id=group_id)
                    new_runs.append(out_run)
                    i += self.k
                    group_id += 1
                current = new_runs
                pass_id += 1

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

            self.steps.append({
                "type": "finished",
                "final_preview": final_preview[:PREVIEW_LIMIT]
            })

        finally:
            self.cleanup()

        return self.steps

    def get_steps(self):
        return self.steps


# ---------- wrapper functions ----------

def external_merge_sort(input_path, output_path, block_size=None, k=None):
    """Sort thật — nhanh, ít log, trả file .bin"""
    sorter = ExternalMergeSorter(input_path, block_size=block_size, k=k, visualize=False)
    sorter.sort(output_path)
    return sorter.get_steps()


def external_merge_sort_visualize(input_path, block_size=None, k=None):
    """Sort để visualize — sample input nhỏ, log chi tiết, không cần output file"""
    sorter = ExternalMergeSorter(input_path, block_size=block_size, k=k, visualize=True)
    return sorter.sort_visualize()
