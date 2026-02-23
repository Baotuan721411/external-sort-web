# sorter/external_merge_sort.py
import struct
import os
import heapq
import tempfile
import shutil
from .auto_config import auto_tune_params

# constants for visualization sampling
PREVIEW_LIMIT = 60       # sample size for run previews
MERGED_VISUAL_EVERY = 50 # take one merged value per N outputs for visual snapshots
INPUT_PREVIEW_AHEAD = 8  # how many upcoming values to peek for each input file in merge


class ExternalMergeSorter:

    def __init__(self, input_file, block_size=None, k=None):
        self.input_file = input_file
        self.block_size = block_size
        self.k = k

        self.work_dir = tempfile.mkdtemp(prefix="extsort_")
        self.runs = []  # list of run file paths (not grouped by bucket here; grouping later)
        self.steps = []  # list of step dicts (ordered)

    def cleanup(self):
        shutil.rmtree(self.work_dir, ignore_errors=True)

    # ---------- helpers ----------
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

    def _read_block(self, f):
        chunk = f.read(8 * self.block_size)
        if not chunk:
            return None
        return list(struct.unpack(f"{len(chunk) // 8}d", chunk))

    def _read_double(self, f):
        data = f.read(8)
        if not data:
            return None
        return struct.unpack("d", data)[0]

    # peek next N doubles from current position, then seek back
    def _peek_next(self, f, n=INPUT_PREVIEW_AHEAD):
        pos = f.tell()
        data = f.read(8 * n)
        nums = []
        if data:
            nums = list(struct.unpack(f"{len(data) // 8}d", data))
        f.seek(pos)
        return nums

    # ---------- run generation (phase 1) ----------
    def generate_runs(self):
        # read input file by blocks, sort each block and write a run file
        self.runs = []
        run_id = 0

        with open(self.input_file, "rb") as f:
            while True:
                nums = self._read_block(f)
                if nums is None:
                    break

                # log read_block (raw-ish sample)
                self.steps.append({
                    "type": "read_block",
                    "run_index": run_id,
                    "sample": self._sample(nums, limit=40),  # smaller preview
                    "count": len(nums)
                })

                nums.sort()

                run_path = os.path.join(self.work_dir, f"run_{run_id}.bin")
                with open(run_path, "wb") as out:
                    out.write(struct.pack(f"{len(nums)}d", *nums))

                # after writing run, record sorted preview
                self.steps.append({
                    "type": "sort_block",
                    "run_index": run_id,
                    "sorted_sample": self._sample(nums, limit=40),
                    "count": len(nums)
                })

                self.runs.append(run_path)
                run_id += 1

        # meta step: runs created
        self.steps.insert(0, {
            "type": "file_info",
            "file_name": os.path.basename(self.input_file),
            "runs_count": len(self.runs),
            "suggested_k": self.k or None,
            "suggested_block_size": self.block_size or None
        })

    # ---------- k-way merge with rich visual steps ----------
    def merge_k_runs(self, input_runs, output_run, pass_id=None, group_id=None):
        # open files
        files = [open(p, "rb") for p in input_runs]
        heap = []

        # read first element from each file and push to heap
        for i, f in enumerate(files):
            num = self._read_double(f)
            if num is not None:
                heapq.heappush(heap, (num, i))

        # capture initial input buffer previews
        input_buffers = []
        for f in files:
            input_buffers.append(self._peek_next(f, n=INPUT_PREVIEW_AHEAD))

        # record merge start
        self.steps.append({
            "type": "merge_start",
            "pass_id": pass_id,
            "group_id": group_id,
            "inputs": [os.path.basename(p) for p in input_runs],
            "initial_heap": [ { "value": v, "src": idx } for v, idx in heap ],
            "input_buffers": input_buffers
        })

        merged_output_preview = []
        counter = 0

        with open(output_run, "wb") as out:
            while heap:
                value, src_idx = heapq.heappop(heap)
                out.write(struct.pack("d", value))

                # occasionally append to merged preview for visualization
                if counter % MERGED_VISUAL_EVERY == 0:
                    merged_output_preview.append(value)

                counter += 1

                # read next from the source file (this advances that file)
                nxt = self._read_double(files[src_idx])
                if nxt is not None:
                    heapq.heappush(heap, (nxt, src_idx))

                # update preview for that source (peek ahead without consuming)
                input_buffers[src_idx] = self._peek_next(files[src_idx], n=INPUT_PREVIEW_AHEAD)

                # periodically record heap snapshot (don't do it every single pop to avoid huge step list)
                if counter % (MERGED_VISUAL_EVERY // 2 or 1) == 0:
                    # snapshot the heap (take small sample sorted view)
                    heap_snapshot = sorted(heap)[:40]  # smallest items in heap (for display)
                    self.steps.append({
                        "type": "heap_snapshot",
                        "pass_id": pass_id,
                        "group_id": group_id,
                        "heap": [ {"value": v, "src": idx} for v, idx in heap_snapshot ],
                        "input_buffers": [ self._sample(b, limit=10) for b in input_buffers ],
                        "output_buffer_sample": self._sample(merged_output_preview, limit=30),
                        "consumed_count": counter
                    })

        # close files
        for f in files:
            f.close()

        # merge end step
        self.steps.append({
            "type": "merge_end",
            "pass_id": pass_id,
            "group_id": group_id,
            "output_sample": self._sample(merged_output_preview, limit=60),
            "merged_count_est": counter
        })

    def merge_pass(self, runs, pass_id, k):
        new_runs = []
        i = 0
        group_id = 0

        while i < len(runs):
            group = runs[i:i + k]
            out_run = os.path.join(self.work_dir, f"pass{pass_id}_run{group_id}.bin")
            self.merge_k_runs(group, out_run, pass_id=pass_id, group_id=group_id)
            new_runs.append(out_run)
            i += k
            group_id += 1

        return new_runs

    def multi_pass_merge(self, k):
        current = list(self.runs)
        pass_id = 0
        while len(current) > 1:
            self.steps.append({
                "type": "pass_info",
                "pass_id": pass_id,
                "runs_before": len(current),
                "k": k
            })
            current = self.merge_pass(current, pass_id, k)
            pass_id += 1

        return current[0] if current else None

    # ---------- main pipeline ----------
    def sort(self, output_file):
        try:
            # if block_size or k not provided, auto tune
            if self.block_size is None or self.k is None:
                b, kk = auto_tune_params(self.input_file)
                if self.block_size is None:
                    self.block_size = b
                if self.k is None:
                    self.k = kk

            # log chosen params for visualization
            self.steps.append({
                "type": "params_chosen",
                "block_size": self.block_size,
                "k": self.k
            })

            # generate run files
            self.generate_runs()

            # perform multi-pass k-way merge
            final = self.multi_pass_merge(self.k)
            if final is None:
                # nothing to sort -> create empty output
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                open(output_file, "wb").close()
            else:
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                if os.path.exists(output_file):
                    os.remove(output_file)
                shutil.move(final, output_file)

            # final preview
            preview = []
            try:
                with open(output_file, "rb") as f:
                    for _ in range(120):
                        data = f.read(8)
                        if not data:
                            break
                        preview.append(struct.unpack("d", data)[0])
            except Exception:
                preview = []

            self.steps.append({
                "type": "finished",
                "final_preview": self._sample(preview, limit=120)
            })

        finally:
            # note: steps are already collected — cleanup files
            self.cleanup()

    def get_steps(self):
        return self.steps


# wrapper used by main server - accepts optional block_size and k
def external_merge_sort(input_path, output_path, block_size=None, k=None):
    if block_size is None or k is None:
        # auto_tune_params may return sensible defaults if None passed
        suggested_block, suggested_k = auto_tune_params(input_path)
        if block_size is None:
            block_size = suggested_block
        if k is None:
            k = suggested_k

    sorter = ExternalMergeSorter(input_path, block_size=block_size, k=k)
    sorter.sort(output_path)
    return sorter.get_steps()