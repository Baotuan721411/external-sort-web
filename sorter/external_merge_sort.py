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


class ExternalMergeSorter:
    def __init__(self, input_file, block_size=None, k=None):
        self.input_file = input_file
        self.block_size = block_size
        self.k = k

        self.work_dir = tempfile.mkdtemp(prefix="extsort_")
        self.runs = []  # list of run file paths
        self.steps = []  # ordered list of event dicts for frontend

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

    # ---------- phase 1: run generation ----------
    def generate_runs(self):
        self.runs = []
        run_id = 0

        # log file_info early for frontend
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

                # read_block event (shows what was read before sorting)
                self.steps.append({
                    "type": "read_block",
                    "run_index": run_id,
                    "sample": self._sample_small(nums, limit=INPUT_BUFFER_SAMPLE),
                    "count": len(nums)
                })

                nums.sort()

                # write run file
                run_path = os.path.join(self.work_dir, f"run_{run_id}.bin")
                with open(run_path, "wb") as out:
                    out.write(struct.pack(f"{len(nums)}d", *nums))

                # sort_block event (sorted preview)
                self.steps.append({
                    "type": "sort_block",
                    "run_index": run_id,
                    "sorted_sample": self._sample_small(nums, limit=INPUT_BUFFER_SAMPLE),
                    "count": len(nums)
                })

                self.runs.append(run_path)
                run_id += 1

        # meta step (first real meta visible to frontend)
        self.steps.insert(0, {
            "type": "meta",
            "block_size": self.block_size,
            "k": self.k,
            "runs_count": len(self.runs)
        })

    # ---------- K-way merge (with detailed events) ----------
    def merge_k_runs(self, input_runs, output_run, pass_id=None, group_id=None):
        files = [open(r, "rb") for r in input_runs]
        heap = []

        # init: read first element of each file => push to heap
        for i, f in enumerate(files):
            num = self._read_double(f)
            if num is not None:
                heapq.heappush(heap, (num, i))
                # log initial push
                self.steps.append({
                    "type": "heap_push",
                    "pass_id": pass_id,
                    "group_id": group_id,
                    "value": num,
                    "src": i,
                    "heap_snapshot": [list(x) for x in sorted(heap)[:HEAP_SNAPSHOT_LIMIT]]
                })

        # initial input buffers preview (peek)
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
                # pop smallest
                value, src_idx = heapq.heappop(heap)
                # log pop
                self.steps.append({
                    "type": "heap_pop",
                    "pass_id": pass_id,
                    "group_id": group_id,
                    "value": value,
                    "src": src_idx,
                    "heap_snapshot": [list(x) for x in sorted(heap)[:HEAP_SNAPSHOT_LIMIT]]
                })

                # write to output
                out.write(struct.pack("d", value))
                merged_count += 1

                # log output emit
                self.steps.append({
                    "type": "output_emit",
                    "pass_id": pass_id,
                    "group_id": group_id,
                    "value": value,
                    "emitted_count": merged_count
                })

                # read next from the same source file (advances that file)
                nxt = self._read_double(files[src_idx])
                if nxt is not None:
                    heapq.heappush(heap, (nxt, src_idx))
                    self.steps.append({
                        "type": "heap_push",
                        "pass_id": pass_id,
                        "group_id": group_id,
                        "value": nxt,
                        "src": src_idx,
                        "heap_snapshot": [list(x) for x in sorted(heap)[:HEAP_SNAPSHOT_LIMIT]]
                    })

                # update input buffer preview for that source (peek)
                input_buffers[src_idx] = self._peek_next(files[src_idx], n=INPUT_PREVIEW_AHEAD)

                # periodically emit input_buffer_update to reduce event rate
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

        # merge end
        self.steps.append({
            "type": "merge_end",
            "pass_id": pass_id,
            "group_id": group_id,
            "merged_count": merged_count
        })

    # ---------- one merge pass (grouping by k) ----------
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
            # record pass info
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
            # if params not set, auto tune
            if self.block_size is None or self.k is None:
                b, kk = auto_tune_params(self.input_file)
                if self.block_size is None:
                    self.block_size = b
                if self.k is None:
                    self.k = kk

            # record chosen params
            self.steps.append({
                "type": "params_chosen",
                "block_size": self.block_size,
                "k": self.k
            })

            # generate runs
            self.generate_runs()

            # merges
            final = self.multi_pass_merge()

            # move final run to output_file
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            if final is None:
                open(output_file, "wb").close()
            else:
                if os.path.exists(output_file):
                    os.remove(output_file)
                shutil.move(final, output_file)

            # final preview (read first PREVIEW_LIMIT doubles)
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
            # cleanup run files
            self.cleanup()

    def get_steps(self):
        # return the steps collected
        return self.steps


# wrapper function used by server
def external_merge_sort(input_path, output_path, block_size=None, k=None):
    sorter = ExternalMergeSorter(input_path, block_size=block_size, k=k)
    sorter.sort(output_path)
    return sorter.get_steps()