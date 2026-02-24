# sorter/external_merge_sort.py
import struct
import os
import heapq
import tempfile
import shutil
from .auto_config import auto_tune_params


# ══════════════════════════════════════════════════════════════
#  SORT THẬT — giữ nguyên 100% logic gốc của bạn, không thay đổi
# ══════════════════════════════════════════════════════════════

class ExternalMergeSorter:

    def __init__(self, input_file, block_size=100, k=5):
        self.input_file = input_file
        self.block_size = block_size
        self.k = k
        self.work_dir = tempfile.mkdtemp(prefix="extsort_")
        self.runs = [[] for _ in range(k)]

    def cleanup(self):
        shutil.rmtree(self.work_dir, ignore_errors=True)

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

    def generate_runs(self):
        self.runs = [[] for _ in range(self.k)]
        with open(self.input_file, "rb") as f:
            run_id = 0
            while True:
                nums = self._read_block(f)
                if nums is None:
                    break
                nums.sort()
                run_name = os.path.join(self.work_dir, f"run{run_id}.bin")
                with open(run_name, "wb") as out:
                    out.write(struct.pack(f"{len(nums)}d", *nums))
                self.runs[run_id % self.k].append(run_name)
                run_id += 1

    def merge_k_runs(self, input_runs, output_run):
        files = [open(r, "rb") for r in input_runs]
        heap  = []
        for i, f in enumerate(files):
            num = self._read_double(f)
            if num is not None:
                heapq.heappush(heap, (num, i))
        with open(output_run, "wb") as out:
            while heap:
                value, idx = heapq.heappop(heap)
                out.write(struct.pack("d", value))
                nxt = self._read_double(files[idx])
                if nxt is not None:
                    heapq.heappush(heap, (nxt, idx))
        for f in files:
            f.close()

    def merge_pass(self, runs, pass_id):
        new_runs, i, group_id = [], 0, 0
        while i < len(runs):
            group   = runs[i:i + self.k]
            new_run = os.path.join(self.work_dir, f"pass{pass_id}_run{group_id}.bin")
            self.merge_k_runs(group, new_run)
            new_runs.append(new_run)
            i        += self.k
            group_id += 1
        return new_runs

    def multi_pass_merge(self):
        current_runs = []
        for r in self.runs:
            current_runs.extend(r)
        pass_id = 0
        while len(current_runs) > 1:
            current_runs = self.merge_pass(current_runs, pass_id)
            pass_id += 1
        return current_runs[0]

    def sort(self, output_file):
        try:
            self.generate_runs()
            final_run = self.multi_pass_merge()
            out_dir = os.path.dirname(output_file)
            if out_dir:
                os.makedirs(out_dir, exist_ok=True)
            if os.path.exists(output_file):
                os.remove(output_file)
            shutil.move(final_run, output_file)
        finally:
            self.cleanup()


# ══════════════════════════════════════════════════════════════
#  VISUALIZE — hoàn toàn tách biệt, chỉ chạy trên sample nhỏ
#  KHÔNG ảnh hưởng gì đến sort thật ở trên
# ══════════════════════════════════════════════════════════════

VISUALIZE_MAX_NUMBERS = 300   # tối đa 300 số để visualize
SAMPLE_BARS           = 18    # số bars hiển thị mỗi run card
PREVIEW_LIMIT         = 120


class VisualizeSort:
    """
    Sort trên sample nhỏ, sinh steps cho frontend.
    Hoàn toàn độc lập với ExternalMergeSorter.
    """

    def __init__(self, input_file):
        self.input_file = input_file   # chỉ đọc, không bao giờ ghi
        self.steps      = []

    def _read_double_raw(self, f):
        data = f.read(8)
        if not data:
            return None
        return struct.unpack("d", data)[0]

    def _sample_small(self, arr):
        return arr[:SAMPLE_BARS]

    # ── chạy toàn bộ, trả về steps ───────────────────────────
    def run(self):
        viz_dir = tempfile.mkdtemp(prefix="extsort_viz_")
        try:
            self._run_inner(viz_dir)
        finally:
            shutil.rmtree(viz_dir, ignore_errors=True)
        return self.steps

    def _run_inner(self, viz_dir):
        # 1. Lấy params từ file gốc (kích thước thật)
        b, kk      = auto_tune_params(self.input_file)
        orig_size  = os.path.getsize(self.input_file)

        # giới hạn cho dễ visualize
        viz_block  = min(b, max(10, VISUALIZE_MAX_NUMBERS // 4))  # ~75 số/block → ~4 runs
        viz_k      = min(kk, 4)

        self.steps.append({
            "type":                  "params_chosen",
            "block_size":            viz_block,
            "k":                     viz_k,
            "visualize_sample_size": min(VISUALIZE_MAX_NUMBERS,
                                         orig_size // 8),
            "original_file_size":    orig_size,
        })

        # 2. Đọc sample từ file gốc
        sampled = []
        with open(self.input_file, "rb") as f:
            while len(sampled) < VISUALIZE_MAX_NUMBERS:
                data = f.read(8)
                if not data:
                    break
                sampled.append(struct.unpack("d", data)[0])

        actual_count = len(sampled)
        self.steps.append({
            "type":      "file_info",
            "file_name": os.path.basename(self.input_file),
            "file_size": orig_size,
        })

        # 3. Ghi sample vào file tạm trong viz_dir
        viz_input = os.path.join(viz_dir, "_sample.bin")
        with open(viz_input, "wb") as f:
            f.write(struct.pack(f"{actual_count}d", *sampled))

        # 4. Phase 1: generate runs từ sample
        runs   = []
        run_id = 0
        with open(viz_input, "rb") as f:
            while True:
                chunk = f.read(8 * viz_block)
                if not chunk:
                    break
                nums = list(struct.unpack(f"{len(chunk)//8}d", chunk))

                self.steps.append({
                    "type":      "read_block",
                    "run_index": run_id,
                    "sample":    self._sample_small(nums),
                    "count":     len(nums),
                })

                nums.sort()

                run_path = os.path.join(viz_dir, f"run_{run_id}.bin")
                with open(run_path, "wb") as out:
                    out.write(struct.pack(f"{len(nums)}d", *nums))

                self.steps.append({
                    "type":          "sort_block",
                    "run_index":     run_id,
                    "sorted_sample": self._sample_small(nums),
                    "count":         len(nums),
                })

                runs.append(run_path)
                run_id += 1

        self.steps.insert(0, {
            "type":        "meta",
            "block_size":  viz_block,
            "k":           viz_k,
            "runs_count":  len(runs),
        })

        # 5. Merge passes
        current = list(runs)
        pass_id = 0
        while len(current) > 1:
            self.steps.append({
                "type":        "pass_info",
                "pass_id":     pass_id,
                "runs_before": len(current),
                "k":           viz_k,
            })
            new_runs, group_id, i = [], 0, 0
            while i < len(current):
                group   = current[i:i + viz_k]
                out_run = os.path.join(viz_dir, f"pass{pass_id}_run{group_id}.bin")
                self._merge_with_steps(group, out_run, pass_id, group_id)
                new_runs.append(out_run)
                i        += viz_k
                group_id += 1
            current = new_runs
            pass_id += 1

        # 6. Final preview
        final_preview = []
        if current:
            with open(current[0], "rb") as f:
                for _ in range(PREVIEW_LIMIT):
                    data = f.read(8)
                    if not data:
                        break
                    final_preview.append(struct.unpack("d", data)[0])

        self.steps.append({
            "type":          "finished",
            "final_preview": final_preview,
        })

    def _merge_with_steps(self, input_runs, output_run, pass_id, group_id):
        """Merge có log steps cho visualizer."""
        files = [open(r, "rb") for r in input_runs]
        heap  = []

        # init heap + log
        for i, f in enumerate(files):
            num = self._read_double_raw(f)
            if num is not None:
                heapq.heappush(heap, (num, i))
                self.steps.append({
                    "type":           "heap_push",
                    "pass_id":        pass_id,
                    "group_id":       group_id,
                    "value":          num,
                    "src":            i,
                    "heap_snapshot":  [list(x) for x in sorted(heap)[:20]],
                })

        # peek input buffers
        input_buffers = []
        for f in files:
            pos  = f.tell()
            data = f.read(8 * 8)
            buf  = list(struct.unpack(f"{len(data)//8}d", data)) if data else []
            f.seek(pos)
            input_buffers.append(buf[:SAMPLE_BARS])

        self.steps.append({
            "type":          "merge_start",
            "pass_id":       pass_id,
            "group_id":      group_id,
            "inputs":        [os.path.basename(p) for p in input_runs],
            "input_buffers": input_buffers,
            "initial_heap":  [list(x) for x in sorted(heap)[:20]],
        })

        merged_count = 0
        with open(output_run, "wb") as out:
            while heap:
                value, src_idx = heapq.heappop(heap)

                self.steps.append({
                    "type":          "heap_pop",
                    "pass_id":       pass_id,
                    "group_id":      group_id,
                    "value":         value,
                    "src":           src_idx,
                    "heap_snapshot": [list(x) for x in sorted(heap)[:20]],
                })

                out.write(struct.pack("d", value))
                merged_count += 1

                self.steps.append({
                    "type":         "output_emit",
                    "pass_id":      pass_id,
                    "group_id":     group_id,
                    "value":        value,
                    "emitted_count":merged_count,
                })

                nxt = self._read_double_raw(files[src_idx])
                if nxt is not None:
                    heapq.heappush(heap, (nxt, src_idx))
                    self.steps.append({
                        "type":          "heap_push",
                        "pass_id":       pass_id,
                        "group_id":      group_id,
                        "value":         nxt,
                        "src":           src_idx,
                        "heap_snapshot": [list(x) for x in sorted(heap)[:20]],
                    })

        for f in files:
            f.close()

        self.steps.append({
            "type":         "merge_end",
            "pass_id":      pass_id,
            "group_id":     group_id,
            "merged_count": merged_count,
        })


# ══════════════════════════════════════════════════════════════
#  Wrapper functions — dùng trong main.py
# ══════════════════════════════════════════════════════════════

def external_merge_sort(input_path, output_path):
    """Sort thật — dùng params từ auto_tune, không log gì."""
    block_size, k = auto_tune_params(input_path)
    sorter = ExternalMergeSorter(input_path, block_size, k)
    sorter.sort(output_path)


def external_merge_sort_visualize(input_path):
    """Visualize — sample 300 số, log đầy đủ steps, không output file."""
    viz = VisualizeSort(input_path)
    return viz.run()