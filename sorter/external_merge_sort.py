import struct
import os
import heapq
import tempfile
import shutil
from .auto_config import auto_tune_params


class ExternalMergeSorter:

    def __init__(self, input_file, block_size=100, k=5):
        self.input_file = input_file
        self.block_size = block_size
        self.k = k

        self.work_dir = tempfile.mkdtemp(prefix="extsort_")
        self.runs = [[] for _ in range(k)]

        self.steps = {
            "block_size": self.block_size,
            "k": self.k,
            "runs": [],
            "passes": [],
            "final_preview": []
        }

    # ================= CLEANUP =================
    def cleanup(self):
        shutil.rmtree(self.work_dir, ignore_errors=True)

    # ================= VISUAL SAMPLING =================
    def _sample_for_visual(self, arr, limit=60):
        if not arr:
            return []

        n = len(arr)
        if n <= limit:
            return arr

        step = n / limit
        result = []
        i = 0.0

        while int(i) < n:
            result.append(arr[int(i)])
            i += step

        return result[:limit]

    # ================= READ BLOCK =================
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

    # ================= PHASE 1 =================
    def generate_runs(self):
        self.runs = [[] for _ in range(self.k)]
        self.steps["runs"] = []

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

                # CHỈ LẤY MẪU ĐỂ VẼ
                self.steps["runs"].append(self._sample_for_visual(nums))

                run_id += 1

    # ================= MERGE =================
    def merge_k_runs(self, input_runs, output_run, pass_id=None, group_id=None):

        files = [open(r, "rb") for r in input_runs]
        heap = []

        for i, f in enumerate(files):
            num = self._read_double(f)
            if num is not None:
                heapq.heappush(heap, (num, i))

        merged_visual = []
        counter = 0
        VISUAL_EVERY = 50

        with open(output_run, "wb") as out:
            while heap:
                value, idx = heapq.heappop(heap)
                out.write(struct.pack("d", value))

                # CHỈ SAMPLE ĐỂ TRÁNH JSON 10MB
                if counter % VISUAL_EVERY == 0:
                    merged_visual.append(value)

                counter += 1

                nxt = self._read_double(files[idx])
                if nxt is not None:
                    heapq.heappush(heap, (nxt, idx))

        for f in files:
            f.close()

        self.steps["passes"].append({
            "pass_id": pass_id,
            "group_id": group_id,
            "merged": self._sample_for_visual(merged_visual)
        })

    # ================= MERGE PASS =================
    def merge_pass(self, runs, pass_id):
        new_runs = []
        i = 0
        group_id = 0

        while i < len(runs):
            group = runs[i:i + self.k]

            new_run = os.path.join(
                self.work_dir,
                f"pass{pass_id}_run{group_id}.bin"
            )

            self.merge_k_runs(group, new_run, pass_id, group_id)

            new_runs.append(new_run)

            i += self.k
            group_id += 1

        return new_runs

    # ================= MULTI PASS =================
    def multi_pass_merge(self):
        current_runs = []
        for r in self.runs:
            current_runs.extend(r)

        pass_id = 0

        while len(current_runs) > 1:
            current_runs = self.merge_pass(current_runs, pass_id)
            pass_id += 1

        return current_runs[0]

    # ================= MAIN SORT =================
    def sort(self, output_file):

        try:
            self.generate_runs()
            final_run = self.multi_pass_merge()

            os.makedirs(os.path.dirname(output_file), exist_ok=True)

            if os.path.exists(output_file):
                os.remove(output_file)

            shutil.move(final_run, output_file)

            # FINAL PREVIEW
            preview = []
            with open(output_file, "rb") as f:
                for _ in range(120):
                    data = f.read(8)
                    if not data:
                        break
                    preview.append(struct.unpack("d", data)[0])

            self.steps["final_preview"] = self._sample_for_visual(preview)

        finally:
            self.cleanup()

    def get_steps(self):

        steps_list = []

        steps_list.append({
            "type": "meta",
            "block_size": self.block_size,
            "k": self.k
        })

        for idx, run in enumerate(self.steps["runs"]):
            steps_list.append({
                "type": "read_block",
                "data": run
            })

            steps_list.append({
                "type": "sort_block",
                "data": run
            })

        for p in self.steps["passes"]:
            steps_list.append({
                "type": "merge",
                "data": p["merged"]
            })

        steps_list.append({
            "type": "finished",
            "data": self.steps["final_preview"]
        })

        return steps_list


def external_merge_sort(input_path, output_path):
    block_size, k = auto_tune_params(input_path)
    sorter = ExternalMergeSorter(input_path, block_size, k)
    sorter.sort(output_path)
    return sorter.get_steps()