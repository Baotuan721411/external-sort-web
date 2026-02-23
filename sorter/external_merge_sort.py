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

    def _preview_trim(self, arr, limit=120):
        if arr is None:
            return []
        return arr[:limit]

    # ================= RUN GENERATION =================
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
                self.steps["runs"].append(self._preview_trim(nums))
                run_id += 1

    # ================= MERGE =================
    def merge_k_runs(self, input_runs, output_run, pass_id=None, group_id=None):
        files = [open(r, "rb") for r in input_runs]
        heap = []

        for i, f in enumerate(files):
            num = self._read_double(f)
            if num is not None:
                heapq.heappush(heap, (num, i))

        merged_values = []

        with open(output_run, "wb") as out:
            while heap:
                value, idx = heapq.heappop(heap)
                out.write(struct.pack("d", value))
                merged_values.append(value)

                nxt = self._read_double(files[idx])
                if nxt is not None:
                    heapq.heappush(heap, (nxt, idx))

        for f in files:
            f.close()

        self.steps["passes"].append({
            "pass_id": pass_id,
            "group_id": group_id,
            "merged": merged_values
        })

    def merge_pass(self, runs, pass_id):
        new_runs = []
        i = 0
        group_id = 0

        while i < len(runs):
            group = runs[i:i + self.k]
            new_run = os.path.join(self.work_dir, f"pass{pass_id}_run{group_id}.bin")
            self.merge_k_runs(group, new_run, pass_id, group_id)
            new_runs.append(new_run)

            i += self.k
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

    def sort(self, output_file="output/sorted.bin"):
        try:
            self.generate_runs()
            final_run = self.multi_pass_merge()

            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            if os.path.exists(output_file):
                os.remove(output_file)

            shutil.move(final_run, output_file)

        finally:
            self.cleanup()

    # ⭐ QUAN TRỌNG
    def get_steps(self):
        steps_list = []

        # meta step (frontend đọc k & block_size từ đây)
        steps_list.append({
            "type": "meta",
            "block_size": self.block_size,
            "k": self.k,
            "runs_count": len(self.steps["runs"])
        })

        for idx, run in enumerate(self.steps["runs"]):
            steps_list.append({
                "type": "sort_block",
                "run_index": idx,
                "data": run
            })

        for p in self.steps["passes"]:
            steps_list.append({
                "type": "merge",
                "pass_id": p["pass_id"],
                "group_id": p["group_id"],
                "result": self._preview_trim(p["merged"])
            })

        return steps_list


def external_merge_sort(input_path, output_path):
    block_size, k = auto_tune_params(input_path)
    sorter = ExternalMergeSorter(input_path, block_size, k)
    sorter.sort(output_path)
    return sorter.get_steps()