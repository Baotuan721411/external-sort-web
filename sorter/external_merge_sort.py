import struct
import os
import heapq
import tempfile
import shutil
from .auto_config import auto_tune_params

class ExternalMergeSorter:

    # ================= INIT =================
    def __init__(self, input_file, block_size=100, k=5):
        self.input_file = input_file
        self.block_size = block_size
        self.k = k

        # tạo thư mục làm việc tạm
        self.work_dir = tempfile.mkdtemp(prefix="extsort_")

        # ma trận run (mỗi bucket chứa danh sách run files)
        self.runs = [[] for _ in range(k)]

        # steps để frontend animate ( nhẹ, lưu run list + pass merged arrays )
        # structure:
        # {
        #   "block_size": int,
        #   "k": int,
        #   "runs": [ [nums], [nums], ... ],
        #   "passes": [ {pass_id, group_id, inputs, merged}, ... ],
        #   "final_preview": [nums...] (optional)
        # }
        self.steps = {
            "block_size": self.block_size,
            "k": self.k,
            "runs": [],
            "passes": [],
            "final_preview": []
        }

    # ================= CLEANUP =================
    def cleanup(self):
        """Xóa toàn bộ file tạm"""
        shutil.rmtree(self.work_dir, ignore_errors=True)

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

    # helper: truncate preview arrays to avoid huge payloads for frontend
    def _preview_trim(self, arr, limit=120):
        if arr is None:
            return []
        if len(arr) <= limit:
            return arr
        # keep first `limit` elements for visualization
        return arr[:limit]

    # ================= PHASE 1: RUN GENERATION =================
    def generate_runs(self):
        # reset runs khi sort nhiều lần
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

                # lưu file run vào cấu trúc runs (file paths)
                self.runs[run_id % self.k].append(run_name)

                # log run (dùng để vẽ cột run trên frontend) - store the sorted values (preview-trimmed)
                self.steps["runs"].append(self._preview_trim(nums))

                run_id += 1

        print("Initial runs created. Total runs:", run_id)

    # ================= K-WAY MERGE =================
    def merge_k_runs(self, input_runs, output_run, pass_id=None, group_id=None):
        """
        Merge một nhóm input_runs (tối đa k) thành output_run.
        Ghi lại merged_values cho steps.
        """
        files = [open(r, "rb") for r in input_runs]
        heap = []

        # đưa phần tử đầu tiên vào heap
        for i, f in enumerate(files):
            num = self._read_double(f)
            if num is not None:
                heapq.heappush(heap, (num, i))

        merged_values = []

        with open(output_run, "wb") as out:
            while heap:
                value, idx = heapq.heappop(heap)
                out.write(struct.pack("d", value))

                # log value vào merged array cho animation (we collect full merged for this group,
                # but will trim when returning steps to frontend)
                merged_values.append(value)

                nxt = self._read_double(files[idx])
                if nxt is not None:
                    heapq.heappush(heap, (nxt, idx))

        for f in files:
            f.close()

        # lưu thông tin 1 merge-group vào steps
        try:
            input_basenames = [os.path.basename(p) for p in input_runs]
        except Exception:
            input_basenames = []

        self.steps["passes"].append({
            "pass_id": pass_id,
            "group_id": group_id,
            "inputs": input_basenames,
            "merged": merged_values
        })

    # ================= ONE MERGE PASS =================
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

            # merge group → new_run, đồng thời log merged values
            self.merge_k_runs(group, new_run, pass_id=pass_id, group_id=group_id)

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
            print(f"Merge Pass {pass_id}: {len(current_runs)} runs")
            current_runs = self.merge_pass(current_runs, pass_id)
            pass_id += 1

        return current_runs[0]

    # ================= MAIN SORT PIPELINE =================
    def sort(self, output_file="output/sorted.bin"):
        try:
            # 1. tạo runs
            self.generate_runs()

            # 2. merge
            final_run = self.multi_pass_merge()

            # 3. đảm bảo thư mục output tồn tại
            os.makedirs(os.path.dirname(output_file), exist_ok=True)

            # 4. nếu file cũ tồn tại → xóa (ghi đè)
            if os.path.exists(output_file):
                os.remove(output_file)

            # 5. chuyển file kết quả ra ngoài temp
            shutil.move(final_run, output_file)

            # 6. tạo preview final (chỉ đọc tối đa PREVIEW_LIMIT phần tử)
            PREVIEW_LIMIT = 120
            final_preview = []
            try:
                with open(output_file, "rb") as f:
                    # read up to PREVIEW_LIMIT doubles
                    for _ in range(PREVIEW_LIMIT):
                        data = f.read(8)
                        if not data:
                            break
                        final_preview.append(struct.unpack("d", data)[0])
            except Exception as e:
                print("Warning: failed to read final preview:", e)
                final_preview = []

            self.steps["final_preview"] = self._preview_trim(final_preview, PREVIEW_LIMIT)

            print("SORT COMPLETED.")
            print("Output file:", output_file)

        finally:
            # 7. xóa toàn bộ run files
            # (LƯU Ý: steps đã được ghi vào self.steps trước khi xóa)
            self.cleanup()

    def get_steps(self):
        """
        Trả một LIST các step (chuẩn JSON serializable) cho frontend.
        Format trả về: [ {type:... , data:...}, ... ]
        """
        steps_list = []

        # meta step (optional)
        steps_list.append({
            "type": "meta",
            "block_size": self.block_size,
            "k": self.k,
            "runs_count": len(self.steps.get("runs", []))
        })

        # show runs (as read_block then sort_block)
        for idx, run in enumerate(self.steps.get("runs", [])):
            # read_block (we only have the post-sort values; it's okay)
            steps_list.append({
                "type": "read_block",
                "run_index": idx,
                "data": self._preview_trim(run)
            })
            # sort_block
            steps_list.append({
                "type": "sort_block",
                "run_index": idx,
                "data": self._preview_trim(run)
            })

        # show merge passes (in recorded order)
        for p in self.steps.get("passes", []):
            steps_list.append({
                "type": "merge",
                "pass_id": p.get("pass_id"),
                "group_id": p.get("group_id"),
                "inputs": p.get("inputs", []),
                "result": self._preview_trim(p.get("merged", []))
            })

        # finished: use final_preview if available, else use last merged result
        final_preview = self.steps.get("final_preview", [])
        if final_preview:
            steps_list.append({
                "type": "finished",
                "data": self._preview_trim(final_preview)
            })
        else:
            # fallback to last pass merged array
            passes = self.steps.get("passes", [])
            if passes:
                steps_list.append({
                    "type": "finished",
                    "data": self._preview_trim(passes[-1].get("merged", []))
                })
            else:
                # as last resort combine runs (may be empty)
                combined = []
                for r in self.steps.get("runs", []):
                    combined.extend(r)
                steps_list.append({
                    "type": "finished",
                    "data": self._preview_trim(combined)
                })

        return steps_list


def external_merge_sort(input_path, output_path):
    """
    Hàm wrapper: trả về steps sau khi sort xong.
    """
    block_size, k = auto_tune_params(input_path)
    sorter = ExternalMergeSorter(input_path, block_size, k)
    sorter.sort(output_path)
    return sorter.get_steps()