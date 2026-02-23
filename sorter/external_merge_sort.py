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

        # steps để frontend animate ( nhẹ, chỉ lưu số và arrays ngắn )
        self.steps = {
            "block_size": self.block_size,
            "k": self.k,
            "initial": None,   # optional: sẽ đặt khi cần
            "runs": [],        # danh sách các run (mỗi run là list các số đã sort)
            "passes": []       # danh sách các merge-groups: mỗi item chứa pass_id, group_id, inputs, merged
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

    # ================= PHASE 1: RUN GENERATION =================
    def generate_runs(self):
        # reset runs khi sort nhiều lần
        self.runs = [[] for _ in range(self.k)]

        # optional: read initial small prefix for visualization (không bắt buộc)
        # Không đọc toàn file để tránh dùng nhiều RAM.

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

                # lưu file run vào cấu trúc runs
                self.runs[run_id % self.k].append(run_name)

                # log run (dùng để vẽ cột run trên frontend)
                self.steps["runs"].append(nums.copy())

                run_id += 1

        # Nếu muốn, frontend có thể cần initial array nhỏ: không bắt buộc
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

                # log value vào merged array cho animation
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

            print("SORT COMPLETED.")
            print("Output file:", output_file)

        finally:
            # 6. xóa toàn bộ run files
            # (LƯU Ý: steps đã được ghi vào self.steps trước khi xóa)
            self.cleanup()

    def get_steps(self):
        """
        Trả object steps (chuẩn JSON serializable) cho frontend.
        """
        return self.steps


def external_merge_sort(input_path, output_path):
    """
    Hàm wrapper: trả về steps sau khi sort xong.
    """
    block_size, k = auto_tune_params(input_path)
    sorter = ExternalMergeSorter(input_path, block_size, k)
    sorter.sort(output_path)
    return sorter.get_steps()