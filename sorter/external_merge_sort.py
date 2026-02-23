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

        # Events stream for visualization (list of dict)
        self.events = []

        # small metadata and previews
        self.preview_limit = 120

    # ---------- utils ----------
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

    def _preview_trim(self, arr, limit=None):
        if arr is None:
            return []
        if limit is None:
            limit = self.preview_limit
        if len(arr) <= limit:
            return arr
        return arr[:limit]

    def log_event(self, ev: dict):
        # add a shallow copy to event stream (keeps memory predictable)
        self.events.append(ev.copy())

    # ---------- run generation ----------
    def generate_runs(self):
        self.runs = [[] for _ in range(self.k)]

        with open(self.input_file, "rb") as f:
            run_id = 0
            while True:
                nums = self._read_block(f)
                if nums is None:
                    break

                # event: read block (raw)
                self.log_event({
                    "type": "read_block",
                    "block_id": run_id,
                    "values": self._preview_trim(nums)
                })

                nums.sort()

                run_name = os.path.join(self.work_dir, f"run{run_id}.bin")
                with open(run_name, "wb") as out:
                    out.write(struct.pack(f"{len(nums)}d", *nums))

                self.runs[run_id % self.k].append(run_name)

                # event: run created (sorted values preview)
                self.log_event({
                    "type": "run_created",
                    "run_id": run_id,
                    "file": os.path.basename(run_name),
                    "values": self._preview_trim(nums)
                })

                run_id += 1

        # metadata
        self.log_event({
            "type": "meta",
            "block_size": self.block_size,
            "k": self.k,
            "runs_count": run_id
        })

    # ---------- k-way merge ----------
    def merge_k_runs(self, input_runs, output_run, pass_id=None, group_id=None):
        files = [open(r, "rb") for r in input_runs]
        heap = []

        # event: merge group start
        try:
            input_basenames = [os.path.basename(p) for p in input_runs]
        except Exception:
            input_basenames = []

        self.log_event({
            "type": "merge_group_start",
            "pass_id": pass_id,
            "group_id": group_id,
            "inputs": input_basenames
        })

        # push initial heads
        for i, f in enumerate(files):
            num = self._read_double(f)
            if num is not None:
                heapq.heappush(heap, (num, i))
                # log load head
                self.log_event({
                    "type": "load_head",
                    "run": os.path.basename(input_runs[i]),
                    "value": num
                })
                # log heap snapshot
                self.log_event({
                    "type": "heap_push",
                    "heap": [h[0] for h in heap]
                })

        merged_values = []
        output_preview = []

        with open(output_run, "wb") as out:
            while heap:
                value, idx = heapq.heappop(heap)

                # log pop
                self.log_event({
                    "type": "heap_pop",
                    "popped": value,
                    "heap": [h[0] for h in heap]
                })

                out.write(struct.pack("d", value))
                merged_values.append(value)

                # maintain small output preview for visualization
                if len(output_preview) < self.preview_limit:
                    output_preview.append(value)
                # log write_output
                self.log_event({
                    "type": "write_output",
                    "value": value,
                    "output_preview": list(output_preview)
                })

                nxt = self._read_double(files[idx])
                if nxt is not None:
                    heapq.heappush(heap, (nxt, idx))
                    self.log_event({
                        "type": "load_head",
                        "run": os.path.basename(input_runs[idx]),
                        "value": nxt
                    })
                    self.log_event({
                        "type": "heap_push",
                        "heap": [h[0] for h in heap]
                    })

        for f in files:
            f.close()

        # event: merge group end, include merged preview (trimmed)
        self.log_event({
            "type": "merge_group_end",
            "pass_id": pass_id,
            "group_id": group_id,
            "merged": self._preview_trim(merged_values)
        })

        # append to internal passes (kept for summary; optional)
        try:
            input_basenames = [os.path.basename(p) for p in input_runs]
        except Exception:
            input_basenames = []

        # store full merged values for summary, but be careful with memory
        self.events.append({
            "type": "__internal_pass_record",
            "pass_id": pass_id,
            "group_id": group_id,
            "inputs": input_basenames,
            "merged": None  # avoid storing full merged (already in merged_values if needed)
        })

    # ---------- one pass ----------
    def merge_pass(self, runs, pass_id):
        new_runs = []
        i = 0
        group_id = 0

        while i < len(runs):
            group = runs[i:i + self.k]
            new_run = os.path.join(self.work_dir, f"pass{pass_id}_run{group_id}.bin")

            self.merge_k_runs(group, new_run, pass_id=pass_id, group_id=group_id)
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
            self.log_event({
                "type": "pass_start",
                "pass_id": pass_id,
                "runs_count": len(current_runs)
            })
            current_runs = self.merge_pass(current_runs, pass_id)
            self.log_event({
                "type": "pass_end",
                "pass_id": pass_id,
                "new_runs_count": len(current_runs)
            })
            pass_id += 1

        return current_runs[0]

    # ---------- sort pipeline ----------
    def sort(self, output_file="output/sorted.bin"):
        try:
            self.generate_runs()
            final_run = self.multi_pass_merge()

            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            if os.path.exists(output_file):
                os.remove(output_file)
            shutil.move(final_run, output_file)

            # final preview read
            final_preview = []
            try:
                with open(output_file, "rb") as f:
                    for _ in range(self.preview_limit):
                        data = f.read(8)
                        if not data:
                            break
                        final_preview.append(struct.unpack("d", data)[0])
            except Exception as e:
                final_preview = []
                # non-fatal

            self.log_event({
                "type": "finished",
                "final": self._preview_trim(final_preview)
            })

        finally:
            # cleanup temp runs
            self.cleanup()

    def get_steps(self):
        # Remove internal-only records before returning
        out_events = [e for e in self.events if e.get("type") != "__internal_pass_record"]
        return out_events


def external_merge_sort(input_path, output_path):
    block_size, k = auto_tune_params(input_path)
    sorter = ExternalMergeSorter(input_path, block_size, k)
    sorter.sort(output_path)
    return sorter.get_steps()