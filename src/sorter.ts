/**
 * External Merge Sort Logic for Visualization
 */

export interface SortStep {
  type: string;
  [key: string]: any;
}

export class VisualizeSort {
  private steps: SortStep[] = [];
  private readonly VISUALIZE_MAX_NUMBERS = 300;
  private readonly SAMPLE_BARS = 18;

  constructor(private data: number[]) {}

  private sampleSmall(arr: number[]): number[] {
    return arr.slice(0, this.SAMPLE_BARS);
  }

  public run(): SortStep[] {
    this.steps = [];
    const n = this.data.length;
    
    // Heuristic for visualization params
    const vizBlock = Math.min(75, Math.max(10, Math.floor(this.VISUALIZE_MAX_NUMBERS / 4)));
    const vizK = 4;

    this.steps.push({
      type: "params_chosen",
      block_size: vizBlock,
      k: vizK,
      visualize_sample_size: Math.min(this.VISUALIZE_MAX_NUMBERS, n),
      original_file_size: n * 8,
    });

    const sampled = this.data.slice(0, this.VISUALIZE_MAX_NUMBERS);
    
    this.steps.push({
      type: "file_info",
      file_name: "input_data.bin",
      file_size: n * 8,
    });

    // Phase 1: Generate Runs
    let runs: number[][] = [];
    let runId = 0;
    for (let i = 0; i < sampled.length; i += vizBlock) {
      const chunk = sampled.slice(i, i + vizBlock);
      
      this.steps.push({
        type: "read_block",
        run_index: runId,
        sample: this.sampleSmall(chunk),
        count: chunk.length,
      });

      const sortedChunk = [...chunk].sort((a, b) => a - b);
      
      this.steps.push({
        type: "sort_block",
        run_index: runId,
        sorted_sample: this.sampleSmall(sortedChunk),
        count: sortedChunk.length,
      });

      runs.push(sortedChunk);
      runId++;
    }

    this.steps.unshift({
      type: "meta",
      block_size: vizBlock,
      k: vizK,
      runs_count: runs.length,
    });

    // Phase 2: Merge Passes
    let currentRuns = [...runs];
    let passId = 0;
    
    while (currentRuns.length > 1) {
      this.steps.push({
        type: "pass_info",
        pass_id: passId,
        runs_before: currentRuns.length,
        k: vizK,
      });

      let nextRuns: number[][] = [];
      let groupId = 0;
      
      for (let i = 0; i < currentRuns.length; i += vizK) {
        const group = currentRuns.slice(i, i + vizK);
        const merged = this.mergeWithSteps(group, passId, groupId);
        nextRuns.push(merged);
        groupId++;
      }
      
      currentRuns = nextRuns;
      passId++;
    }

    this.steps.push({
      type: "finished",
      final_preview: currentRuns[0]?.slice(0, 120) || [],
    });

    return this.steps;
  }

  private mergeWithSteps(inputRuns: number[][], passId: number, groupId: number): number[] {
    const pointers = new Array(inputRuns.length).fill(0);
    const heap: { value: number; src: number }[] = [];
    const result: number[] = [];

    // Initial heap push
    for (let i = 0; i < inputRuns.length; i++) {
      if (inputRuns[i].length > 0) {
        const val = inputRuns[i][0];
        heap.push({ value: val, src: i });
        pointers[i]++;
        
        // We'll sift up manually or just sort for simplicity in viz
        this.steps.push({
          type: "heap_push",
          pass_id: passId,
          group_id: groupId,
          value: val,
          src: i,
          heap_snapshot: [...heap].sort((a, b) => a.value - b.value).slice(0, 20),
        });
      }
    }

    // Sort heap initially
    heap.sort((a, b) => a.value - b.value);

    const inputBuffers = inputRuns.map(r => this.sampleSmall(r));

    this.steps.push({
      type: "merge_start",
      pass_id: passId,
      group_id: groupId,
      inputs: inputRuns.map((_, idx) => `run_${idx}.bin`),
      input_buffers: inputBuffers,
      initial_heap: [...heap].slice(0, 20),
    });

    let mergedCount = 0;
    while (heap.length > 0) {
      // In a real min-heap, we'd pop the root.
      // For viz, we assume the heap is maintained.
      const { value, src } = heap.shift()!;
      
      this.steps.push({
        type: "heap_pop",
        pass_id: passId,
        group_id: groupId,
        value: value,
        src: src,
        heap_snapshot: [...heap].slice(0, 20),
      });

      result.push(value);
      mergedCount++;

      this.steps.push({
        type: "output_emit",
        pass_id: passId,
        group_id: groupId,
        value: value,
        emitted_count: mergedCount,
      });

      if (pointers[src] < inputRuns[src].length) {
        const nextVal = inputRuns[src][pointers[src]];
        heap.push({ value: nextVal, src: src });
        pointers[src]++;
        
        // Re-sort heap to maintain min-heap property for next iteration
        heap.sort((a, b) => a.value - b.value);

        this.steps.push({
          type: "heap_push",
          pass_id: passId,
          group_id: groupId,
          value: nextVal,
          src: src,
          heap_snapshot: [...heap].slice(0, 20),
        });

        // Check if we need to show a "refill" (simulated here)
        if (pointers[src] % 10 === 0) {
           this.steps.push({
             type: "input_buffer_update",
             pass_id: passId,
             group_id: groupId,
             input_buffers: inputRuns.map((r, idx) => this.sampleSmall(r.slice(pointers[idx])))
           });
        }
      }
    }

    this.steps.push({
      type: "merge_end",
      pass_id: passId,
      group_id: groupId,
      merged_count: mergedCount,
    });

    return result;
  }
}
