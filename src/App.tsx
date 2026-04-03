import React, { useState, useEffect, useRef, useMemo } from 'react';
import { 
  Upload, 
  Play, 
  Pause, 
  SkipForward, 
  RotateCcw, 
  Download, 
  Database, 
  Cpu, 
  FileCode, 
  Info,
  ChevronRight,
  Activity
} from 'lucide-react';
import { motion, AnimatePresence } from 'motion/react';
import { VisualizeSort, SortStep } from './sorter';

// --- Types ---
interface VisualState {
  idx: number;
  phaseBadgeText: string;
  runs: Record<number, { nums: number[]; state: string; count: number }>;
  bufs: Record<string, { nums: number[]; usedCount: number; refilling: boolean }>;
  heap: { value: number; src: number }[];
  output: number[];
  outputCount: number;
  logEntries: { msg: string; type: string; t: string }[];
  passId: number | null;
  groupId: number | null;
  mergeZones: { passId: number; groupId: number; inputs: string[] }[];
  transferMsg: string;
}

const INITIAL_VS: VisualState = {
  idx: 0,
  phaseBadgeText: 'Chờ upload...',
  runs: {},
  bufs: {},
  heap: [],
  output: [],
  outputCount: 0,
  logEntries: [],
  passId: null,
  groupId: null,
  mergeZones: [],
  transferMsg: '',
};

// --- Constants ---
const H_R = 22;
const H_LH = 62;
const H_VB_W = 500;
const H_VB_H = 260;
const H_MAX_NODES = 15;

export default function App() {
  const [file, setFile] = useState<File | null>(null);
  const [steps, setSteps] = useState<SortStep[]>([]);
  const [vs, setVs] = useState<VisualState>(INITIAL_VS);
  const [paused, setPaused] = useState(true);
  const [speed, setSpeed] = useState(0.25);
  const [history, setHistory] = useState<VisualState[]>([]);
  const [meta, setMeta] = useState<any>(null);
  const [params, setParams] = useState<any>(null);
  const [isProcessing, setIsProcessing] = useState(false);

  const timerRef = useRef<NodeJS.Timeout | null>(null);
  const logListRef = useRef<HTMLDivElement>(null);

  // --- Helpers ---
  const fmt = (v: number) => v.toFixed(1);

  const log = (msg: string, type = '') => {
    const t = new Date().toLocaleTimeString('vi', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    setVs(prev => ({
      ...prev,
      logEntries: [{ msg, type, t }, ...prev.logEntries].slice(0, 120)
    }));
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      setFile(e.target.files[0]);
    }
  };

  const startVisualize = async () => {
    if (!file) return;
    setIsProcessing(true);
    log('Đang xử lý file...', 'info');

    // In a real app, we'd read the binary file. 
    // For this demo, we'll generate some random data based on the file size or just use a fixed sample.
    const reader = new FileReader();
    reader.onload = (e) => {
      const buffer = e.target?.result as ArrayBuffer;
      const floatArray = new Float64Array(buffer);
      const data = Array.from(floatArray);
      
      const sorter = new VisualizeSort(data.length > 0 ? data : Array.from({length: 300}, () => Math.random() * 2000 - 1000));
      const generatedSteps = sorter.run();
      
      setSteps(generatedSteps);
      setVs({ ...INITIAL_VS, idx: 0 });
      setHistory([]);
      
      const foundMeta = generatedSteps.find(s => s.type === 'meta');
      const foundParams = generatedSteps.find(s => s.type === 'params_chosen');
      setMeta(foundMeta);
      setParams(foundParams);
      
      setIsProcessing(false);
      log(`Nhận ${generatedSteps.length} bước - nhấn Start để bắt đầu`, 'ok');
    };
    reader.readAsArrayBuffer(file);
  };

  const processStep = (step: SortStep) => {
    setVs(prev => {
      const next = { ...prev, idx: prev.idx + 1 };
      
      switch (step.type) {
        case 'params_chosen':
          next.phaseBadgeText = 'Tham số đã chọn';
          break;
        case 'file_info':
          next.logEntries = [{ msg: `💾 File: ${step.file_name}`, type: 'disk', t: new Date().toLocaleTimeString() }, ...next.logEntries];
          break;
        case 'read_block':
          next.phaseBadgeText = `Phase 1 — Run ${step.run_index}`;
          next.runs = { ...next.runs, [step.run_index]: { nums: step.sample, state: 'reading', count: step.count } };
          next.transferMsg = `📖 Đọc ${step.count} số từ Disk vào RAM`;
          break;
        case 'sort_block':
          next.runs = { ...next.runs, [step.run_index]: { ...next.runs[step.run_index], nums: step.sorted_sample, state: 'sorted' } };
          break;
        case 'pass_info':
          next.phaseBadgeText = `Lượt trộn ${step.pass_id}`;
          next.outputCount = 0;
          next.output = [];
          break;
        case 'merge_start':
          next.passId = step.pass_id;
          next.groupId = step.group_id;
          next.mergeZones = [...next.mergeZones, { passId: step.pass_id, groupId: step.group_id, inputs: step.inputs }];
          if (step.input_buffers) {
            step.input_buffers.forEach((arr: number[], i: number) => {
              const key = `${step.pass_id}-${step.group_id}-${i}`;
              next.bufs[key] = { nums: arr, usedCount: 0, refilling: false };
            });
          }
          next.heap = step.initial_heap.map((h: any) => ({ value: h.value, src: h.src }));
          break;
        case 'heap_push':
          next.heap = [...next.heap, { value: step.value, src: step.src }].sort((a, b) => a.value - b.value);
          if (next.passId !== null && step.src !== undefined) {
            const key = `${next.passId}-${next.groupId}-${step.src}`;
            if (next.bufs[key]) {
              next.bufs[key] = { ...next.bufs[key], usedCount: next.bufs[key].usedCount + 1 };
            }
          }
          break;
        case 'heap_pop':
          next.heap = next.heap.filter(h => h.value !== step.value || h.src !== step.src);
          break;
        case 'output_emit':
          next.output = [...next.output, step.value].slice(-60);
          next.outputCount = step.emitted_count;
          if (step.emitted_count % 6 === 0) {
            next.transferMsg = `💾 Ghi ${fmt(step.value)} → Disk`;
          }
          break;
        case 'input_buffer_update':
          if (step.input_buffers) {
            step.input_buffers.forEach((arr: number[], i: number) => {
              const key = `${next.passId}-${next.groupId}-${i}`;
              next.bufs[key] = { nums: arr, usedCount: 0, refilling: true };
              setTimeout(() => {
                setVs(v => ({
                  ...v,
                  bufs: { ...v.bufs, [key]: { ...v.bufs[key], refilling: false } }
                }));
              }, 1000);
            });
          }
          break;
        case 'finished':
          next.phaseBadgeText = '✓ Hoàn thành';
          break;
      }
      return next;
    });
  };

  const handleNext = () => {
    if (vs.idx < steps.length) {
      setHistory(prev => [...prev, vs]);
      processStep(steps[vs.idx]);
    } else {
      setPaused(true);
    }
  };

  const handleBack = () => {
    if (history.length > 0) {
      const prev = history[history.length - 1];
      setVs(prev);
      setHistory(history.slice(0, -1));
    }
  };

  useEffect(() => {
    if (!paused) {
      timerRef.current = setInterval(handleNext, Math.max(50, 400 / speed));
    } else if (timerRef.current) {
      clearInterval(timerRef.current);
    }
    return () => {
      if (timerRef.current) clearInterval(timerRef.current);
    };
  }, [paused, vs.idx, steps, speed]);

  // --- Render Helpers ---
  const renderHeap = () => {
    if (vs.heap.length === 0) return <div className="flex items-center justify-center h-full text-muted font-mono text-sm">Heap trống</div>;

    const nodes = vs.heap.slice(0, H_MAX_NODES);
    const getPos = (i: number) => {
      const depth = Math.floor(Math.log2(i + 1));
      const maxDepth = Math.floor(Math.log2(nodes.length));
      const levelStart = (1 << depth) - 1;
      const posInLevel = i - levelStart;
      const slots = 1 << depth;
      const cx = (posInLevel + 0.5) * (H_VB_W / slots);
      const cy = H_R + 20 + depth * H_LH;
      return { cx, cy };
    };

    return (
      <svg viewBox={`0 0 ${H_VB_W} ${H_VB_H}`} className="w-full h-full overflow-visible">
        {nodes.map((_, i) => {
          const left = 2 * i + 1;
          const right = 2 * i + 2;
          const p = getPos(i);
          return (
            <React.Fragment key={`edges-${i}`}>
              {left < nodes.length && (
                <line 
                  x1={p.cx} y1={p.cy} 
                  x2={getPos(left).cx} y2={getPos(left).cy} 
                  stroke="rgba(90,110,190,0.3)" strokeWidth="1.5" 
                />
              )}
              {right < nodes.length && (
                <line 
                  x1={p.cx} y1={p.cy} 
                  x2={getPos(right).cx} y2={getPos(right).cy} 
                  stroke="rgba(90,110,190,0.3)" strokeWidth="1.5" 
                />
              )}
            </React.Fragment>
          );
        })}
        {nodes.map((node, i) => {
          const { cx, cy } = getPos(i);
          const isMin = i === 0;
          return (
            <motion.g 
              key={`node-${i}-${node.value}`}
              initial={{ opacity: 0, scale: 0.5 }}
              animate={{ opacity: 1, scale: 1, x: 0, y: 0 }}
              transition={{ type: 'spring', stiffness: 300, damping: 20 }}
            >
              <circle 
                cx={cx} cy={cy} r={H_R} 
                className={`fill-[#1a0f2e] stroke-violet-500/50 stroke-[1.6] ${isMin ? 'fill-[#2d0820] stroke-red-500 stroke-[2.2] shadow-[0_0_12px_rgba(244,63,94,0.9)]' : ''}`}
                style={isMin ? { filter: 'drop-shadow(0 0 8px rgba(244,63,94,0.6))' } : {}}
              />
              <text 
                x={cx} y={cy - 2} 
                className={`text-[10px] font-mono text-center fill-violet-200 pointer-events-none ${isMin ? 'fill-white font-bold' : ''}`}
                textAnchor="middle"
              >
                {fmt(node.value)}
              </text>
              <text 
                x={cx} y={cy + 10} 
                className="text-[8px] font-mono fill-violet-400/60 pointer-events-none"
                textAnchor="middle"
              >
                s{node.src}
              </text>
            </motion.g>
          );
        })}
      </svg>
    );
  };

  return (
    <div className="min-h-screen bg-[#080c14] text-[#dde6f0] font-sans selection:bg-cyan-500/30">
      <div className="max-w-[1600px] mx-auto p-6 flex flex-col gap-6">
        
        {/* --- Header --- */}
        <header className="flex flex-wrap items-center justify-between gap-4">
          <div className="flex items-baseline gap-3">
            <h1 className="font-mono text-xl font-bold text-white tracking-tight">ext_merge_sort</h1>
            <span className="font-mono text-[10px] text-cyan-400 border border-cyan-400/50 px-2 py-0.5 rounded uppercase opacity-80">Visualizer</span>
          </div>

          <div className="flex items-center gap-4">
            <div className="relative">
              <input 
                type="file" 
                id="fileInput" 
                className="hidden" 
                onChange={handleFileChange}
                accept=".bin"
              />
              <label 
                htmlFor="fileInput"
                className="flex items-center gap-2 px-4 py-2 bg-[#111926] border border-white/10 rounded-lg cursor-pointer hover:border-cyan-500/50 transition-colors text-sm text-muted hover:text-white"
              >
                <Upload size={16} />
                {file ? file.name : 'Chọn file .bin'}
              </label>
            </div>
            
            <button 
              onClick={startVisualize}
              disabled={!file || isProcessing}
              className="flex items-center gap-2 px-5 py-2 bg-gradient-to-br from-cyan-500 to-violet-600 text-white rounded-lg font-medium text-sm disabled:opacity-30 transition-all active:scale-95"
            >
              {isProcessing ? <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" /> : <Activity size={16} />}
              Visualize
            </button>
          </div>
        </header>

        {/* --- Info Bar --- */}
        <AnimatePresence>
          {steps.length > 0 && (
            <motion.div 
              initial={{ opacity: 0, y: -10 }}
              animate={{ opacity: 1, y: 0 }}
              className="flex flex-wrap items-center gap-3"
            >
              <div className="px-3 py-1 bg-[#111926] border border-white/10 rounded-full text-[11px] font-mono text-muted">
                k-way <span className="text-cyan-400 font-bold">{meta?.k || '—'}</span>
              </div>
              <div className="px-3 py-1 bg-[#111926] border border-white/10 rounded-full text-[11px] font-mono text-muted">
                block_size <span className="text-violet-400 font-bold">{meta?.block_size || '—'}</span>
              </div>
              <div className="px-3 py-1 bg-[#111926] border border-white/10 rounded-full text-[11px] font-mono text-muted">
                Runs <span className="text-white font-bold">{meta?.runs_count || '—'}</span>
              </div>
              <div className="px-3 py-1 bg-[#111926] border border-white/10 rounded-full text-[11px] font-mono text-muted">
                Sample <span className="text-white font-bold">{params?.visualize_sample_size || '—'}</span> số
              </div>
              <div className="ml-auto px-4 py-1 bg-cyan-500/10 border border-cyan-500/20 rounded-full text-[11px] font-mono text-cyan-400">
                {vs.phaseBadgeText}
              </div>
            </motion.div>
          )}
        </AnimatePresence>

        {/* --- Progress --- */}
        {steps.length > 0 && (
          <div className="flex items-center gap-4">
            <div className="flex-1 h-1.5 bg-white/5 rounded-full overflow-hidden">
              <motion.div 
                className="h-full bg-gradient-to-r from-cyan-500 to-violet-600"
                initial={{ width: 0 }}
                animate={{ width: `${(vs.idx / steps.length) * 100}%` }}
              />
            </div>
            <span className="font-mono text-[11px] text-muted whitespace-nowrap">{vs.idx} / {steps.length}</span>
          </div>
        )}

        {/* --- Main Layout --- */}
        <main className="grid grid-cols-1 lg:grid-cols-[1fr_320px] gap-6 flex-1">
          
          <div className="flex flex-col gap-6">
            {steps.length === 0 ? (
              <div className="flex-1 flex flex-col items-center justify-center gap-4 opacity-30 py-20">
                <Database size={64} strokeWidth={1} />
                <p className="font-mono text-sm">Upload file .bin để bắt đầu mô phỏng</p>
              </div>
            ) : (
              <div className="flex flex-col gap-6">
                
                {/* Phase 1: Disk Zone */}
                <div className="bg-[#06111f] border border-dashed border-[#1e4080] rounded-xl p-5 flex flex-col gap-4">
                  <div className="flex items-center gap-2">
                    <Database size={16} className="text-[#4a7abf]" />
                    <h2 className="font-mono text-[10px] font-bold uppercase tracking-widest text-[#4a7abf]">Disk — Bộ nhớ ngoài</h2>
                  </div>
                  <div className="text-[9px] font-mono text-muted uppercase tracking-widest border-b border-white/5 pb-1">Phase 1 — Tạo Initial Runs</div>
                  <div className="flex flex-wrap gap-3">
                    {Object.entries(vs.runs).map(([id, run]) => (
                      <motion.div 
                        key={id}
                        layout
                        className={`min-w-[160px] p-3 rounded-lg border transition-all ${
                          run.state === 'reading' ? 'bg-yellow-500/5 border-yellow-500/40' : 
                          run.state === 'sorted' ? 'bg-green-500/5 border-green-500/40' : 
                          'bg-[#0a1825] border-white/10'
                        }`}
                      >
                        <div className="flex justify-between items-center mb-2">
                          <span className="text-[10px] font-mono text-muted">Run {id}</span>
                          <span className={`text-[9px] font-mono px-1.5 py-0.5 rounded border ${
                            run.state === 'reading' ? 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30' :
                            run.state === 'sorted' ? 'bg-green-500/20 text-green-400 border-green-500/30' :
                            'bg-orange-500/20 text-orange-400 border-orange-500/30'
                          }`}>
                            {run.state === 'reading' ? '📖 Đọc' : run.state === 'sorted' ? '✓ Sort' : 'Chưa sort'}
                          </span>
                        </div>
                        <div className="font-mono text-[10px] text-cyan-400/80 flex flex-wrap gap-1.5">
                          {run.nums.slice(0, 6).map((n, i) => (
                            <span key={i} className={i === 0 && run.state === 'reading' ? 'text-yellow-400 font-bold' : ''}>{fmt(n)}</span>
                          ))}
                          {run.nums.length > 6 && <span className="opacity-40">+{run.nums.length - 6}</span>}
                        </div>
                      </motion.div>
                    ))}
                  </div>
                </div>

                {/* Transfer Arrow */}
                <div className="flex items-center gap-4 px-2">
                  <div className="flex-1 h-px bg-gradient-to-r from-[#1e4080] to-[#5b21b6]" />
                  <div className={`px-4 py-1 rounded-full border border-white/10 bg-[#111926] font-mono text-[10px] text-muted transition-all ${vs.transferMsg ? 'text-yellow-400 border-yellow-500/40 bg-yellow-500/5' : ''}`}>
                    {vs.transferMsg || '💾 Disk ↔ 🧠 RAM'}
                  </div>
                  <div className="flex-1 h-px bg-gradient-to-r from-[#5b21b6] to-[#1e4080]" />
                </div>

                {/* Phase 2: RAM Zone */}
                {vs.mergeZones.length > 0 && (
                  <div className="bg-[#0e0620] border border-[#5b21b6] rounded-xl p-5 flex flex-col gap-4">
                    <div className="flex items-center gap-2">
                      <Cpu size={16} className="text-[#9b6fd4]" />
                      <h2 className="font-mono text-[10px] font-bold uppercase tracking-widest text-[#9b6fd4]">RAM — Bộ nhớ trong</h2>
                    </div>
                    
                    <div className="text-[9px] font-mono text-muted uppercase tracking-widest border-b border-white/5 pb-1">Input Buffers</div>
                    <div className="flex flex-wrap gap-3">
                      {Object.entries(vs.bufs).map(([key, buf]) => {
                        const [p, g, b] = key.split('-').map(Number);
                        if (p !== vs.passId || g !== vs.groupId) return null;
                        return (
                          <div key={key} className={`min-w-[160px] p-3 rounded-lg border bg-[#0d061e] transition-all ${buf.nums.length === 0 ? 'opacity-40' : 'border-violet-500/40'}`}>
                            <div className="flex justify-between items-center mb-2">
                              <span className="text-[10px] font-mono text-[#9070cc]">Buffer {b}</span>
                              {buf.refilling && <span className="text-[8px] font-mono text-yellow-400 animate-pulse">⬇ Nạp...</span>}
                            </div>
                            <div className="font-mono text-[10px] flex flex-wrap gap-1.5">
                              {buf.nums.map((n, i) => (
                                <span key={i} className={i < buf.usedCount ? 'opacity-20 text-muted' : i === buf.usedCount ? 'text-violet-400 font-bold' : 'text-[#9d7eed]'}>
                                  {fmt(n)}
                                </span>
                              ))}
                            </div>
                          </div>
                        );
                      })}
                    </div>

                    <div className="text-[9px] font-mono text-muted uppercase tracking-widest border-b border-white/5 pb-1 mt-2">Min-Heap — Binary Tree</div>
                    <div className="bg-black/30 border border-violet-500/20 rounded-xl p-4 min-h-[300px] relative">
                      <div className="absolute top-3 left-4 text-[9px] font-mono text-[#9070cc] uppercase tracking-widest">Min-Heap</div>
                      <div className="absolute top-3 right-4 text-[9px] font-mono text-muted">{vs.heap.length} phần tử</div>
                      <div className="w-full h-full min-h-[260px]">
                        {renderHeap()}
                      </div>
                    </div>
                  </div>
                )}

                {/* Output Zone */}
                {vs.outputCount > 0 && (
                  <div className="bg-[#06111f] border border-dashed border-green-500/30 rounded-xl p-5 flex flex-col gap-4">
                    <div className="flex items-center gap-2">
                      <Database size={16} className="text-green-500" />
                      <h2 className="font-mono text-[10px] font-bold uppercase tracking-widest text-green-500">Disk — Output</h2>
                    </div>
                    <div className="bg-black/20 border border-green-500/20 rounded-lg p-4">
                      <div className="flex justify-between items-center mb-2">
                        <span className="text-[9px] font-mono text-green-500/70 uppercase tracking-widest">Luồng output (đã sắp xếp)</span>
                        <span className="text-[9px] font-mono text-muted">{vs.outputCount} phần tử</span>
                      </div>
                      <div className="font-mono text-[10px] text-green-500/60 flex flex-wrap gap-2 max-h-20 overflow-hidden">
                        {vs.output.map((v, i) => (
                          <span key={i} className={i === vs.output.length - 1 ? 'text-green-400 font-bold' : ''}>{fmt(v)}</span>
                        ))}
                      </div>
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>

          {/* --- Sidebar --- */}
          <aside className="flex flex-col gap-6">
            
            {/* File Info Card */}
            {params && (
              <div className="bg-[#0d1422] border border-white/5 rounded-xl p-4 flex flex-col gap-3">
                <h3 className="text-[10px] font-mono text-muted uppercase tracking-widest">📁 Thông tin File</h3>
                <div className="flex flex-col gap-2">
                  <div className="flex justify-between text-xs">
                    <span className="text-muted">Tên file</span>
                    <span className="font-mono text-cyan-400">{file?.name}</span>
                  </div>
                  <div className="flex justify-between text-xs">
                    <span className="text-muted">Kích thước</span>
                    <span className="font-mono">{(params.original_file_size / 1024 / 1024).toFixed(2)} MB</span>
                  </div>
                  <div className="flex justify-between text-xs">
                    <span className="text-muted">Mô phỏng</span>
                    <span className="font-mono text-yellow-400">{params.visualize_sample_size} số</span>
                  </div>
                </div>
              </div>
            )}

            {/* Controls Card */}
            <div className="bg-[#0d1422] border border-white/5 rounded-xl p-4 flex flex-col gap-4">
              <h3 className="text-[10px] font-mono text-muted uppercase tracking-widest">🎮 Điều khiển</h3>
              
              <AnimatePresence>
                {!paused && vs.idx === 0 && (
                  <motion.div 
                    initial={{ opacity: 0, x: -5 }}
                    animate={{ opacity: 1, x: 0 }}
                    exit={{ opacity: 0 }}
                    className="flex items-center gap-2 text-cyan-400 animate-pulse"
                  >
                    <ChevronRight size={14} />
                    <span className="text-[10px] font-mono uppercase">Nhấn Start để bắt đầu</span>
                  </motion.div>
                )}
              </AnimatePresence>

              <div className="grid grid-cols-2 gap-2">
                <button 
                  onClick={() => setPaused(!paused)}
                  disabled={steps.length === 0 || vs.idx >= steps.length}
                  className="flex items-center justify-center gap-2 px-3 py-2 bg-white/5 border border-white/10 rounded-lg text-xs font-medium hover:bg-white/10 transition-colors disabled:opacity-20"
                >
                  {paused ? <Play size={14} /> : <Pause size={14} />}
                  {paused ? 'Start' : 'Pause'}
                </button>
                <button 
                  onClick={handleNext}
                  disabled={steps.length === 0 || vs.idx >= steps.length}
                  className="flex items-center justify-center gap-2 px-3 py-2 bg-white/5 border border-white/10 rounded-lg text-xs font-medium hover:bg-white/10 transition-colors disabled:opacity-20"
                >
                  <SkipForward size={14} />
                  Step
                </button>
                <button 
                  onClick={handleBack}
                  disabled={history.length === 0}
                  className="flex items-center justify-center gap-2 px-3 py-2 bg-orange-500/10 border border-orange-500/20 text-orange-400 rounded-lg text-xs font-medium hover:bg-orange-500/20 transition-colors disabled:opacity-20"
                >
                  <RotateCcw size={14} />
                  Back
                </button>
                <button 
                  onClick={() => window.location.reload()}
                  className="flex items-center justify-center gap-2 px-3 py-2 bg-white/5 border border-white/10 rounded-lg text-xs font-medium hover:bg-white/10 transition-colors"
                >
                  Reset
                </button>
              </div>

              <div className="flex flex-col gap-2 mt-2">
                <div className="flex justify-between items-center">
                  <label className="text-[10px] font-mono text-muted uppercase">Speed</label>
                  <span className="text-[10px] font-mono text-cyan-400">{speed}x</span>
                </div>
                <input 
                  type="range" 
                  min="0.25" max="4" step="0.25" 
                  value={speed}
                  onChange={(e) => setSpeed(parseFloat(e.target.value))}
                  className="w-full h-1 bg-white/10 rounded-full appearance-none cursor-pointer accent-cyan-500"
                />
              </div>
            </div>

            {/* Event Log Card */}
            <div className="bg-[#0d1422] border border-white/5 rounded-xl p-4 flex flex-col gap-3 flex-1 min-h-[300px]">
              <h3 className="text-[10px] font-mono text-muted uppercase tracking-widest">📋 Event Log</h3>
              <div 
                ref={logListRef}
                className="flex flex-col gap-2 overflow-y-auto max-h-[400px] pr-2 scrollbar-thin scrollbar-thumb-white/10"
              >
                {vs.logEntries.map((entry, i) => (
                  <div key={i} className={`text-[10px] font-mono leading-relaxed flex gap-2 ${
                    entry.type === 'info' ? 'text-cyan-400' :
                    entry.type === 'ok' ? 'text-green-400' :
                    entry.type === 'warn' ? 'text-orange-400' :
                    entry.type === 'disk' ? 'text-[#3d7a96]' :
                    'text-muted'
                  }`}>
                    <span className="opacity-30 shrink-0">{entry.t}</span>
                    <span>{entry.msg}</span>
                  </div>
                ))}
                {vs.logEntries.length === 0 && (
                  <div className="text-[10px] font-mono text-muted/30 italic">Chưa có sự kiện nào...</div>
                )}
              </div>
            </div>

          </aside>
        </main>
      </div>
    </div>
  );
}
