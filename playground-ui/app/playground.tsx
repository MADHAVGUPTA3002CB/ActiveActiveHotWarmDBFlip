"use client";

import {
  Activity,
  ArrowRight,
  Check,
  ChevronRight,
  CircleAlert,
  Database,
  Gauge,
  History,
  LoaderCircle,
  Pause,
  Play,
  Radio,
  RefreshCw,
  RotateCcw,
  Settings2,
  ShieldCheck,
  Square,
  Trash2,
  X,
  Zap,
} from "lucide-react";
import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";

const API = "http://localhost:8090/api";
const SUPERVISOR = "http://localhost:8091/api";

type Settings = {
  active_rows_per_partition: number;
  retiring_rows_per_partition: number;
  active_pause_ms: number;
  retiring_pause_ms: number;
  payload_bytes: number;
  mode: "legacy_batch" | "target_rate_v1";
  active_target_tps: number;
  retiring_target_tps: number;
  active_rows_per_transaction: number;
  retiring_rows_per_transaction: number;
  active_workers: number;
  retiring_workers: number;
  max_queue_size: number;
  rate_window_seconds: number;
  min_achievement_percent: number;
  write_fence_mode: "warm_tracker_advisory_v1" | "hot_transactional_v1" | "optimistic_detach_v1";
  optimistic_admission_check_mode: "state_and_epoch_v1" | "state_only_v1";
};

type TrafficSnapshot = {
  scheduled_transactions: number;
  started_transactions: number;
  committed_transactions: number;
  failed_transactions: number;
  rejected_transactions: number;
  committed_rows: number;
  in_flight_transactions: number;
  queue_depth: number;
  committed_tps: number;
  rows_per_second: number;
  latency_p50_ms: number;
  latency_p95_ms: number;
  latency_p99_ms: number;
  last_error: string | null;
};

type Thresholds = {
  max_source_lag_bytes: number;
  max_sink_lag_records_per_partition: number;
  stable_samples: number;
  park_budget_ms: number;
  revert_reserve_ms: number;
  poll_ms: number;
};

type Sample = {
  at: string;
  source_lag_bytes: number;
  source_slot_active: boolean;
  source_lag_bytes_by_lane: Record<string, number>;
  source_slots_active_by_lane: Record<string, boolean>;
  sink_lag_records: number;
  max_sink_lag_records: number;
  sink_lag_by_partition: Record<string, number>;
  active_rows_total: number;
  retiring_rows_total: number;
  active_rows_per_second: number;
  retiring_rows_per_second: number;
  active_transactions_per_second: number;
  retiring_transactions_per_second: number;
  transactions: {
    active: TrafficSnapshot;
    retiring: TrafficSnapshot;
    target_tps: number;
    achieved_tps: number;
    achievement_percent: number;
    rate_valid: boolean;
  } | null;
  admission_ready: boolean;
  healthy_samples: number;
  tracker_states: Record<string, string>;
};

type Event = { stage: string; monotonic_ns: number; table?: string; duration_ns?: number };

type State = {
  environment: {
    mode: string;
    table_count: number;
    database_partitions_per_table: number;
    retiring_topic_partitions: number;
    kafka_partitions_per_leaf_topic: number;
    cell: string;
    retiring_timeslot: string;
    topology_mutable: boolean;
    topology_note: string;
    environment_generation_id: string;
    source_topology: "shared" | "isolated";
    source_connector_count: number;
    fence_source_lane: string;
    source_lanes: Array<{ lane: string; connector: string; slot: string; publication: string }>;
    supported_fence_wakeup_modes: Array<"passive" | "immediate_heartbeat">;
    supported_source_proof_modes: Array<"slot_lsn_v1" | "per_leaf_marker_v1" | "atomic_detach_marker_v1" | "parallel_atomic_detach_marker_v1">;
  };
  workload: {
    running: boolean;
    active_writer_alive: boolean;
    retiring_writer_alive: boolean;
    settings: Settings;
  };
  thresholds: Thresholds;
  latest: Sample | null;
  history: Sample[];
  connectors: Record<string, string>;
  flip: {
    status: "idle" | "running" | "succeeded" | "verification_failed" | "reverted" | "failed";
    run_id: string | null;
    events: Event[];
    timestamps_ns: Record<string, number>;
    elapsed_ns: number;
    durations_ns: Record<string, number>;
    detach_ns_by_table: Record<string, number>;
    outcome: string | null;
    verification_outcome: string | null;
    error: string | null;
    fence_wakeup_mode: "passive" | "immediate_heartbeat";
    source_proof_mode: "slot_lsn_v1" | "per_leaf_marker_v1" | "atomic_detach_marker_v1" | "parallel_atomic_detach_marker_v1";
  };
  metrics_error: string | null;
};

type SupervisorState = {
  status: "idle" | "running" | "completed" | "failed";
  phase: string;
  step: number;
  total_steps: number;
  table_count: number | null;
  source_topology: "shared" | "isolated" | null;
  job_id: string | null;
  environment_generation_id: string | null;
  started_at_utc: string | null;
  finished_at_utc: string | null;
  error: string | null;
  error_code: string | null;
  recovery_hint: string | null;
  logs: string[];
};

type SavedRun = {
  artifact_type: "ownership_grant" | "completed_run";
  run_id: string;
  attempt_id: string | null;
  recorded_at_utc: string | null;
  outcome: "success" | "failed" | "reverted";
  verification_outcome: "pending" | "passed" | "failed" | "not_run";
  table_count: number;
  profile: string | null;
  environment_generation_id: string | null;
  source_topology: string | null;
  fence_wakeup_mode: string;
  fence_wakeup_applied: boolean | null;
  source_proof_mode: string;
  tracker_lock_ns: number | null;
  source_proof_ns: number | null;
  atomic_detach_marker_ns: number | null;
  parallel_detach_wall_ns: number | null;
  fence_wakeup_ns: number | null;
  slot_wait_after_wakeup_ns: number | null;
  capture_e_ns: number | null;
  sink_proof_ns: number | null;
  grant_ns: number | null;
  forward_until_failure_ns: number | null;
  revert_ns: number | null;
  writer_park_ns: number | null;
  whole_lifecycle_ns: number | null;
  source_lag_bytes: number | null;
  sink_lag_records: number | null;
  historical_saved_run: true;
  workload_mode: "legacy_batch" | "target_rate_v1" | null;
  target_tps: number | null;
  achieved_tps: number | null;
  write_fence_mode: string;
  optimistic_admission_check_mode: string;
  transaction_shape: string;
  operations_per_api_batch: number | null;
  ownership_reads_per_api_batch: number | null;
  ownership_epoch_checks_per_api_batch: number | null;
  postgres_transactions_per_api_batch: number | null;
  hot_fence_park_ns: number | null;
  admission_fence_ns: number | null;
  in_flight_resolution_ns: number | null;
};

const STAGES = [
  ["t1", "Admit", "Thresholds proved"],
  ["t2", "Lock", "Ownership locked"],
  ["t2q", "Park", "Retiring writer parked"],
  ["t4", "Detach", "Leaves detached"],
  ["t7", "Source proof", "WAL fence reached"],
  ["t8", "Capture E", "Kafka targets frozen"],
  ["t11", "Sink proof", "Warm offsets reached"],
  ["t13", "Grant", "Warm owns data"],
] as const;

function formatNumber(value: number | null | undefined) {
  if (value === undefined || value === null) return "—";
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 }).format(value);
}

function formatBytes(value: number | null | undefined) {
  if (value === undefined || value === null) return "—";
  if (value < 1024) return `${value} B`;
  if (value < 1024 ** 2) return `${(value / 1024).toFixed(1)} KiB`;
  return `${(value / 1024 ** 2).toFixed(2)} MiB`;
}

function formatDuration(ns: number | null | undefined) {
  if (ns === undefined || ns === null) return "—";
  const ms = ns / 1_000_000;
  if (ms < 1) return `${(ns / 1000).toFixed(0)} µs`;
  if (ms < 1000) return `${ms.toFixed(ms < 10 ? 2 : 1)} ms`;
  return `${(ms / 1000).toFixed(3)} s`;
}

function formatFenceExperiment(run: SavedRun) {
  if (run.source_proof_mode === "parallel_atomic_detach_marker_v1") {
    return "H · parallel atomic detach + per-leaf marker";
  }
  if (run.source_proof_mode === "atomic_detach_marker_v1") {
    return "G · atomic detach + per-leaf marker";
  }
  if (run.source_proof_mode === "per_leaf_marker_v1") {
    return "F · per-leaf CDC marker receipts";
  }
  if (run.write_fence_mode === "optimistic_detach_v1") {
    return "E · batch-admitted optimistic detach";
  }
  if (run.write_fence_mode === "hot_transactional_v1") {
    return "D · hot-local transaction fence";
  }
  const variant = run.source_topology === "shared" ? "A" : run.source_topology === "isolated" ? "B" : null;
  if (!variant || !["passive", "immediate_heartbeat"].includes(run.fence_wakeup_mode)) {
    return "legacy/unknown";
  }
  return run.fence_wakeup_mode === "immediate_heartbeat"
    ? `${variant}+ · immediate fence nudge`
    : `${variant} · passive control`;
}

async function request(path: string, options?: RequestInit) {
  const response = await fetch(`${API}${path}`, {
    ...options,
    headers: { "Content-Type": "application/json", ...options?.headers },
  });
  const payload = await response.json();
  if (!response.ok || !payload.ok) throw new Error(payload.error || `HTTP ${response.status}`);
  return payload.data;
}

async function supervisorRequest(path: string, options?: RequestInit) {
  const response = await fetch(`${SUPERVISOR}${path}`, {
    ...options,
    headers: {
      Accept: "application/json",
      ...(options?.body ? { "Content-Type": "application/json" } : {}),
      ...options?.headers,
    },
  });
  const payload = await response.json();
  if (!response.ok || !payload.ok) throw new Error(payload.error || `HTTP ${response.status}`);
  return payload.data;
}

function formatRecordedAt(value: string | null) {
  if (!value) return "Time unavailable";
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? "Time unavailable" : parsed.toLocaleString();
}

function MiniChart({ data, keyName, color }: { data: Sample[]; keyName: "source_lag_bytes" | "sink_lag_records"; color: string }) {
  const values = data.slice(-60).map((item) => item[keyName]);
  const max = Math.max(...values, 1);
  const points = values
    .map((value, index) => `${(index / Math.max(1, values.length - 1)) * 100},${42 - (value / max) * 38}`)
    .join(" ");
  return (
    <svg className="mini-chart" viewBox="0 0 100 44" preserveAspectRatio="none" aria-label={`${keyName} history`}>
      <line x1="0" y1="42" x2="100" y2="42" className="chart-base" />
      {points && <polyline points={points} fill="none" stroke={color} strokeWidth="1.8" vectorEffect="non-scaling-stroke" />}
    </svg>
  );
}

function NumberField({ label, value, onChange, unit, min = 0, max, disabled = false }: { label: string; value: number; onChange: (value: number) => void; unit?: string; min?: number; max?: number; disabled?: boolean }) {
  return (
    <label className="field">
      <span>{label}</span>
      <div className="input-shell">
        <input type="number" min={min} max={max} value={value} disabled={disabled} onChange={(event) => onChange(Number(event.target.value))} />
        {unit && <small>{unit}</small>}
      </div>
    </label>
  );
}

function StatusDot({ state }: { state: string }) {
  const healthy = state === "RUNNING" || state === "hot_primary" || state === "warm_primary";
  return <span className={`status-dot ${healthy ? "healthy" : "warning"}`} aria-hidden="true" />;
}

export function Playground() {
  const [state, setState] = useState<State | null>(null);
  const [workload, setWorkload] = useState<Settings | null>(null);
  const [thresholds, setThresholds] = useState<Thresholds | null>(null);
  const [fenceWakeupMode, setFenceWakeupMode] = useState<"passive" | "immediate_heartbeat">("passive");
  const [sourceProofMode, setSourceProofMode] = useState<"slot_lsn_v1" | "per_leaf_marker_v1" | "atomic_detach_marker_v1" | "parallel_atomic_detach_marker_v1">("slot_lsn_v1");
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState<string | null>(null);
  const [connected, setConnected] = useState(false);
  const [supervisor, setSupervisor] = useState<SupervisorState | null>(null);
  const [supervisorAvailable, setSupervisorAvailable] = useState(false);
  const [savedRuns, setSavedRuns] = useState<SavedRun[]>([]);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [restartOpen, setRestartOpen] = useState(false);
  const [restartTableCount, setRestartTableCount] = useState(5);
  const [restartSourceTopology, setRestartSourceTopology] = useState<"shared" | "isolated">("shared");
  const [restartConfirmation, setRestartConfirmation] = useState("");
  const [restartError, setRestartError] = useState<string | null>(null);
  const [restartInitiatedJobId, setRestartInitiatedJobId] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      const next = (await request("/state")) as State;
      setState(next);
      setWorkload((current) => current ?? next.workload.settings);
      setThresholds((current) => current ?? next.thresholds);
      setFenceWakeupMode((current) => next.flip.status === "running" ? next.flip.fence_wakeup_mode : current);
      setSourceProofMode((current) => next.flip.status === "running" ? next.flip.source_proof_mode : current);
      setConnected(true);
    } catch (caught) {
      setConnected(false);
      setError(caught instanceof Error ? caught.message : "Playground API unavailable");
    }
  }, []);

  useEffect(() => {
    const initial = window.setTimeout(refresh, 0);
    const timer = window.setInterval(refresh, 750);
    return () => {
      window.clearTimeout(initial);
      window.clearInterval(timer);
    };
  }, [refresh]);

  const refreshSupervisor = useCallback(async () => {
    try {
      const nextSupervisor = (await supervisorRequest("/state")) as SupervisorState;
      setSupervisor(nextSupervisor);
      setSupervisorAvailable(true);
    } catch {
      setSupervisorAvailable(false);
    }
  }, []);

  const refreshSavedRuns = useCallback(async () => {
    try {
      const nextRuns = (await supervisorRequest("/runs")) as SavedRun[];
      setSavedRuns(nextRuns);
      setSelectedRunId((current) => current ?? nextRuns[0]?.run_id ?? null);
      setSupervisorAvailable(true);
    } catch {
      setSupervisorAvailable(false);
    }
  }, []);

  useEffect(() => {
    const initial = window.setTimeout(refreshSupervisor, 0);
    const timer = window.setInterval(refreshSupervisor, 1000);
    return () => {
      window.clearTimeout(initial);
      window.clearInterval(timer);
    };
  }, [refreshSupervisor]);

  useEffect(() => {
    const initial = window.setTimeout(refreshSavedRuns, 0);
    const timer = window.setInterval(refreshSavedRuns, 3000);
    return () => {
      window.clearTimeout(initial);
      window.clearInterval(timer);
    };
  }, [refreshSavedRuns]);

  useEffect(() => {
    if (supervisor?.status !== "completed") return;
    const timer = window.setTimeout(() => {
      setWorkload(null);
      setThresholds(null);
      setError(null);
      void refresh();
    }, 350);
    return () => window.clearTimeout(timer);
  }, [supervisor?.status, supervisor?.job_id, refresh]);

  const act = async (name: string, callback: () => Promise<unknown>) => {
    setPending(name);
    setError(null);
    try {
      await callback();
      await refresh();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Action failed");
    } finally {
      setPending(null);
    }
  };

  const saveWorkload = (event: FormEvent) => {
    event.preventDefault();
    if (!workload) return;
    act("workload", () => request("/workload", { method: "PATCH", body: JSON.stringify(workload) }));
  };

  const saveThresholds = (event: FormEvent) => {
    event.preventDefault();
    if (!thresholds) return;
    act("thresholds", () => request("/thresholds", { method: "PATCH", body: JSON.stringify(thresholds) }));
  };

  const startNewExperiment = async (event: FormEvent) => {
    event.preventDefault();
    if (restartConfirmation !== "RESET") return;
    if (!connected) {
      setRestartError("Control API unavailable at localhost:8090. Start it and retry; no volumes were deleted.");
      return;
    }
    setRestartError(null);
    try {
      const next = (await supervisorRequest("/environment/restart", {
        method: "POST",
        body: JSON.stringify({ table_count: restartTableCount, source_topology: restartSourceTopology, confirmation: restartConfirmation }),
      })) as SupervisorState;
      setSupervisor(next);
      setRestartInitiatedJobId(next.job_id);
      setRestartConfirmation("");
    } catch (caught) {
      setRestartError(caught instanceof Error ? caught.message : "Restart request failed");
    }
  };

  const completedStages = useMemo(() => new Set(state?.flip.events.map((event) => event.stage) ?? []), [state]);
  const latest = state?.latest;
  const detachEvents = state?.flip.events.filter((event) => event.stage.startsWith("t4_")) ?? [];
  const detachDone = state ? detachEvents.length >= state.environment.table_count : false;
  const optimisticDetach = workload?.write_fence_mode === "optimistic_detach_v1";
  const hotGateMode = workload?.write_fence_mode === "hot_transactional_v1" || optimisticDetach;
  const visibleStages = optimisticDetach ? [
    ["t1", "Admit", "Thresholds proved"],
    ["t2", "Lock", "Ownership locked"],
    ["t2f", "Fence", "New retiring APIs stopped"],
    ["t4", "Detach", "Leaves detached"],
    ["t2q", "Resolve", "In-flight APIs finished or rolled back"],
    ["t7", "Source proof", sourceProofMode !== "slot_lsn_v1" ? "Leaf markers observed" : "WAL fence reached"],
    ["t8", "Capture E", sourceProofMode !== "slot_lsn_v1" ? "Marker offsets persisted" : "Kafka targets frozen"],
    ["t11", "Sink proof", sourceProofMode !== "slot_lsn_v1" ? "Warm receipts visible" : "Warm offsets reached"],
    ["t13", "Grant", "Warm owns data"],
  ] as const : STAGES;
  const stageComplete = (stage: string) => stage === "t4" ? detachDone : completedStages.has(stage);
  const restartRunning = supervisor?.status === "running";
  const controlsDisabled = pending !== null || restartRunning;
  const flipAllowed = Boolean(latest?.admission_ready && state?.workload.retiring_writer_alive && state?.flip.status !== "running" && !restartRunning);
  const selectedRun = savedRuns.find((run) => run.run_id === selectedRunId) ?? savedRuns[0] ?? null;
  const restartJustCompleted = Boolean(restartInitiatedJobId && supervisor?.job_id === restartInitiatedJobId && supervisor.status === "completed");
  const controlApiRecoveryCommand = `make playground-api-rf3 TABLE_COUNT=${state?.environment.table_count ?? restartTableCount} SOURCE_TOPOLOGY=${state?.environment.source_topology ?? restartSourceTopology}`;
  const restartMayHaveChangedVolumes = Boolean(supervisor?.status === "failed" && supervisor.step > 1);

  const liveDuration = (key: string) => {
    if (key === "atomic_detach_marker_ns") {
      const values = Object.values(state?.flip.detach_ns_by_table ?? {});
      return values.length === state?.environment.table_count
        ? values.reduce((total, value) => total + value, 0)
        : undefined;
    }
    if (key === "parallel_detach_wall_ns") {
      const event = state?.flip.events.find(
        (item) => item.stage === "t4_parallel"
      );
      if (event?.duration_ns !== undefined) return event.duration_ns;
    }
    const completed = state?.flip.durations_ns[key];
    if (completed !== undefined) return completed;
    if (!state || state.flip.status !== "running") return undefined;
    const timestamps = state.flip.timestamps_ns;
    const bounds: Record<string, [string, string]> = {
      tracker_lock_ns: hotGateMode ? ["t2h", "t2w"] : ["t1", "t2"],
      hot_fence_park_ns: ["t2", "t2h"],
      admission_fence_ns: ["t2w", "t2f"],
      in_flight_resolution_ns: ["t2f", "t2q"],
      source_proof_ns: ["t5", "t7"],
      parallel_detach_wall_ns: ["t3_parallel", "t4_parallel"],
      fence_wakeup_ns: ["t6", "t6w"],
      slot_wait_after_wakeup_ns: ["t6w", "t7"],
      capture_e_ns: ["t7", "t8"],
      sink_proof_ns: ["t8", "t11"],
      grant_ns: ["t11", "t13"],
      writer_park_ns: ["t2", "t13"],
    };
    const [start, end] = bounds[key] ?? [];
    if (!start || timestamps[start] === undefined) return undefined;
    return (timestamps[end] ?? state.flip.elapsed_ns) - timestamps[start];
  };

  if (!state || !workload || !thresholds) {
    return (
      <main className="loading-page">
        <LoaderCircle className="spin" size={24} />
        <h1>{restartRunning ? "Rebuilding the experiment" : "Connecting to Flipbench"}</h1>
        <p>{restartRunning ? `Step ${supervisor?.step} of ${supervisor?.total_steps}: ${supervisor?.phase}` : "Waiting for the local control API at localhost:8090."}</p>
        {restartRunning && <div className="loading-progress"><div className="progress-bar"><i style={{ width: `${Math.max(4, ((supervisor?.step ?? 0) / Math.max(1, supervisor?.total_steps ?? 1)) * 100)}%` }} /></div><span>{savedRuns.length} historical results remain preserved.</span></div>}
        {!restartRunning && error && <div className="alert error"><CircleAlert size={16} />{error}</div>}
        {!supervisorAvailable && <p className="supervisor-note">Restart control is unavailable at localhost:8091.</p>}
      </main>
    );
  }

  const ownership = latest?.tracker_states?.[state.environment.retiring_timeslot] ?? "unknown";
  const sourceRatio = Math.min(100, ((latest?.source_lag_bytes ?? 0) / Math.max(1, thresholds.max_source_lag_bytes)) * 100);
  const sinkRatio = Math.min(100, ((latest?.max_sink_lag_records ?? 0) / Math.max(1, thresholds.max_sink_lag_records_per_partition)) * 100);

  return (
    <main className="app-shell">
      <header className="topbar">
        <div className="brand">
          <div className="brand-mark"><Database size={19} /></div>
          <div><h1>Flipbench</h1><span>Hot → Warm control room</span></div>
        </div>
        <div className="top-status">
          <span className="live-pill"><Radio size={13} /> LIVE LOCAL</span>
          <span><StatusDot state={state.connectors.source} />Debezium {state.connectors.source.toLowerCase()}</span>
          <span><StatusDot state={ownership} />Retiring: {ownership}</span>
          <span className={connected ? "connected" : "disconnected"}>{connected ? "Connected" : "Disconnected"}</span>
        </div>
      </header>

      <div className="workspace">
        <aside className="control-panel">
          <section className="panel-section">
            <div className="section-title"><Settings2 size={15} /><span>Scenario topology</span></div>
            <div className="field-grid two">
              <NumberField label="Tables" value={state.environment.table_count} onChange={() => undefined} disabled />
              <NumberField label="DB leaves / table" value={state.environment.database_partitions_per_table} onChange={() => undefined} disabled />
            </div>
            <div className="topology-row"><span>Retiring leaf topics</span><strong>{state.environment.table_count}</strong></div>
            <div className="topology-row"><span>Kafka partitions / topic</span><strong>{state.environment.kafka_partitions_per_leaf_topic}</strong></div>
            <div className="topology-row"><span>Source topology</span><strong>{state.environment.source_topology === "isolated" ? "B · isolated" : "A · shared"}</strong></div>
            <div className="topology-row"><span>Debezium sources</span><strong>{state.environment.source_connector_count}</strong></div>
            <div className="info-note"><CircleAlert size={14} /><span>{state.environment.topology_note}</span></div>
            <button className="button danger full restart-control" type="button" disabled={restartRunning || state.flip.status === "running"} onClick={() => { setRestartOpen(true); setRestartTableCount(state.environment.table_count); setRestartSourceTopology(state.environment.source_topology); setRestartError(null); setRestartInitiatedJobId(null); }}>
              {restartRunning ? <LoaderCircle className="spin" size={15} /> : <Trash2 size={15} />} {restartRunning ? "Restarting system…" : "New experiment"}
            </button>
            {!supervisorAvailable && <div className="supervisor-note">Restart control unavailable. Run <code>make playground-supervisor</code> in a second terminal.</div>}
          </section>

          <form className="panel-section" onSubmit={saveWorkload}>
            <div className="section-title"><Zap size={15} /><span>Live workload</span><em>{state.workload.running ? "RUNNING" : "STOPPED"}</em></div>
            <label className="field"><span>Workload model</span><div className="select-shell"><select value={workload.mode} disabled={state.workload.running || controlsDisabled} onChange={(event) => setWorkload({ ...workload, mode: event.target.value as Settings["mode"] })}><option value="target_rate_v1">Target transactions / second</option><option value="legacy_batch">Legacy rows / batch</option></select></div></label>
            {workload.mode === "target_rate_v1" ? <>
              <label className="field"><span>Ownership fence</span><div className="select-shell"><select value={workload.write_fence_mode} disabled={state.workload.running || controlsDisabled} onChange={(event) => {
                const mode = event.target.value as Settings["write_fence_mode"];
                setWorkload({
                  ...workload,
                  write_fence_mode: mode,
                  optimistic_admission_check_mode: "state_and_epoch_v1",
                });
                if (mode !== "optimistic_detach_v1") setSourceProofMode("slot_lsn_v1");
                if (mode === "hot_transactional_v1" || mode === "optimistic_detach_v1") setFenceWakeupMode("immediate_heartbeat");
              }}>
                <option value="warm_tracker_advisory_v1">A/B/B+ — warm tracker guard</option>
                <option value="hot_transactional_v1" disabled={state.environment.source_topology !== "isolated"}>D — hot-local transaction fence</option>
                <option value="optimistic_detach_v1" disabled={state.environment.source_topology !== "isolated"}>E — batch admission + separate commits</option>
              </select></div></label>
              {workload.write_fence_mode === "optimistic_detach_v1" && <label className="field"><span>API batch admission check</span><div className="select-shell"><select value={workload.optimistic_admission_check_mode} disabled={state.workload.running || controlsDisabled} onChange={(event) => {
                const mode = event.target.value as Settings["optimistic_admission_check_mode"];
                setWorkload({ ...workload, optimistic_admission_check_mode: mode });
                if (mode === "state_only_v1") {
                  setSourceProofMode("parallel_atomic_detach_marker_v1");
                  setFenceWakeupMode("passive");
                } else if (sourceProofMode === "parallel_atomic_detach_marker_v1") {
                  setSourceProofMode("slot_lsn_v1");
                  setFenceWakeupMode("immediate_heartbeat");
                }
              }}>
                <option value="state_and_epoch_v1">E–G — state + epoch</option>
                <option value="state_only_v1">H — state only</option>
              </select></div></label>}
              <div className="topology-row"><span>Total requested rate</span><strong>{formatNumber(workload.active_target_tps + workload.retiring_target_tps)} TPS</strong></div>
              <div className="field-grid two">
                <NumberField label="Active target TPS" value={workload.active_target_tps} min={1} max={99_999} onChange={(value) => setWorkload({ ...workload, active_target_tps: value })} />
                <NumberField label="Retiring target TPS" value={workload.retiring_target_tps} min={1} max={99_999} onChange={(value) => setWorkload({ ...workload, retiring_target_tps: value })} />
              </div>
              <div className="field-grid two">
                <NumberField label="Active rows / transaction" value={workload.active_rows_per_transaction} min={1} max={100_000} onChange={(value) => setWorkload({ ...workload, active_rows_per_transaction: value })} />
                <NumberField label="Retiring rows / transaction" value={workload.retiring_rows_per_transaction} min={1} max={100_000} onChange={(value) => setWorkload({ ...workload, retiring_rows_per_transaction: value })} />
              </div>
              <div className="field-grid two">
                <NumberField label="Active DB workers" value={workload.active_workers} min={1} max={63} disabled={state.workload.running} onChange={(value) => setWorkload({ ...workload, active_workers: value })} />
                <NumberField label="Retiring DB workers" value={workload.retiring_workers} min={1} max={63} disabled={state.workload.running} onChange={(value) => setWorkload({ ...workload, retiring_workers: value })} />
              </div>
              <div className="field-grid two">
                <NumberField label="Maximum pending queue" value={workload.max_queue_size} min={1} max={100_000} disabled={state.workload.running} onChange={(value) => setWorkload({ ...workload, max_queue_size: value })} />
                <NumberField label="Rate window" value={workload.rate_window_seconds} unit="seconds" min={1} max={60} disabled={state.workload.running} onChange={(value) => setWorkload({ ...workload, rate_window_seconds: value })} />
              </div>
              <NumberField label="Minimum achieved rate" value={workload.min_achievement_percent} unit="% of target" min={1} max={100} onChange={(value) => setWorkload({ ...workload, min_achievement_percent: value })} />
              <div className="info-note"><CircleAlert size={14} /><span>{workload.write_fence_mode === "optimistic_detach_v1" ? workload.optimistic_admission_check_mode === "state_only_v1" ? `Variant H checks only that the hot gate is open in the first of ${state.environment.table_count} separately committed operations. It sends no ownership epoch; detach errors protect later operations.` : `Variants E–G check state and epoch in the first of ${state.environment.table_count} separately committed operations; the remaining operations make no gate read.` : workload.write_fence_mode === "hot_transactional_v1" ? "Variant D checks the durable ownership epoch once inside every hot transaction and makes no warm PostgreSQL call. It requires isolated sources and the immediate fence nudge." : "TPS is aggregate across all tables. One transaction targets one table and is counted only after PostgreSQL COMMIT succeeds."}</span></div>
            </> : <>
              <NumberField label="Active rows / partition / batch" value={workload.active_rows_per_partition} min={1} onChange={(value) => setWorkload({ ...workload, active_rows_per_partition: value })} />
              <NumberField label="Retiring rows / partition / batch" value={workload.retiring_rows_per_partition} onChange={(value) => setWorkload({ ...workload, retiring_rows_per_partition: value })} />
              <div className="field-grid two">
                <NumberField label="Active pause" value={workload.active_pause_ms} unit="ms" onChange={(value) => setWorkload({ ...workload, active_pause_ms: value })} />
                <NumberField label="Retiring pause" value={workload.retiring_pause_ms} unit="ms" onChange={(value) => setWorkload({ ...workload, retiring_pause_ms: value })} />
              </div>
            </>}
            <NumberField label="Payload size" value={workload.payload_bytes} unit="bytes" min={1} onChange={(value) => setWorkload({ ...workload, payload_bytes: value })} />
            <button className="button secondary full" type="submit" disabled={controlsDisabled}>
              {pending === "workload" ? <LoaderCircle className="spin" size={15} /> : <RefreshCw size={15} />} Apply live settings
            </button>
            <div className="button-row">
              <button className="button primary" type="button" disabled={state.workload.running || controlsDisabled || ownership !== "hot_primary"} onClick={() => act("start", () => request("/workload/start", { method: "POST" }))}>
                <Play size={15} fill="currentColor" /> Start writes
              </button>
              <button className="button ghost" type="button" disabled={!state.workload.running || state.flip.status === "running" || controlsDisabled} onClick={() => act("stop", () => request("/workload/stop", { method: "POST" }))}>
                <Square size={14} fill="currentColor" /> Stop
              </button>
            </div>
          </form>

          <form className="panel-section" onSubmit={saveThresholds}>
            <div className="section-title"><ShieldCheck size={15} /><span>Flip admission</span></div>
            <NumberField label="Max WAL → Debezium lag" value={thresholds.max_source_lag_bytes} unit="bytes" onChange={(value) => setThresholds({ ...thresholds, max_source_lag_bytes: value })} />
            <NumberField label="Max Kafka → warm lag" value={thresholds.max_sink_lag_records_per_partition} unit="records / partition" onChange={(value) => setThresholds({ ...thresholds, max_sink_lag_records_per_partition: value })} />
            <div className="field-grid two">
              <NumberField label="Stable samples" value={thresholds.stable_samples} min={1} onChange={(value) => setThresholds({ ...thresholds, stable_samples: value })} />
              <NumberField label="Poll interval" value={thresholds.poll_ms} unit="ms" min={10} onChange={(value) => setThresholds({ ...thresholds, poll_ms: value })} />
            </div>
            <div className="field-grid two">
              <NumberField label="Park budget" value={thresholds.park_budget_ms} unit="ms" min={100} onChange={(value) => setThresholds({ ...thresholds, park_budget_ms: value })} />
              <NumberField label="Revert reserve" value={thresholds.revert_reserve_ms} unit="ms" onChange={(value) => setThresholds({ ...thresholds, revert_reserve_ms: value })} />
            </div>
            <label className="field"><span>Fence wake-up experiment</span><div className="select-shell"><select value={fenceWakeupMode} disabled={controlsDisabled || state.flip.status === "running" || workload.optimistic_admission_check_mode === "state_only_v1"} onChange={(event) => setFenceWakeupMode(event.target.value as "passive" | "immediate_heartbeat")}>
              <option value="passive">{state.environment.source_topology === "isolated" ? "B — Passive heartbeat (control)" : "A — Passive heartbeat (control)"}</option>
              <option value="immediate_heartbeat">{state.environment.source_topology === "isolated" ? "B+ — Immediate fence nudge" : "A+ — Immediate fence nudge"}</option>
            </select></div></label>
            <label className="field"><span>Source and sink proof</span><div className="select-shell"><select value={sourceProofMode} disabled={controlsDisabled || state.flip.status === "running"} onChange={(event) => {
              const mode = event.target.value as "slot_lsn_v1" | "per_leaf_marker_v1" | "atomic_detach_marker_v1" | "parallel_atomic_detach_marker_v1";
              setSourceProofMode(mode);
              if (mode !== "slot_lsn_v1") setFenceWakeupMode("passive");
            }}>
              <option value="slot_lsn_v1">Existing — LSN + committed offsets</option>
              <option value="per_leaf_marker_v1" disabled={state.environment.source_topology !== "isolated" || workload.write_fence_mode !== "optimistic_detach_v1" || workload.optimistic_admission_check_mode !== "state_and_epoch_v1"}>F — Per-leaf CDC marker receipts</option>
              <option value="atomic_detach_marker_v1" disabled={state.environment.source_topology !== "isolated" || workload.write_fence_mode !== "optimistic_detach_v1" || workload.optimistic_admission_check_mode !== "state_and_epoch_v1"}>G — Atomic detach + per-leaf marker</option>
              <option value="parallel_atomic_detach_marker_v1" disabled={state.environment.source_topology !== "isolated" || workload.write_fence_mode !== "optimistic_detach_v1" || workload.optimistic_admission_check_mode !== "state_only_v1"}>H — Parallel atomic detach + markers</option>
            </select></div></label>
            <div className="info-note"><CircleAlert size={14} /><span>{sourceProofMode === "parallel_atomic_detach_marker_v1" ? "Variant H starts one independent detach + marker transaction for every retiring leaf at the same time. If any leaf fails, all workers finish and every detached leaf is reattached before the gate reopens." : sourceProofMode === "atomic_detach_marker_v1" ? "Variant G commits each blocking leaf detach and its unique CDC marker in the same hot PostgreSQL transaction, then waits for that exact marker in Kafka and warm PostgreSQL." : sourceProofMode === "per_leaf_marker_v1" ? "Variant F emits one durable PostgreSQL marker per retiring leaf after concurrent detaches, observes its exact Kafka offset, and waits for the matching warm JDBC receipt." : "The existing proof waits for the migration slot LSN and then the JDBC sink consumer offsets."}</span></div>
            <button className="button secondary full" type="submit" disabled={controlsDisabled}>
              {pending === "thresholds" ? <LoaderCircle className="spin" size={15} /> : <Check size={15} />} Apply thresholds
            </button>
          </form>
        </aside>

        <section className="dashboard">
          {restartRunning && <div className="alert restart-alert"><LoaderCircle className="spin" size={16} /><span>Rebuilding the local experiment: step {supervisor.step} of {supervisor.total_steps} — {supervisor.phase}. Live metrics may disconnect briefly.</span><button onClick={() => setRestartOpen(true)}>View</button></div>}
          {!restartRunning && (error || state.metrics_error) && <div className="alert error"><CircleAlert size={16} /><span>{error || state.metrics_error}</span><button onClick={() => setError(null)}>Dismiss</button></div>}
          {state.flip.status === "verification_failed" && <div className="alert warning"><CircleAlert size={16} /><span>Warm ownership was granted, but post-grant parity verification failed. Cleanup is not eligible.</span></div>}
          {state.flip.status === "reverted" && <div className="alert warning"><RotateCcw size={16} /><span>The forward flip missed its safety condition and reverted to hot ownership. Revert timing is preserved below.</span></div>}
          <div className="dashboard-heading">
            <div><span className="eyebrow">CELL {state.environment.cell}</span><h2>Live replication health</h2><p>Measured from the PostgreSQL slot and Kafka consumer offsets every 500 ms.</p></div>
            <button className="icon-button" onClick={refresh} aria-label="Refresh metrics" title="Refresh metrics"><RefreshCw size={17} /></button>
          </div>

          <div className="metrics-grid">
            <article className="metric-card source">
              <div className="metric-head"><span>WAL → Debezium lag</span><Database size={16} /></div>
              <strong>{formatBytes(latest?.source_lag_bytes)}</strong>
              <div className="threshold-line"><span>Threshold {formatBytes(thresholds.max_source_lag_bytes)}</span><span>{sourceRatio.toFixed(0)}%</span></div>
              {state.environment.source_topology === "isolated" && <div className="threshold-line"><span>Active {formatBytes(latest?.source_lag_bytes_by_lane?.active)}</span><span>Migration {formatBytes(latest?.source_lag_bytes_by_lane?.migration)}</span></div>}
              <div className="meter"><i style={{ width: `${sourceRatio}%` }} /></div>
              <MiniChart data={state.history} keyName="source_lag_bytes" color="#ff7448" />
            </article>
            <article className="metric-card sink">
              <div className="metric-head"><span>Kafka → warm lag</span><Activity size={16} /></div>
              <strong>{formatNumber(latest?.sink_lag_records)} <small>records total</small></strong>
              <div className="threshold-line"><span>Worst partition {formatNumber(latest?.max_sink_lag_records)} / {thresholds.max_sink_lag_records_per_partition}</span><span>{sinkRatio.toFixed(0)}%</span></div>
              <div className="meter"><i style={{ width: `${sinkRatio}%` }} /></div>
              <MiniChart data={state.history} keyName="sink_lag_records" color="#29b6a4" />
            </article>
            <article className="metric-card throughput">
              <div className="metric-head"><span>Committed transaction rate</span><Gauge size={16} /></div>
              {latest?.transactions ? <>
                <div className="rate-pair"><div><strong>{formatNumber(latest.transactions.achieved_tps)}</strong><span>achieved TPS</span></div><div><strong>{formatNumber(latest.transactions.target_tps)}</strong><span>target TPS</span></div></div>
                <div className="threshold-line"><span>{formatNumber(latest.transactions.achievement_percent)}% of target</span><span>{latest.transactions.rate_valid ? "admissible" : "below minimum"}</span></div>
                <div className="meter"><i style={{ width: `${Math.min(100, latest.transactions.achievement_percent)}%` }} /></div>
                <div className="totals"><span>{formatNumber(latest.active_transactions_per_second)} active TPS</span><span>{formatNumber(latest.retiring_transactions_per_second)} retiring TPS</span></div>
                <div className="totals"><span>{formatNumber(latest.transactions.active.queue_depth + latest.transactions.retiring.queue_depth)} queued</span><span>{formatNumber(latest.transactions.active.rejected_transactions + latest.transactions.retiring.rejected_transactions)} rejected/missed</span></div>
                <div className="totals"><span>p95 {formatNumber(Math.max(latest.transactions.active.latency_p95_ms, latest.transactions.retiring.latency_p95_ms))} ms</span><span>{formatNumber(latest.active_rows_per_second + latest.retiring_rows_per_second)} rows/s</span></div>
              </> : <>
                <div className="rate-pair"><div><strong>{formatNumber(latest?.active_rows_per_second)}</strong><span>active rows/s</span></div><div><strong>{formatNumber(latest?.retiring_rows_per_second)}</strong><span>retiring rows/s</span></div></div>
                <div className="totals"><span>{formatNumber(latest?.active_rows_total)} active total</span><span>{formatNumber(latest?.retiring_rows_total)} retiring total</span></div>
              </>}
            </article>
          </div>

          <section className="flip-card">
            <div className="flip-summary">
              <div>
                <span className="eyebrow">OWNERSHIP TRANSFER</span>
                <h3>{state.flip.status === "running" ? "Flip in progress" : state.flip.status === "succeeded" ? "Warm ownership granted" : state.flip.status === "verification_failed" ? "Granted; verification failed" : state.flip.status === "reverted" ? "Safely reverted to hot" : "Ready the retiring timeslot"}</h3>
                <p>The flip starts only after every admission condition stays healthy for {thresholds.stable_samples} consecutive samples.</p>
              </div>
              <div className={`readiness ${latest?.admission_ready ? "ready" : "waiting"}`}>
                {latest?.admission_ready ? <ShieldCheck size={20} /> : <Pause size={20} />}
                <div><strong>{latest?.admission_ready ? "Admission ready" : "Waiting for admission"}</strong><span>{latest?.healthy_samples ?? 0} / {thresholds.stable_samples} healthy samples</span></div>
              </div>
              <button className="flip-button" disabled={!flipAllowed || controlsDisabled} onClick={() => act("flip", () => request("/flip/start", { method: "POST", body: JSON.stringify({ fence_wakeup_mode: fenceWakeupMode, source_proof_mode: sourceProofMode }) }))}>
                {state.flip.status === "running" || pending === "flip" ? <LoaderCircle className="spin" size={18} /> : <Zap size={18} fill="currentColor" />}
                {state.flip.status === "running" ? "Flipping…" : `Start flip · ${sourceProofMode === "parallel_atomic_detach_marker_v1" ? "H" : sourceProofMode === "atomic_detach_marker_v1" ? "G" : sourceProofMode === "per_leaf_marker_v1" ? "F" : workload.write_fence_mode === "optimistic_detach_v1" ? "E" : workload.write_fence_mode === "hot_transactional_v1" ? "D" : fenceWakeupMode === "immediate_heartbeat" ? `${state.environment.source_topology === "isolated" ? "B+" : "A+"}` : `${state.environment.source_topology === "isolated" ? "B" : "A"}`}`}
                <ArrowRight size={17} />
              </button>
            </div>

            <div className="pipeline">
              {visibleStages.map(([stage, label, detail], index) => {
                const complete = stageComplete(stage);
                const active = !complete && state.flip.status === "running" && visibleStages.slice(0, index).every(([previous]) => stageComplete(previous));
                return <div className={`stage ${complete ? "complete" : active ? "active" : ""}`} key={stage}>
                  <div className="stage-node">{complete ? <Check size={14} /> : active ? <LoaderCircle className="spin" size={14} /> : index + 1}</div>
                  <div><strong>{label}</strong><span>{detail}</span></div>
                  {index < visibleStages.length - 1 && <ChevronRight className="stage-arrow" size={15} />}
                </div>;
              })}
            </div>
          </section>

          <div className="detail-grid">
            <section className="detail-card timing-card">
              <div className="card-heading"><div><span className="eyebrow">MEASURED BREAKDOWN</span><h3>Where the flip spends time</h3></div><span className={`result-badge ${state.flip.status}`}>{state.flip.status}</span></div>
              <div className="timing-list">
                {[
                  ["Tracker lock", "tracker_lock_ns", hotGateMode ? "t2h → t2w" : "t1 → t2"],
                  ...(hotGateMode ? [["Hot ownership fence", "hot_fence_park_ns", "t2 → t2h"]] : []),
                  ...(optimisticDetach ? [["Admission stop", "admission_fence_ns", "t2w → t2f"], ["In-flight resolution", "in_flight_resolution_ns", "t2f → t2q"]] : []),
                  ["Source fence proof", "source_proof_ns", "t5 → t7"],
                  ...(sourceProofMode === "parallel_atomic_detach_marker_v1" ? [["Parallel detach + markers", "parallel_detach_wall_ns", "all t3 → t4"], ["Marker delivery wait", "slot_wait_after_wakeup_ns", "t6w → t7"]] : sourceProofMode === "atomic_detach_marker_v1" ? [["Atomic detach + markers", "atomic_detach_marker_ns", "Σ t3 → t4"], ["Marker delivery wait", "slot_wait_after_wakeup_ns", "t6w → t7"]] : sourceProofMode === "per_leaf_marker_v1" ? [["Marker emission", "fence_wakeup_ns", "t6 → t6w"], ["Marker delivery wait", "slot_wait_after_wakeup_ns", "t6w → t7"]] : [["Fence wake-up", "fence_wakeup_ns", "t6 → t6w"], ["Slot wait after wake-up", "slot_wait_after_wakeup_ns", "t6w → t7"]]),
                  ["Capture E", "capture_e_ns", "t7 → t8"],
                  ["Warm sink proof", "sink_proof_ns", "t8 → t11"],
                  ["Ownership grant", "grant_ns", "t11 → t13"],
                  [state.flip.status === "reverted" ? "Forward until failure" : "Total writer park", state.flip.status === "reverted" ? "forward_until_failure_ns" : "writer_park_ns", state.flip.status === "reverted" ? "t2 → revert start" : "t2 → t13"],
                  ...(state.flip.status === "reverted" ? [["Safe reattach / revert", "revert_ns", "revert start → end"]] : []),
                ].map(([label, key, range]) => <div className="timing-row" key={key}><div><strong>{label}</strong><span>{range}</span></div><b>{formatDuration(liveDuration(key))}</b></div>)}
              </div>
            </section>

            <section className="detail-card partition-card">
              <div className="card-heading"><div><span className="eyebrow">RETIRING ROUTES</span><h3>Per-table detach</h3></div><span>{Object.keys(state.flip.detach_ns_by_table).length} / {state.environment.table_count}</span></div>
              <div className="partition-list">
                {Array.from({ length: state.environment.table_count }, (_, index) => {
                  const table = `cards_${String(index + 1).padStart(2, "0")}`;
                  const observed = Object.entries(state.flip.detach_ns_by_table)[index];
                  const lag = latest?.sink_lag_by_partition ? Object.values(latest.sink_lag_by_partition)[index] : undefined;
                  return <div className="partition-row" key={table}><span className="table-index">{String(index + 1).padStart(2, "0")}</span><div><strong>{observed?.[0] ?? table}</strong><span>topic partition 0 · lag {formatNumber(lag)}</span></div><b>{formatDuration(observed?.[1])}</b></div>;
                })}
              </div>
            </section>
          </div>

          <section className="history-card">
            <div className="card-heading history-heading">
              <div><span className="eyebrow">DURABLE LOCAL RESULTS</span><h3>Saved experiment history</h3><p>The ownership result is saved as soon as the retiring timeslot reaches <code>warm_primary</code>.</p></div>
              <span><History size={14} /> {savedRuns.length} saved</span>
            </div>
            {!supervisorAvailable ? (
              <div className="history-empty"><CircleAlert size={17} /><div><strong>History service is unavailable</strong><span>Start <code>make playground-supervisor</code> to read preserved results.</span></div></div>
            ) : savedRuns.length === 0 ? (
              <div className="history-empty"><History size={17} /><div><strong>No saved ownership results yet</strong><span>Complete a flip through t13; it will appear here automatically.</span></div></div>
            ) : (
              <div className="history-layout">
                <div className="history-list" role="list" aria-label="Saved experiment runs">
                  {savedRuns.map((run) => (
                    <button className={`history-row ${selectedRun?.run_id === run.run_id ? "selected" : ""}`} type="button" role="listitem" key={run.run_id} onClick={() => setSelectedRunId(run.run_id)}>
                      <span className={`history-status ${run.verification_outcome === "passed" ? "passed" : run.verification_outcome === "failed" ? "failed" : "pending"}`}><Check size={12} /></span>
                      <span><strong>{run.table_count} tables · {formatDuration(run.writer_park_ns)}</strong><small>{formatRecordedAt(run.recorded_at_utc)}</small></span>
                      <em>{run.artifact_type === "ownership_grant" ? "T13 saved" : run.outcome === "success" ? run.verification_outcome : run.outcome}</em>
                      <ChevronRight size={14} />
                    </button>
                  ))}
                </div>
                {selectedRun && <div className="run-detail">
                  <div className="run-detail-title"><div><span className="eyebrow">HISTORICAL · NON-AUTHORITATIVE</span><h4>{selectedRun.table_count}-table ownership transfer</h4></div><span className={`result-badge ${selectedRun.outcome === "success" && selectedRun.verification_outcome === "passed" ? "succeeded" : selectedRun.outcome === "reverted" ? "reverted" : selectedRun.outcome === "failed" || selectedRun.verification_outcome === "failed" ? "failed" : "running"}`}>{selectedRun.outcome === "success" ? selectedRun.verification_outcome : selectedRun.outcome}</span></div>
                  <div className="saved-timing-title"><span className="eyebrow">MEASURED BREAKDOWN</span><h5>Saved timing breakdown</h5></div>
                  <div className="saved-timing-list">
                    {(selectedRun.outcome === "reverted" ? [
                      ["Forward until failure", "t2 → revert start", selectedRun.forward_until_failure_ns],
                      ["Safe reattach / revert", "revert start → end", selectedRun.revert_ns],
                      ["Total writer park", "t2 → revert end", selectedRun.writer_park_ns],
                    ] : [
                      ["Tracker lock", ["hot_transactional_v1", "optimistic_detach_v1"].includes(selectedRun.write_fence_mode) ? "t2h → t2w" : "t1 → t2", selectedRun.tracker_lock_ns],
                      ...(["hot_transactional_v1", "optimistic_detach_v1"].includes(selectedRun.write_fence_mode) ? [["Hot ownership fence", "t2 → t2h", selectedRun.hot_fence_park_ns]] : []),
                      ...(selectedRun.write_fence_mode === "optimistic_detach_v1" ? [["Admission stop", "t2w → t2f", selectedRun.admission_fence_ns], ["In-flight resolution", "t2f → t2q", selectedRun.in_flight_resolution_ns]] : []),
                      ["Source fence proof", "t5 → t7", selectedRun.source_proof_ns],
                      ...(selectedRun.source_proof_mode === "parallel_atomic_detach_marker_v1" ? [["Parallel detach + markers", "all t3 → t4", selectedRun.parallel_detach_wall_ns], ["Marker delivery wait", "t6w → t7", selectedRun.slot_wait_after_wakeup_ns]] : selectedRun.source_proof_mode === "atomic_detach_marker_v1" ? [["Atomic detach + markers", "Σ t3 → t4", selectedRun.atomic_detach_marker_ns], ["Marker delivery wait", "t6w → t7", selectedRun.slot_wait_after_wakeup_ns]] : selectedRun.source_proof_mode === "per_leaf_marker_v1" ? [["Marker emission", "t6 → t6w", selectedRun.fence_wakeup_ns], ["Marker delivery wait", "t6w → t7", selectedRun.slot_wait_after_wakeup_ns]] : [["Fence wake-up", "t6 → t6w", selectedRun.fence_wakeup_ns], ["Slot wait after wake-up", "t6w → t7", selectedRun.slot_wait_after_wakeup_ns]]),
                      ["Capture E", "t7 → t8", selectedRun.capture_e_ns],
                      ["Warm sink proof", "t8 → t11", selectedRun.sink_proof_ns],
                      ["Ownership grant", "t11 → t13", selectedRun.grant_ns],
                      ["Total writer park", "t2 → t13", selectedRun.writer_park_ns],
                    ]).map(([label, range, duration]) => <div className="saved-timing-row" key={String(label)}><div><strong>{label}</strong><span>{range}</span></div><b>{formatDuration(duration as number | null)}</b></div>)}
                  </div>
                  <div className="saved-context-title"><span className="eyebrow">RUN CONTEXT</span></div>
                  <div className="run-metrics">
                    <div><span>Whole lifecycle</span><strong>{formatDuration(selectedRun.whole_lifecycle_ns)}</strong></div>
                    <div><span>Source lag at admission</span><strong>{formatBytes(selectedRun.source_lag_bytes)}</strong></div>
                    <div><span>Sink lag at admission</span><strong>{formatNumber(selectedRun.sink_lag_records)} records</strong></div>
                    <div><span>Target transaction rate</span><strong>{selectedRun.target_tps === null ? "Not recorded" : `${formatNumber(selectedRun.target_tps)} TPS`}</strong></div>
                    <div><span>Achieved transaction rate</span><strong>{selectedRun.achieved_tps === null ? "Not recorded" : `${formatNumber(selectedRun.achieved_tps)} TPS`}</strong></div>
                  </div>
                  <dl><div><dt>Run ID</dt><dd>{selectedRun.run_id}</dd></div><div><dt>Attempt ID</dt><dd>{selectedRun.attempt_id ?? "unknown"}</dd></div><div><dt>Profile</dt><dd>{selectedRun.profile ?? "unknown"}</dd></div><div><dt>Transaction shape</dt><dd>{selectedRun.transaction_shape === "api_batch_separate_commits_v1" ? `${selectedRun.operations_per_api_batch ?? "?"} operations / API batch · ${selectedRun.ownership_reads_per_api_batch ?? "?"} ownership read · ${selectedRun.ownership_epoch_checks_per_api_batch ?? "?"} epoch checks · ${selectedRun.postgres_transactions_per_api_batch ?? "?"} separate PostgreSQL transactions` : selectedRun.transaction_shape === "single_table_api" ? "one selected table / transaction" : selectedRun.transaction_shape === "legacy_unreserved_batch_scheduler" ? "legacy unreserved API batch (superseded)" : selectedRun.transaction_shape === "legacy_batch_admission_extra_transaction" ? "legacy standalone admission transaction (superseded)" : selectedRun.transaction_shape === "legacy_per_transaction_gate_api" ? "legacy gate read per transaction (superseded)" : selectedRun.transaction_shape === "legacy_all_tables_api" ? "legacy all-tables / transaction (superseded)" : "legacy/unknown"}</dd></div><div><dt>Admission check</dt><dd>{selectedRun.optimistic_admission_check_mode === "state_only_v1" ? "state only (H)" : selectedRun.optimistic_admission_check_mode === "state_and_epoch_v1" ? "state + epoch" : "legacy/unknown"}</dd></div><div><dt>Source topology</dt><dd>{selectedRun.source_topology ?? "legacy/unknown"}</dd></div><div><dt>Fence wake-up</dt><dd>{formatFenceExperiment(selectedRun)}</dd></div><div><dt>Heartbeat applied</dt><dd>{selectedRun.fence_wakeup_applied === true ? "yes" : selectedRun.fence_wakeup_applied === false ? "no" : "unknown"}</dd></div><div><dt>Generation</dt><dd>{selectedRun.environment_generation_id ?? "legacy"}</dd></div></dl>
                  {selectedRun.artifact_type === "ownership_grant" && <p className="checkpoint-note"><CircleAlert size={13} />Ownership was durably granted at t13. Post-grant parity verification was still pending when this checkpoint was read.</p>}
                </div>}
              </div>
            )}
          </section>

          <footer className="footer-note"><RotateCcw size={14} /><span>A completed flip is one-way. “New experiment” performs a scoped local rebuild and preserves this result history.</span></footer>
        </section>
      </div>

      {restartOpen && <div className="modal-backdrop" role="presentation" onMouseDown={(event) => { if (event.target === event.currentTarget && !restartRunning) { setRestartOpen(false); setRestartInitiatedJobId(null); } }}>
        <form className="modal" role="dialog" aria-modal="true" aria-labelledby="restart-title" onSubmit={startNewExperiment}>
          <div className="modal-head"><div className="danger-icon"><Trash2 size={18} /></div><div><span className="eyebrow">DESTRUCTIVE LOCAL ACTION</span><h3 id="restart-title">Start a new experiment</h3></div><button className="icon-button" type="button" disabled={restartRunning} onClick={() => { setRestartOpen(false); setRestartInitiatedJobId(null); }} aria-label="Close restart dialog"><X size={17} /></button></div>
          {restartRunning ? <div className="restart-progress">
            <div className="progress-copy"><div><strong>{supervisor.phase}</strong><span>Step {supervisor.step} of {supervisor.total_steps}</span></div><LoaderCircle className="spin" size={18} /></div>
            <div className="progress-bar"><i style={{ width: `${Math.max(4, (supervisor.step / supervisor.total_steps) * 100)}%` }} /></div>
            <p>The API can disconnect while containers and local volumes are rebuilt. This dialog keeps following the host-side supervisor.</p>
            {supervisor.logs.at(-1) && <pre className="log-preview">{supervisor.logs.at(-1)}</pre>}
          </div> : <>
            <div className="confirm-copy"><CircleAlert size={17} /><p>This stops writes, deletes only the RF3 benchmark volumes, recreates PostgreSQL/Kafka/Connect, and verifies a fresh <code>hot_primary</code> environment. Saved result files are kept.</p></div>
            <label className="field"><span>Tables in the fresh experiment</span><div className="select-shell"><select value={restartTableCount} onChange={(event) => setRestartTableCount(Number(event.target.value))}>{[5, 10, 15, 20].map((count) => <option value={count} key={count}>{count} tables</option>)}</select></div></label>
            <label className="field"><span>Source topology for A/B measurement</span><div className="select-shell"><select value={restartSourceTopology} onChange={(event) => setRestartSourceTopology(event.target.value as "shared" | "isolated")}><option value="shared">A — shared cell source</option><option value="isolated">B — active / migration isolation</option></select></div></label>
            <label className="field"><span>Type RESET exactly to confirm</span><div className="input-shell"><input value={restartConfirmation} autoComplete="off" spellCheck={false} onChange={(event) => setRestartConfirmation(event.target.value)} placeholder="RESET" /></div></label>
            {!supervisorAvailable && <div className="alert error"><CircleAlert size={15} /><span>Supervisor unavailable at localhost:8091. Start <code>make playground-supervisor</code>.</span></div>}
            {!connected && <div className="alert error"><CircleAlert size={15} /><span>Control API unavailable at localhost:8090. Run <code>{controlApiRecoveryCommand}</code>, wait for the API to become healthy, then retry. {restartMayHaveChangedVolumes ? "The restart reached a destructive step, so local volumes may already have changed; review the failure below." : "No volumes were deleted by this attempt."}</span></div>}
            {restartInitiatedJobId && supervisor?.job_id === restartInitiatedJobId && supervisor.status === "failed" && <div className="alert error"><CircleAlert size={15} /><span><strong>{supervisor.error ?? "The restart failed."}</strong>{supervisor.recovery_hint && <small>{supervisor.recovery_hint}</small>}{supervisor.error_code === "control_api_unavailable" && <code>{controlApiRecoveryCommand}</code>}</span></div>}
            {restartError && <div className="alert error"><CircleAlert size={15} /><span>{restartError}</span></div>}
          </>}
          <div className="modal-actions">
            {restartJustCompleted && <span className="restart-success"><Check size={14} /> Fresh {supervisor?.table_count}-table environment is ready</span>}
            <button className="button ghost" type="button" disabled={restartRunning} onClick={() => { setRestartOpen(false); setRestartInitiatedJobId(null); }}>{restartJustCompleted ? "Done" : "Cancel"}</button>
            {!restartJustCompleted && <button className="button danger" type="submit" disabled={!connected || !supervisorAvailable || restartRunning || restartConfirmation !== "RESET" || state.flip.status === "running"}>{restartRunning ? "Restarting…" : "Delete and rebuild"}</button>}
          </div>
        </form>
      </div>}
    </main>
  );
}
