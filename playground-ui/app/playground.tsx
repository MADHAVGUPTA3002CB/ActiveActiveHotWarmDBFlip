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
  sink_lag_records: number;
  max_sink_lag_records: number;
  sink_lag_by_partition: Record<string, number>;
  active_rows_total: number;
  retiring_rows_total: number;
  active_rows_per_second: number;
  retiring_rows_per_second: number;
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
  };
  metrics_error: string | null;
};

type SupervisorState = {
  status: "idle" | "running" | "completed" | "failed";
  phase: string;
  step: number;
  total_steps: number;
  table_count: number | null;
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
  tracker_lock_ns: number | null;
  source_proof_ns: number | null;
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

function NumberField({ label, value, onChange, unit, min = 0, disabled = false }: { label: string; value: number; onChange: (value: number) => void; unit?: string; min?: number; disabled?: boolean }) {
  return (
    <label className="field">
      <span>{label}</span>
      <div className="input-shell">
        <input type="number" min={min} value={value} disabled={disabled} onChange={(event) => onChange(Number(event.target.value))} />
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
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState<string | null>(null);
  const [connected, setConnected] = useState(false);
  const [supervisor, setSupervisor] = useState<SupervisorState | null>(null);
  const [supervisorAvailable, setSupervisorAvailable] = useState(false);
  const [savedRuns, setSavedRuns] = useState<SavedRun[]>([]);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [restartOpen, setRestartOpen] = useState(false);
  const [restartTableCount, setRestartTableCount] = useState(5);
  const [restartConfirmation, setRestartConfirmation] = useState("");
  const [restartError, setRestartError] = useState<string | null>(null);
  const [restartInitiatedJobId, setRestartInitiatedJobId] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      const next = (await request("/state")) as State;
      setState(next);
      setWorkload((current) => current ?? next.workload.settings);
      setThresholds((current) => current ?? next.thresholds);
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
        body: JSON.stringify({ table_count: restartTableCount, confirmation: restartConfirmation }),
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
  const stageComplete = (stage: string) => stage === "t4" ? detachDone : completedStages.has(stage);
  const restartRunning = supervisor?.status === "running";
  const controlsDisabled = pending !== null || restartRunning;
  const flipAllowed = Boolean(latest?.admission_ready && state?.workload.retiring_writer_alive && state?.flip.status !== "running" && !restartRunning);
  const selectedRun = savedRuns.find((run) => run.run_id === selectedRunId) ?? savedRuns[0] ?? null;
  const restartJustCompleted = Boolean(restartInitiatedJobId && supervisor?.job_id === restartInitiatedJobId && supervisor.status === "completed");
  const controlApiRecoveryCommand = `make playground-api-rf3 TABLE_COUNT=${state?.environment.table_count ?? restartTableCount}`;
  const restartMayHaveChangedVolumes = Boolean(supervisor?.status === "failed" && supervisor.step > 1);

  const liveDuration = (key: string) => {
    const completed = state?.flip.durations_ns[key];
    if (completed !== undefined) return completed;
    if (!state || state.flip.status !== "running") return undefined;
    const timestamps = state.flip.timestamps_ns;
    const bounds: Record<string, [string, string]> = {
      tracker_lock_ns: ["t1", "t2"],
      source_proof_ns: ["t5", "t7"],
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
            <div className="info-note"><CircleAlert size={14} /><span>{state.environment.topology_note}</span></div>
            <button className="button danger full restart-control" type="button" disabled={restartRunning || state.flip.status === "running"} onClick={() => { setRestartOpen(true); setRestartTableCount(state.environment.table_count); setRestartError(null); setRestartInitiatedJobId(null); }}>
              {restartRunning ? <LoaderCircle className="spin" size={15} /> : <Trash2 size={15} />} {restartRunning ? "Restarting system…" : "New experiment"}
            </button>
            {!supervisorAvailable && <div className="supervisor-note">Restart control unavailable. Run <code>make playground-supervisor</code> in a second terminal.</div>}
          </section>

          <form className="panel-section" onSubmit={saveWorkload}>
            <div className="section-title"><Zap size={15} /><span>Live workload</span><em>{state.workload.running ? "RUNNING" : "STOPPED"}</em></div>
            <NumberField label="Active rows / partition / batch" value={workload.active_rows_per_partition} min={1} onChange={(value) => setWorkload({ ...workload, active_rows_per_partition: value })} />
            <NumberField label="Retiring rows / partition / batch" value={workload.retiring_rows_per_partition} onChange={(value) => setWorkload({ ...workload, retiring_rows_per_partition: value })} />
            <div className="field-grid two">
              <NumberField label="Active pause" value={workload.active_pause_ms} unit="ms" onChange={(value) => setWorkload({ ...workload, active_pause_ms: value })} />
              <NumberField label="Retiring pause" value={workload.retiring_pause_ms} unit="ms" onChange={(value) => setWorkload({ ...workload, retiring_pause_ms: value })} />
            </div>
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
              <div className="metric-head"><span>Committed write rate</span><Gauge size={16} /></div>
              <div className="rate-pair"><div><strong>{formatNumber(latest?.active_rows_per_second)}</strong><span>active rows/s</span></div><div><strong>{formatNumber(latest?.retiring_rows_per_second)}</strong><span>retiring rows/s</span></div></div>
              <div className="totals"><span>{formatNumber(latest?.active_rows_total)} active total</span><span>{formatNumber(latest?.retiring_rows_total)} retiring total</span></div>
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
              <button className="flip-button" disabled={!flipAllowed || controlsDisabled} onClick={() => act("flip", () => request("/flip/start", { method: "POST" }))}>
                {state.flip.status === "running" || pending === "flip" ? <LoaderCircle className="spin" size={18} /> : <Zap size={18} fill="currentColor" />}
                {state.flip.status === "running" ? "Flipping…" : "Start flip"}
                <ArrowRight size={17} />
              </button>
            </div>

            <div className="pipeline">
              {STAGES.map(([stage, label, detail], index) => {
                const complete = stageComplete(stage);
                const active = !complete && state.flip.status === "running" && STAGES.slice(0, index).every(([previous]) => stageComplete(previous));
                return <div className={`stage ${complete ? "complete" : active ? "active" : ""}`} key={stage}>
                  <div className="stage-node">{complete ? <Check size={14} /> : active ? <LoaderCircle className="spin" size={14} /> : index + 1}</div>
                  <div><strong>{label}</strong><span>{detail}</span></div>
                  {index < STAGES.length - 1 && <ChevronRight className="stage-arrow" size={15} />}
                </div>;
              })}
            </div>
          </section>

          <div className="detail-grid">
            <section className="detail-card timing-card">
              <div className="card-heading"><div><span className="eyebrow">MEASURED BREAKDOWN</span><h3>Where the flip spends time</h3></div><span className={`result-badge ${state.flip.status}`}>{state.flip.status}</span></div>
              <div className="timing-list">
                {[
                  ["Tracker lock", "tracker_lock_ns", "t1 → t2"],
                  ["Source fence proof", "source_proof_ns", "t5 → t7"],
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
                      ["Tracker lock", "t1 → t2", selectedRun.tracker_lock_ns],
                      ["Source fence proof", "t5 → t7", selectedRun.source_proof_ns],
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
                  </div>
                  <dl><div><dt>Run ID</dt><dd>{selectedRun.run_id}</dd></div><div><dt>Attempt ID</dt><dd>{selectedRun.attempt_id ?? "unknown"}</dd></div><div><dt>Profile</dt><dd>{selectedRun.profile ?? "unknown"}</dd></div><div><dt>Generation</dt><dd>{selectedRun.environment_generation_id ?? "legacy"}</dd></div></dl>
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
