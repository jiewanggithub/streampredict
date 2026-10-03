"use client";

import { FormEvent, useCallback, useEffect, useRef, useState } from "react";

import { API_BASE_URL, ApiError, api, type DemoChannel, type DemoProfile, type MetricsOverview, type PredictResponse, type RiskLabel } from "@/lib/api";

const POLL_INTERVAL_MS = 1000;
const SPARKLINE_POINTS = 30;
const ACTIVE_STATES = new Set(["starting", "running", "cooling_down"]);

const RISK_LABELS: Record<RiskLabel, string> = { low_risk: "Low risk", review: "Review", high_risk: "High risk" };
const LABEL_ORDER: RiskLabel[] = ["low_risk", "review", "high_risk"];
const SCALED_WORKLOADS = new Set(["api", "consumer", "model-serving"]);
const OUTCOME_LABELS: Record<string, string> = { promoted: "Promoted", rolled_back: "Rolled back", rejected: "Rejected", manual_rollback: "Manual rollback" };

type Tone = "info" | "success" | "warning";
type EventItem = { id: number; time: string; tone: Tone; message: string };
type NodeStatus = "healthy" | "degraded" | "pending";

function timeNow() {
  return new Intl.DateTimeFormat("en-US", { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false }).format(new Date());
}

function fmt(value: number | null | undefined, suffix = "", digits = 1) {
  return value === null || value === undefined ? "—" : `${Number(value.toFixed(digits)).toLocaleString()}${suffix}`;
}

// Serving versions are integers ("2"); show them as "v2". Other labels (e.g. mock builds) pass through.
function versionLabel(version: string | undefined) {
  return version && /^\d+$/.test(version) ? `v${version}` : (version ?? "—");
}

function errorMessage(error: unknown) {
  return error instanceof ApiError ? error.message : "Unexpected error";
}

function Sparkline({ values, color, label = "Requests per second over the last 30 seconds", className = "sparkline" }: { values: number[]; color: string; label?: string; className?: string }) {
  const width = 240;
  const height = 64;
  const max = Math.max(...values, 1);
  const points = values
    .map((value, index) => {
      const x = (index / Math.max(values.length - 1, 1)) * width;
      const y = height - 7 - (value / max) * (height - 14);
      return `${x},${y}`;
    })
    .join(" ");
  const id = `fill-${color.replace("#", "")}`;

  return (
    <svg className={className} viewBox={`0 0 ${width} ${height}`} role="img" aria-label={label}>
      <defs>
        <linearGradient id={id} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={color} stopOpacity="0.28" />
          <stop offset="100%" stopColor={color} stopOpacity="0" />
        </linearGradient>
      </defs>
      <path d={`M ${points} L ${width},${height} L 0,${height} Z`} fill={`url(#${id})`} />
      <polyline points={points} fill="none" stroke={color} strokeWidth="2.4" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

function MetricCard({ label, value, detail, tone, history }: { label: string; value: string; detail: string; tone: "cyan" | "violet" | "mint" | "amber"; history?: number[] }) {
  const colors = { cyan: "#43d7ff", violet: "#9d8cff", mint: "#48e4b7", amber: "#ffb75d" };
  return (
    <article className={`metric-card ${tone}`}>
      <div className="metric-heading">
        <span>{label}</span>
        <span className="live-dot" />
      </div>
      <strong>{value}</strong>
      <small>{detail}</small>
      {history ? <Sparkline values={history} color={colors[tone]} /> : null}
    </article>
  );
}

function ServiceNode({ name, detail, status }: { name: string; detail: string; status: NodeStatus }) {
  return (
    <div className={`service-node ${status}`}>
      <div className={`service-icon ${status}`}>{name.slice(0, 1)}</div>
      <div>
        <strong>{name}</strong>
        <span>{detail}</span>
      </div>
      <span className={`service-state ${status}`}>{status}</span>
    </div>
  );
}

export function StreamPredictDashboard() {
  const [overview, setOverview] = useState<MetricsOverview | null>(null);
  const [connectionError, setConnectionError] = useState<string | null>(null);
  const [prediction, setPrediction] = useState<PredictResponse | null>(null);
  const [predictError, setPredictError] = useState<string | null>(null);
  const [predicting, setPredicting] = useState(false);
  const [demoError, setDemoError] = useState<string | null>(null);
  const [demoPending, setDemoPending] = useState(false);
  const [events, setEvents] = useState<EventItem[]>([]);
  const [releasePending, setReleasePending] = useState(false);
  const [releaseError, setReleaseError] = useState<string | null>(null);
  const [transport, setTransport] = useState<"sse" | "polling">("sse");
  const eventId = useRef(0);
  const previous = useRef<{ connected: boolean | null; redis?: string; kafka?: string; replicas?: number; release?: string; rescale?: string; releaseStage?: string; demoState?: string; sessionId?: string | null }>({ connected: null });

  const addEvent = useCallback((message: string, tone: Tone = "info") => {
    eventId.current += 1;
    const item = { id: eventId.current, time: timeNow(), tone, message };
    setEvents((current) => [item, ...current].slice(0, 8));
  }, []);

  const handleOverview = useCallback(
    (next: MetricsOverview) => {
      const prev = previous.current;
      if (prev.connected !== true) addEvent(`Connected to API · serving ${next.model.name} ${next.model.version}`, "success");
      const rescale = next.infrastructure?.kubernetes?.scaling_events[0];
      const rescaleKey = rescale ? `${rescale.at}-${rescale.message}` : undefined;
      if (prev.connected && rescale && rescaleKey !== prev.rescale) {
        addEvent(`Autoscaler · ${rescale.target.replace("keda-hpa-", "")}: ${rescale.message.split(";")[0]}`, "success");
      }
      const replicas = next.infrastructure?.consumer_replicas ?? undefined;
      if (prev.replicas !== undefined && replicas !== undefined && replicas !== prev.replicas) {
        addEvent(`Consumer replicas ${prev.replicas} → ${replicas}; partitions rebalanced`, replicas > prev.replicas ? "success" : "info");
      }
      const latest = next.deployment?.history[0];
      const latestKey = latest ? `${latest.version}-${latest.started_at}` : undefined;
      if (prev.connected && latest && latestKey !== prev.release) {
        const label = `${versionLabel(latest.previous ?? undefined)} → ${versionLabel(latest.version)}`;
        if (latest.outcome === "promoted") addEvent(`Release ${label} promoted; health gate passed`, "success");
        else if (latest.outcome === "rolled_back") addEvent(`Release ${label} rolled back: ${latest.reasons.join("; ")}`, "warning");
        else if (latest.outcome === "rejected") addEvent(`${versionLabel(latest.version)} rejected before deploy: ${latest.reasons.join("; ")}`, "warning");
        else addEvent(`Manual rollback ${label}`, "warning");
      }
      if (prev.connected && next.deployment?.active && next.deployment.active.stage === "verifying" && prev.releaseStage !== "verifying") {
        addEvent(`Traffic switched to ${versionLabel(next.deployment.active.version)}; verifying against its training profile`);
      }
      const kafkaState = next.kafka?.status;
      if (prev.kafka && kafkaState && prev.kafka !== kafkaState) {
        if (kafkaState === "ok") addEvent("Kafka reachable; async events flowing", "success");
        else addEvent("Kafka unreachable; async events paused", "warning");
      }
      if (prev.redis && prev.redis !== next.dependencies.redis) {
        if (next.dependencies.redis === "ok") addEvent("Redis recovered; cache re-enabled", "success");
        else addEvent("Redis unavailable; predictions bypass the cache", "warning");
      }
      const demo = next.demo;
      if (prev.demoState && (demo.state !== prev.demoState || demo.session_id !== prev.sessionId)) {
        if (demo.state === "running") addEvent(`${demo.profile === "spike" ? "Traffic spike" : "Standard demo"} running ${demo.channel === "events" ? "through Kafka " : ""}· target ${demo.target_rps} RPS for ${demo.duration_seconds}s`);
        if (demo.state === "cooling_down") addEvent("Traffic generation stopped; draining in-flight requests", "info");
        if (demo.state === "completed" && demo.summary)
          addEvent(
            `Demo completed · ${demo.summary.total_requests} requests, peak ${demo.summary.peak_rps} RPS, ${demo.summary.errors} errors` +
              (demo.summary.max_consumer_lag != null ? ` · max lag ${demo.summary.max_consumer_lag}` : "") +
              (demo.summary.peak_consumer_replicas != null ? ` · consumers peaked at ${demo.summary.peak_consumer_replicas}` : "") +
              (demo.summary.lag_recovery_seconds != null ? ` · backlog drained ${demo.summary.lag_recovery_seconds}s after its peak` : ""),
            "success",
          );
        if (demo.state === "failed") addEvent(`Demo ended early (${demo.stop_reason ?? "unknown"})`, "warning");
      }
      previous.current = { connected: true, redis: next.dependencies.redis, kafka: next.kafka?.status, replicas: replicas ?? prev.replicas, release: latestKey, rescale: rescaleKey, releaseStage: next.deployment?.active?.stage, demoState: demo.state, sessionId: demo.session_id };
      setOverview(next);
      setConnectionError(null);
    },
    [addEvent],
  );

  useEffect(() => {
    let cancelled = false;
    let timer: number | undefined;
    let source: EventSource | null = null;

    const markDisconnected = (message: string) => {
      if (previous.current.connected !== false) addEvent("Lost connection to the API; retrying", "warning");
      previous.current = { ...previous.current, connected: false };
      setConnectionError(message);
    };

    // Fallback when the browser or a proxy cannot hold an SSE connection open.
    const poll = async () => {
      try {
        const next = await api.overview();
        if (!cancelled) {
          setTransport("polling");
          handleOverview(next);
        }
      } catch (error) {
        if (!cancelled) markDisconnected(errorMessage(error));
      }
      if (!cancelled) timer = window.setTimeout(poll, POLL_INTERVAL_MS);
    };

    if (typeof EventSource === "undefined") {
      void poll();
    } else {
      source = new EventSource(api.streamUrl);
      source.addEventListener("overview", (message) => {
        if (cancelled) return;
        try {
          handleOverview(JSON.parse((message as MessageEvent<string>).data) as MetricsOverview);
        } catch {
          markDisconnected("Received a malformed metrics event");
        }
      });
      // EventSource reconnects on its own (the server also recycles streams periodically).
      source.onerror = () => {
        if (!cancelled && source?.readyState !== EventSource.OPEN) markDisconnected(`Cannot reach the API at ${API_BASE_URL}`);
      };
    }

    return () => {
      cancelled = true;
      source?.close();
      window.clearTimeout(timer);
    };
  }, [addEvent, handleOverview]);

  const startDemo = async (profile: DemoProfile) => {
    setDemoPending(true);
    setDemoError(null);
    // Spikes go through Kafka when it is healthy so the lag and consumer throughput react.
    const channel: DemoChannel = profile === "spike" && overview?.kafka?.status === "ok" ? "events" : "sync";
    try {
      const status = await api.startDemo(profile, channel);
      setOverview((current) => (current ? { ...current, demo: status } : current));
      addEvent(`${profile === "spike" ? "Traffic spike" : "Standard demo"} requested · ${channel === "events" ? "via Kafka" : "direct"} · capped at ${status.target_rps} RPS`);
    } catch (error) {
      setDemoError(errorMessage(error));
    } finally {
      setDemoPending(false);
    }
  };

  const releaseModel = async (version: string) => {
    setReleasePending(true);
    setReleaseError(null);
    try {
      await api.releaseModel(version);
      addEvent(`Release of ${versionLabel(version)} requested; running gates`);
    } catch (error) {
      setReleaseError(errorMessage(error));
    } finally {
      setReleasePending(false);
    }
  };

  const rollbackModel = async () => {
    setReleasePending(true);
    setReleaseError(null);
    try {
      await api.rollbackModel();
      addEvent("Manual rollback requested", "warning");
    } catch (error) {
      setReleaseError(errorMessage(error));
    } finally {
      setReleasePending(false);
    }
  };

  const stopDemo = async () => {
    setDemoPending(true);
    setDemoError(null);
    try {
      await api.stopDemo();
      addEvent("Demo stopped manually; beginning graceful cooldown", "warning");
    } catch (error) {
      setDemoError(errorMessage(error));
    } finally {
      setDemoPending(false);
    }
  };

  const submitPrediction = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    setPredicting(true);
    setPredictError(null);
    try {
      const result = await api.predict({
        amount: Number(form.get("amount")),
        events_per_hour: Number(form.get("velocity")),
        distance_km: Number(form.get("distance")),
      });
      setPrediction(result);
      addEvent(`Prediction ${result.request_id.slice(0, 8)} · ${RISK_LABELS[result.label]} · cache ${result.cache}`, "success");
    } catch (error) {
      setPredictError(errorMessage(error));
    } finally {
      setPredicting(false);
    }
  };

  const connected = overview !== null && connectionError === null;
  const demo = overview?.demo;
  const active = demo ? ACTIVE_STATES.has(demo.state) : false;
  const traffic = overview?.traffic;
  const cache = overview?.cache;
  const redisStatus: NodeStatus = !connected ? "pending" : overview.dependencies.redis === "ok" ? "healthy" : "degraded";
  const kafka = overview?.kafka ?? null;
  const infra = overview?.infrastructure ?? null;
  const deployment = overview?.deployment ?? null;
  const kube = infra?.kubernetes ?? null;
  const model = overview?.model ?? null;
  const labelTotal = model ? LABEL_ORDER.reduce((sum, label) => sum + model.label_distribution[label], 0) : 0;
  const eventsDemo = active && overview?.demo.channel === "events";
  const kafkaStatus: NodeStatus = !connected || !kafka ? "pending" : kafka.status === "ok" ? "healthy" : "degraded";
  const kafkaDetail = !kafka ? "disabled" : kafka.status === "ok" ? `lag ${fmt(kafka.lag, "", 0)}` : "unreachable";
  const history = traffic ? traffic.rps_history.slice(-SPARKLINE_POINTS) : Array<number>(SPARKLINE_POINTS).fill(0);
  const hitRate = cache?.hit_rate ?? 0;
  const statusLabel = !overview ? "connecting" : demo ? demo.state.replace("_", " ") : "idle";
  const progress = demo && demo.duration_seconds ? Math.min(100, (demo.elapsed_seconds / demo.duration_seconds) * 100) : 0;

  return (
    <main className="shell">
      <header className="topbar">
        <a className="brand" href="#top" aria-label="StreamPredict home">
          <span className="brand-mark">S</span>
          <span>
            <strong>StreamPredict</strong>
            <small>REAL-TIME ML PLATFORM</small>
          </span>
        </a>
        <div className="topbar-actions">
          <span className={`connection ${connected ? "" : "offline"}`}>
            <i /> {connected ? `Live metrics · ${transport === "sse" ? "streaming" : "polling 1s"}` : overview ? "Reconnecting…" : "Connecting…"}
          </span>
          <a className="ghost-button" href="https://github.com/jiewanggithub/streampredict" target="_blank" rel="noreferrer">
            View repository ↗
          </a>
        </div>
      </header>

      {connectionError ? (
        <div className="banner" role="alert">
          <strong>API unreachable.</strong> {connectionError}. Start the stack with <code>make up</code> (or <code>make api-dev</code>). Reconnecting automatically.
        </div>
      ) : null}

      <section className="hero" id="top">
        <div>
          <div className="eyebrow">
            <span /> LIVE SYSTEM
          </div>
          <h1>
            Watch a real-time ML system
            <br />
            <em>respond under pressure.</em>
          </h1>
          <p>Trigger safe, synthetic traffic against the live API gateway and watch request rate, latency, cache behavior, and model serving respond in real time.</p>
        </div>
        <div className="demo-control">
          <div className="demo-status">
            <div>
              <span>DEMO STATUS</span>
              <strong className={active ? "active" : ""}>{statusLabel}</strong>
            </div>
            <small>{demo ? `Hard limit · ${demo.max_rps} RPS · ${Math.round(demo.max_duration_seconds / 60)} min` : "Hard limits enforced by the API"}</small>
          </div>
          <div className="control-row">
            <button className="primary-button" onClick={() => void startDemo("standard")} disabled={!connected || active || demoPending}>
              ▶ Run demo
            </button>
            <button className="spike-button" onClick={() => void startDemo("spike")} disabled={!connected || active || demoPending}>
              ↗ Start traffic spike
            </button>
            {active ? (
              <button className="stop-button" onClick={() => void stopDemo()} disabled={demoPending || demo?.state === "cooling_down"}>
                Stop
              </button>
            ) : null}
          </div>
          {active && demo ? (
            <div className="demo-progress">
              <div className="progress">
                <i style={{ width: `${progress}%` }} />
              </div>
              <small>
                {Math.round(demo.elapsed_seconds)}s / {demo.duration_seconds}s · target {fmt(demo.current_target_rps, " RPS", 0)} · {demo.generated_requests.toLocaleString()} requests
              </small>
            </div>
          ) : null}
          {demoError ? <p className="inline-error">{demoError}</p> : null}
        </div>
      </section>

      <section className="metric-grid" aria-label="Live platform metrics">
        {/* An event-channel demo bypasses the synchronous path, so show the Kafka ingest rate instead. */}
        {eventsDemo ? (
          <MetricCard label="EVENTS / SECOND" value={fmt(kafka?.incoming_rate, "", 1)} detail={`Async demo via Kafka · ${fmt(traffic?.rps, " sync req/s", 1)}`} tone="cyan" />
        ) : (
          <MetricCard label="REQUESTS / SECOND" value={fmt(traffic?.rps, "", 1)} detail={`${active ? "Synthetic demo traffic + API" : "5-second average"}${traffic?.scope === "cluster" ? " · all replicas" : ""}`} tone="cyan" history={history} />
        )}
        <MetricCard label="P95 LATENCY" value={fmt(traffic?.p95_ms, " ms")} detail={traffic?.p50_ms != null ? `p50 ${fmt(traffic.p50_ms, " ms")} · p99 ${fmt(traffic.p99_ms, " ms")}` : "No traffic in the last 60s"} tone="violet" />
        <MetricCard label="SUCCESS RATE" value={fmt(traffic?.success_rate, "%", 2)} detail={traffic ? `${traffic.errors_in_window} errors · ${traffic.requests_in_window.toLocaleString()} requests in 60s` : "Waiting for data"} tone="mint" />
        <MetricCard label="CACHE HIT RATE" value={fmt(cache?.hit_rate, "%")} detail={cache?.lookup_p95_ms != null ? `Redis lookup p95 ${fmt(cache.lookup_p95_ms, " ms", 2)}` : "No cache lookups yet"} tone="amber" />
      </section>

      <section className="dashboard-grid">
        <article className="panel system-panel">
          <div className="panel-heading">
            <div>
              <span className="section-kicker">REQUEST PATH</span>
              <h2>System topology</h2>
            </div>
            <span className={redisStatus === "healthy" ? "panel-badge" : "scaling-label"}>
              {!connected ? "API offline" : redisStatus === "healthy" ? "All live services healthy" : "Degraded · cache bypass"}
            </span>
          </div>
          <div className="service-flow">
            <ServiceNode name="FastAPI" detail={connected ? `${fmt(traffic?.rps)} req/s` : "unreachable"} status={connected ? "healthy" : "pending"} />
            <span className="flow-arrow">→</span>
            <ServiceNode name="Redis" detail={connected ? `${fmt(cache?.hit_rate, "% hit")}` : "unknown"} status={redisStatus} />
            <span className="flow-arrow">→</span>
            <ServiceNode name="Kafka" detail={kafkaDetail} status={kafkaStatus} />
            <span className="flow-arrow">→</span>
            <ServiceNode name="Consumers" detail={infra?.consumer_replicas != null ? `${infra.consumer_replicas} replicas` : "unknown"} status={kafkaStatus} />
            <span className="flow-arrow">→</span>
            <ServiceNode name="Model serving" detail={overview ? `${overview.model.backend} · ${versionLabel(overview.model.version)}` : "unknown"} status={!connected ? "pending" : overview.dependencies.inference === "ok" ? "healthy" : "degraded"} />
          </div>
          <div className="recovery-strip">
            <div>
              <span>Traffic</span>
              <strong>{active ? "Elevated" : "Baseline"}</strong>
            </div>
            <b>→</b>
            <div>
              <span>Cache</span>
              <strong>{redisStatus === "degraded" ? "Bypassed" : cache?.hit_rate != null ? `${fmt(cache.hit_rate, "%")} hit` : "Cold"}</strong>
            </div>
            <b>→</b>
            <div>
              <span>Latency p95</span>
              <strong>{fmt(traffic?.p95_ms, " ms")}</strong>
            </div>
            <b>→</b>
            <div>
              <span>Outcome</span>
              <strong>{demo?.state === "completed" ? "Recovered" : active ? "Under load" : "Monitored"}</strong>
            </div>
          </div>
        </article>

        <article className="panel prediction-panel">
          <div className="panel-heading">
            <div>
              <span className="section-kicker">INTERACTIVE</span>
              <h2>Try a prediction</h2>
            </div>
            <span className="model-chip">{versionLabel(overview?.model.version)}</span>
          </div>
          <form onSubmit={(event) => void submitPrediction(event)}>
            <label>
              Transaction amount
              <input name="amount" type="number" min="0.01" max="1000000" step="0.01" defaultValue="860" required />
            </label>
            <div className="field-row">
              <label>
                Events / hour
                <input name="velocity" type="number" min="0" max="10000" defaultValue="4" required />
              </label>
              <label>
                Distance (km)
                <input name="distance" type="number" min="0" max="20000" defaultValue="120" required />
              </label>
            </div>
            <button className="predict-button" disabled={predicting || !connected}>
              {predicting ? "Running inference…" : "Run prediction →"}
            </button>
          </form>
          <div className={`prediction-result ${prediction && !predictError ? "visible" : ""}`}>
            {predictError ? (
              <p className="inline-error">{predictError}</p>
            ) : prediction ? (
              <>
                <div>
                  <span>RESULT</span>
                  <strong className={prediction.label === "low_risk" ? "safe" : "risk"}>{RISK_LABELS[prediction.label]}</strong>
                </div>
                <div>
                  <span>SCORE</span>
                  <strong>{prediction.score.toFixed(3)}</strong>
                </div>
                <div>
                  <span>LATENCY</span>
                  <strong>{prediction.latency_ms} ms</strong>
                </div>
                <div>
                  <span>CACHE</span>
                  <strong>{prediction.cache}</strong>
                </div>
                <small>
                  request · {prediction.request_id.slice(0, 8)} · {prediction.model_name} {prediction.model_version}
                </small>
              </>
            ) : (
              <p>Submit the form to see a versioned prediction response. Submit it twice to see a cache hit.</p>
            )}
          </div>
        </article>

        <article className="panel detail-panel">
          <div className="panel-heading">
            <div>
              <span className="section-kicker">ONLINE DATA</span>
              <h2>Redis cache</h2>
            </div>
            <span className={redisStatus === "degraded" ? "scaling-label" : "panel-badge"}>{redisStatus === "degraded" ? "Unavailable" : connected ? "Healthy" : "Unknown"}</span>
          </div>
          <div className="detail-stats">
            <div>
              <span>Hit rate</span>
              <strong>{fmt(cache?.hit_rate, "%")}</strong>
            </div>
            <div>
              <span>Lookup p95</span>
              <strong>{fmt(cache?.lookup_p95_ms, " ms", 2)}</strong>
            </div>
            <div>
              <span>TTL</span>
              <strong>{cache ? `${cache.ttl_seconds}s` : "—"}</strong>
            </div>
          </div>
          <div className="cache-split">
            <i style={{ width: `${hitRate}%` }} />
            <b style={{ width: `${cache?.hit_rate != null ? 100 - hitRate : 100}%` }} />
          </div>
          <div className="legend">
            <span>
              <i className="hit" /> Cache hit
            </span>
            <span>
              <i className="miss" /> Cache miss
            </span>
            {cache?.bypass_count ? <span>{cache.bypass_count} bypassed (Redis down)</span> : null}
          </div>
        </article>

        <article className="panel detail-panel">
          <div className="panel-heading">
            <div>
              <span className="section-kicker">LOAD GENERATOR</span>
              <h2>Demo session</h2>
            </div>
            <span className={active ? "scaling-label" : "panel-badge"}>{statusLabel}</span>
          </div>
          {demo?.summary && !active ? (
            <div className="detail-stats">
              <div>
                <span>Requests</span>
                <strong>{demo.summary.total_requests.toLocaleString()}</strong>
              </div>
              <div>
                <span>Peak RPS</span>
                <strong>{demo.summary.peak_rps}</strong>
              </div>
              <div>
                <span>Errors</span>
                <strong>{demo.summary.errors}</strong>
              </div>
              <div>
                <span>Duration</span>
                <strong>{fmt(demo.summary.duration_seconds, "s", 0)}</strong>
              </div>
              <div>
                <span>Cache hit</span>
                <strong>{fmt(demo.summary.cache_hit_rate, "%")}</strong>
              </div>
              <div>
                <span>Stop</span>
                <strong>{demo.stop_reason === "duration_reached" ? "timer" : (demo.stop_reason ?? "—")}</strong>
              </div>
            </div>
          ) : active && demo ? (
            <div className="detail-stats">
              <div>
                <span>Profile</span>
                <strong>{demo.profile}</strong>
              </div>
              <div>
                <span>Target</span>
                <strong>{fmt(demo.current_target_rps, "", 0)}/s</strong>
              </div>
              <div>
                <span>Sent</span>
                <strong>{demo.generated_requests.toLocaleString()}</strong>
              </div>
            </div>
          ) : (
            <p className="empty-state">No demo has run yet. Start one above; it stops automatically at its time limit.</p>
          )}
        </article>

        <article className="panel detail-panel">
          <div className="panel-heading">
            <div>
              <span className="section-kicker">MODEL CONTROL</span>
              <h2>Serving model</h2>
            </div>
            <span className="model-chip">{versionLabel(model?.version)}</span>
          </div>
          <div className="detail-stats">
            <div>
              <span>Model</span>
              <strong title={model?.name}>{model?.name ?? "—"}</strong>
            </div>
            <div>
              <span>Backend</span>
              <strong>{model?.backend ?? "—"}</strong>
            </div>
            <div>
              <span>Error rate</span>
              <strong>{fmt(model?.error_rate, "%", 2)}</strong>
            </div>
          </div>
          {labelTotal > 0 && model ? (
            <>
              <div className="label-bar" role="img" aria-label="Prediction label distribution over the last 60 seconds">
                {LABEL_ORDER.map((label) =>
                  model.label_distribution[label] ? <i key={label} className={label} style={{ flexGrow: model.label_distribution[label] }} /> : null,
                )}
              </div>
              <div className="label-legend">
                {LABEL_ORDER.map((label) => (
                  <span key={label}>
                    <i className={label} />
                    {RISK_LABELS[label]} {fmt((100 * model.label_distribution[label]) / labelTotal, "%", 0)}
                  </span>
                ))}
              </div>
            </>
          ) : (
            <p className="empty-state">The prediction distribution appears once this gateway serves predictions.</p>
          )}
          <small className="panel-note">
            {deployment?.status === "ok" ? `Champion ${versionLabel(deployment.champion ?? undefined)} · registry: MLflow` : "Deployment controller unavailable"}
          </small>
        </article>

        <article className="panel detail-panel">
          <div className="panel-heading">
            <div>
              <span className="section-kicker">STREAMING</span>
              <h2>Kafka pipeline</h2>
            </div>
            <span className={kafkaStatus === "healthy" ? "panel-badge" : "mono"}>{kafka ? `${kafka.partitions ?? "—"} partitions` : "disabled"}</span>
          </div>
          {kafka?.status === "ok" ? (
            <>
              <div className="detail-stats">
                <div>
                  <span>Incoming</span>
                  <strong>{fmt(kafka.incoming_rate, "/s")}</strong>
                </div>
                <div>
                  <span>Consumed</span>
                  <strong>{fmt(kafka.consumer_rate, "/s")}</strong>
                </div>
                <div>
                  <span>Consumer lag</span>
                  <strong>{fmt(kafka.lag, "", 0)}</strong>
                </div>
              </div>
              <Sparkline values={kafka.lag_history.length ? kafka.lag_history : [0]} color="#ffb75d" label="Consumer lag over the last 60 seconds" className="panel-sparkline" />
              <small className="panel-note">
                {kafka.topic} → {kafka.consumer_group}
              </small>
            </>
          ) : (
            <p className="empty-state">
              {kafka ? "Kafka is unreachable; async events are paused and synchronous predictions continue." : "The event pipeline is disabled (KAFKA_BOOTSTRAP_SERVERS is empty)."}
            </p>
          )}
        </article>

        <article className="panel detail-panel">
          <div className="panel-heading">
            <div>
              <span className="section-kicker">INFRASTRUCTURE</span>
              <h2>{kube ? "Workloads & autoscaling" : "Consumer replicas"}</h2>
            </div>
            <span className="mono">{kube ? `k8s · ${kube.namespace}` : (infra?.platform ?? "—")}</span>
          </div>
          {kube?.status === "ok" ? (
            <>
              <ul className="workload-list">
                {kube.workloads
                  .filter((w) => SCALED_WORKLOADS.has(w.name))
                  .map((w) => (
                    <li key={w.name}>
                      <div>
                        <strong>{w.name}</strong>
                        <span className={w.ready < w.replicas ? "scaling-label" : "mono-text"}>
                          {w.ready}/{w.replicas} pods{w.desired_replicas != null && w.desired_replicas !== w.replicas ? ` → ${w.desired_replicas}` : ""}
                        </span>
                      </div>
                      <small>
                        {w.autoscaler ? `${w.min_replicas}–${w.max_replicas} · ${w.scaling_metric ?? "metric pending"}` : "fixed replicas"} · CPU {fmt(w.cpu_millicores, "m", 0)} · mem {fmt(w.memory_mib, " MiB", 0)}
                      </small>
                    </li>
                  ))}
              </ul>
              {kube.scaling_events.length ? (
                <ul className="scaling-events">
                  {kube.scaling_events.slice(0, 3).map((e) => (
                    <li key={`${e.at}-${e.message}`}>
                      <span className="mono-text">{new Date(e.at).toLocaleTimeString([], { hour12: false })}</span> {e.target.replace("keda-hpa-", "")}: {e.message.split(";")[0]}
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="empty-state">No scaling events yet. Start a traffic spike: consumer lag drives KEDA to add pods.</p>
              )}
            </>
          ) : infra?.status === "ok" ? (
            <>
              <div className="detail-stats">
                <div>
                  <span>Replicas</span>
                  <strong>{infra.consumer_replicas ?? "—"}</strong>
                </div>
                <div>
                  <span>Group state</span>
                  <strong>{infra.group_state ?? "—"}</strong>
                </div>
                <div>
                  <span>Partitions</span>
                  <strong>{kafka?.partitions ?? "—"}</strong>
                </div>
              </div>
              {infra.members.length ? (
                <ul className="member-list">
                  {infra.members.map((member) => (
                    <li key={member.member_id}>
                      <span className="mono-text">{member.host}</span>
                      <span>{member.partitions.length} partitions</span>
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="empty-state">No consumers are in the group; events queue up in Kafka until one joins.</p>
              )}
              <small className="panel-note">Pod CPU, memory, and autoscaling appear when the stack runs on Kubernetes (make k8s-up).</small>
            </>
          ) : (
            <p className="empty-state">{infra ? "Kafka is unreachable, so consumer membership is unknown." : "Consumer replicas are read from Kafka, which is disabled."}</p>
          )}
        </article>

        <article className="panel release-panel">
          <div className="panel-heading">
            <div>
              <span className="section-kicker">MODEL LIFECYCLE</span>
              <h2>Model releases</h2>
            </div>
            <span className={deployment?.active ? "scaling-label" : "panel-badge"}>
              {deployment?.active ? `Releasing ${versionLabel(deployment.active.version)} · ${deployment.active.stage}` : deployment?.status === "ok" ? "Idle" : "Unavailable"}
            </span>
          </div>
          {deployment?.status === "ok" ? (
            <div className="release-layout">
              <div className="version-list">
                {deployment.versions.map((v) => {
                  const isChampion = v.version === deployment.champion;
                  return (
                    <div key={v.version} className={`version-card ${isChampion ? "champion" : ""}`}>
                      <div>
                        <strong>{versionLabel(v.version)}</strong>
                        <span className={`status-chip ${v.status}`}>{isChampion ? "champion" : v.status.replace("_", " ")}</span>
                      </div>
                      <small>{v.description || v.profile}</small>
                      <small className="mono-text">AUC {fmt(v.auc, "", 3)}</small>
                      <button className="ghost-button" disabled={isChampion || !!deployment.active || releasePending} onClick={() => void releaseModel(v.version)}>
                        {isChampion ? "Serving" : "Release"}
                      </button>
                    </div>
                  );
                })}
              </div>
              <div className="release-history">
                {deployment.active ? <p className="release-active">{deployment.active.detail || "Validating the candidate…"}</p> : null}
                {deployment.history.length ? (
                  <ul>
                    {deployment.history.slice(0, 4).map((r) => (
                      <li key={`${r.version}-${r.started_at}`} className={r.outcome}>
                        <span>{OUTCOME_LABELS[r.outcome] ?? r.outcome}</span>
                        <strong>
                          {versionLabel(r.previous ?? undefined)} → {versionLabel(r.version)}
                        </strong>
                        <small>{r.reasons.length ? r.reasons.join("; ") : r.metrics.psi !== undefined ? `PSI ${fmt(r.metrics.psi, "", 3)} · high-risk ${fmt(100 * (r.metrics.high_risk_rate ?? 0), "%")}` : ""}</small>
                      </li>
                    ))}
                  </ul>
                ) : (
                  <p className="empty-state">No releases yet. Release a candidate: it is gated on its contract and offline metrics, then verified on live traffic and rolled back automatically if its outputs drift from training.</p>
                )}
                <button className="ghost-button" disabled={!!deployment.active || releasePending || !deployment.history.some((r) => r.outcome === "promoted")} onClick={() => void rollbackModel()}>
                  ⟲ Roll back to previous champion
                </button>
                {releaseError ? <p className="inline-error">{releaseError}</p> : null}
              </div>
            </div>
          ) : (
            <p className="empty-state">{overview?.deployment ? "The deployment controller is unreachable." : "No deployment controller is configured (CONTROLLER_URL)."}</p>
          )}
        </article>

        <article className="panel event-panel">
          <div className="panel-heading">
            <div>
              <span className="section-kicker">ACTIVITY</span>
              <h2>System events</h2>
            </div>
            <span className="mono">this session</span>
          </div>
          {events.length ? (
            <div className="event-list">
              {events.map((item) => (
                <div className="event" key={item.id}>
                  <time>{item.time}</time>
                  <i className={item.tone} />
                  <p>{item.message}</p>
                </div>
              ))}
            </div>
          ) : (
            <p className="empty-state">Waiting for the first update from {API_BASE_URL}…</p>
          )}
        </article>
      </section>

      <footer>
        <span>StreamPredict · synthetic demo environment</span>
        <span>Phase 2 · Next.js → FastAPI → Redis / Kafka → consumers → ONNX Runtime model serving</span>
      </footer>
    </main>
  );
}
