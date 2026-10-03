export const API_BASE_URL = (process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000").replace(/\/$/, "");

export type DependencyStatus = "ok" | "unavailable";
export type DemoState = "idle" | "starting" | "running" | "cooling_down" | "completed" | "failed";
export type DemoProfile = "standard" | "spike";
export type DemoChannel = "sync" | "events";
export type RiskLabel = "low_risk" | "review" | "high_risk";
export type CacheStatus = "hit" | "miss" | "bypass";

export type DemoSummary = {
  total_requests: number;
  errors: number;
  dropped: number;
  peak_rps: number;
  cache_hit_rate: number | null;
  duration_seconds: number;
  max_consumer_lag?: number | null;
  peak_consumer_replicas?: number | null;
  consumer_scaling_events?: number | null;
  lag_recovery_seconds?: number | null;
};

export type DemoStatus = {
  session_id: string | null;
  state: DemoState;
  profile: DemoProfile | null;
  channel: DemoChannel | null;
  current_target_rps: number;
  target_rps: number;
  max_rps: number;
  elapsed_seconds: number;
  duration_seconds: number;
  max_duration_seconds: number;
  generated_requests: number;
  errors: number;
  started_at: string | null;
  ended_at: string | null;
  stop_reason: "duration_reached" | "manual" | "shutdown" | "error" | null;
  summary: DemoSummary | null;
};

export type KafkaMetrics = {
  status: DependencyStatus;
  topic: string;
  consumer_group: string;
  partitions: number | null;
  lag: number | null;
  lag_history: number[];
  incoming_rate: number | null;
  consumer_rate: number | null;
  lag_by_partition: { partition: number; lag: number }[];
};

export type InfrastructureMetrics = {
  platform: string;
  status: DependencyStatus;
  consumer_group: string;
  group_state: string | null;
  consumer_replicas: number | null;
  replicas_history: number[];
  members: { member_id: string; host: string; partitions: number[] }[];
  kubernetes?: KubernetesStatus | null;
};

export type WorkloadStatus = {
  name: string;
  replicas: number;
  ready: number;
  cpu_millicores: number | null;
  memory_mib: number | null;
  autoscaler: string | null;
  min_replicas: number | null;
  max_replicas: number | null;
  desired_replicas: number | null;
  scaling_metric: string | null;
};

export type KubernetesStatus = {
  status: DependencyStatus;
  namespace: string;
  workloads: WorkloadStatus[];
  scaling_events: { at: string; target: string; message: string }[];
};

export type ModelVersionInfo = { version: string; profile: string; description: string; status: string; auc: number | null };

export type ReleaseRecord = {
  version: string;
  previous: string | null;
  outcome: "promoted" | "rejected" | "rolled_back" | "manual_rollback" | string;
  stage: string;
  reasons: string[];
  metrics: Record<string, number>;
  started_at: number;
  finished_at: number;
};

export type DeploymentStatus = {
  status: DependencyStatus;
  champion: string | null;
  serving_default: string | null;
  versions: ModelVersionInfo[];
  active: { version: string; previous: string | null; stage: string; started_at: number; detail: string } | null;
  history: ReleaseRecord[];
  events: { at: number; level: string; message: string }[];
};

export type AlertInfo = { name: string; severity: string; state: "pending" | "firing" | "inactive"; summary: string; active_at: string | null };

export type ObservabilityStatus = {
  status: DependencyStatus;
  p50_ms: number | null;
  p95_ms: number | null;
  p99_ms: number | null;
  alerts: AlertInfo[];
};

export type MetricsOverview = {
  generated_at: string;
  window_seconds: number;
  model: {
    name: string;
    version: string;
    backend: string;
    predictions_in_window: number;
    error_rate: number | null;
    label_distribution: Record<RiskLabel, number>;
  };
  traffic: {
    scope?: "cluster" | "replica";
    latency_scope?: "cluster" | "replica";
    rps: number;
    rps_history: number[];
    p50_ms: number | null;
    p95_ms: number | null;
    p99_ms: number | null;
    success_rate: number | null;
    requests_in_window: number;
    errors_in_window: number;
  };
  cache: {
    hit_rate: number | null;
    miss_rate: number | null;
    bypass_count: number;
    lookup_p95_ms: number | null;
    ttl_seconds: number;
  };
  dependencies: Record<string, DependencyStatus>;
  demo: DemoStatus;
  kafka: KafkaMetrics | null;
  infrastructure: InfrastructureMetrics | null;
  deployment: DeploymentStatus | null;
  observability?: ObservabilityStatus | null;
};

export type PredictionFeatures = {
  amount: number;
  events_per_hour: number;
  distance_km: number;
};

export type PredictResponse = {
  request_id: string;
  model_name: string;
  model_version: string;
  label: RiskLabel;
  score: number;
  cache: CacheStatus;
  latency_ms: number;
};

export class ApiError extends Error {
  constructor(
    message: string,
    readonly code: string,
    readonly status: number,
  ) {
    super(message);
  }
}

async function request<T>(path: string, init?: RequestInit, timeoutMs = 5000): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`${API_BASE_URL}${path}`, {
      ...init,
      headers: { "Content-Type": "application/json", ...init?.headers },
      signal: AbortSignal.timeout(timeoutMs),
    });
  } catch {
    throw new ApiError(`Cannot reach the API at ${API_BASE_URL}`, "network_error", 0);
  }
  const body: unknown = await response.json().catch(() => null);
  if (!response.ok) {
    const error = (body as { error?: { code?: string; message?: string } } | null)?.error;
    throw new ApiError(error?.message ?? `Request failed (${response.status})`, error?.code ?? "http_error", response.status);
  }
  return body as T;
}

export const api = {
  streamUrl: `${API_BASE_URL}/api/v1/metrics/stream`,
  overview: () => request<MetricsOverview>("/api/v1/metrics/overview", undefined, 3000),
  predict: (features: PredictionFeatures) =>
    request<PredictResponse>("/api/v1/predict", { method: "POST", body: JSON.stringify({ features }) }),
  startDemo: (profile: DemoProfile, channel: DemoChannel) =>
    request<DemoStatus>("/api/v1/demo/traffic-spike", { method: "POST", body: JSON.stringify({ profile, channel }) }),
  stopDemo: () => request<DemoStatus>("/api/v1/demo/stop", { method: "POST" }),
  releaseModel: (version: string) => request<unknown>("/api/v1/models/releases", { method: "POST", body: JSON.stringify({ version }) }),
  rollbackModel: () => request<unknown>("/api/v1/models/rollback", { method: "POST" }),
};
