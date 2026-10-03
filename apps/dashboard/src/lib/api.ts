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
};
