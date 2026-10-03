# Observability

Every service exposes Prometheus metrics; Prometheus scrapes each replica, evaluates recording and
alerting rules ([`infra/prometheus/rules`](../infra/prometheus/rules)), and feeds the dashboard and
the gateway's RPS autoscaler.

| Where | Prometheus UI |
| --- | --- |
| Docker Compose | <http://localhost:9090> |
| Kubernetes | `make k8s-prometheus`, then <http://localhost:9090> |

## Conventions

- **Names:** `streampredict_<component>_<what>_<unit>`, `_total` for counters, `_seconds` for
  durations (Prometheus base units), `_bucket`/`_sum`/`_count` from histograms.
- **Labels are bounded enums only:** route templates (never raw paths), status codes, cache
  outcome, model version, partition (≤ 12), outcome/reason codes. No request IDs, user input,
  hosts, or free text. A new label must have a known, small value set.
- **One registry per process**, so tests and multiple apps in one process never collide.
- **Recording rules** (`streampredict:*`) precompute what the dashboard and autoscalers read, so
  their queries stay cheap and identical everywhere.
- **cAdvisor** metrics are filtered at scrape time to two series families in this namespace;
  kube-state-metrics is limited to this namespace's pods, deployments, statefulsets, and HPAs.

## Metric catalog

| Component | Metrics |
| --- | --- |
| Gateway | `streampredict_http_requests_total{method,route,status}`, `streampredict_http_request_duration_seconds`, `streampredict_http_requests_in_flight`, `streampredict_predictions_total{source,cache,outcome}`, `streampredict_prediction_duration_seconds{source}`, `streampredict_prediction_labels_total{source,label}`, `streampredict_model_info{model_name,model_version}` |
| Redis cache | `streampredict_cache_operation_duration_seconds{operation}`, `streampredict_cache_errors_total{operation,reason}`, `streampredict_redis_connections{state}` |
| Kafka (gateway) | `streampredict_events_published_total{outcome}`, `streampredict_event_publish_duration_seconds`, `streampredict_kafka_consumer_lag` |
| Consumers | `streampredict_consumer_events_total{outcome}`, `streampredict_consumer_event_duration_seconds`, `streampredict_consumer_event_age_seconds`, `streampredict_consumer_retries_total`, `streampredict_consumer_dead_letters_total{reason}`, `streampredict_consumer_batch_size`, `streampredict_consumer_batch_failures_total`, `streampredict_consumer_partition_lag{partition}`, `streampredict_consumer_assigned_partitions` |
| Model serving | `streampredict_serving_requests_total{model,version,outcome}`, `streampredict_serving_request_duration_seconds`, `streampredict_serving_queue_duration_seconds`, `streampredict_serving_compute_duration_seconds`, `streampredict_serving_batch_rows`, `streampredict_serving_queue_depth`, `streampredict_serving_output_score`, `streampredict_serving_model_ready{model,version}` |
| Controller | `streampredict_controller_releases{outcome}`, `streampredict_controller_champion_version` |
| Demo | `streampredict_demo_active`, `streampredict_demo_target_rps`, `streampredict_demo_elapsed_seconds`, `streampredict_demo_generated_requests_total`, `streampredict_demo_dropped_requests_total`, `streampredict_demo_sessions_total{state,reason}` |
| Kubernetes | kube-state-metrics (`kube_pod_*`, `kube_deployment_*`, `kube_horizontalpodautoscaler_*`), cAdvisor `container_cpu_usage_seconds_total`, `container_memory_working_set_bytes` |

## Alerts

| Alert | Fires when | Severity |
| --- | --- | --- |
| `StreamPredictTargetDown` | A service cannot be scraped for 1 min | critical |
| `ApiHighErrorRate` | 5xx > 5 % of gateway requests for 2 min | critical |
| `PredictionLatencyHigh` | Cluster p95 > 150 ms for 5 min (README target) | warning |
| `KafkaConsumerLagHigh` | Group lag > 1000 for 2 min | warning |
| `DeadLettersIncreasing` | Any event dead-lettered in the last 5 min | warning |
| `ModelReleaseRolledBack` | A release failed its health gate in the last 15 min | warning |
| `ServingNoModelLoaded` | No serving replica has a model version loaded for 1 min | critical |

Active alerts show on the dashboard (banner and system events). `make prometheus-test` unit-tests
the alert rules with promtool and checks both scrape configs. Alert routing (Alertmanager, paging)
is out of scope for the local demo.

## Dashboard sources

| Figure | Source |
| --- | --- |
| RPS, request counts, success rate | Redis per-second counters summed across gateway replicas (1 s resolution) |
| p50 / p95 / p99 | Prometheus recording rules over every replica; local sample if Prometheus is down |
| Cache hit rate, lookup latency, label distribution | The answering gateway replica (a fair sample) |
| Kafka lag and rates | Kafka offsets, read by the gateway |
| Pods, CPU, memory, autoscalers | Kubernetes API (deployments, HPAs, metrics-server, events) |
| Alerts | Prometheus `/api/v1/alerts` |
