# Load test: synchronous prediction path

Measured 2026-10-03 against the Kubernetes deployment (`make k8s-up`). Raw results:
[`load-test-results.json`](load-test-results.json). Tool: [`tests/load/run.py`](../tests/load/run.py).

## Setup

| Item | Value |
| --- | --- |
| Cluster | kind, single node, Docker Desktop on Apple Silicon (12 CPUs, 7.7 GB) |
| Path | `POST /api/v1/predict` → gateway → Redis cache → model serving (ONNX Runtime, v2) |
| Gateway | 2–4 pods (KEDA: CPU 70 % or 40 RPS/pod), 1 CPU limit each |
| Model serving | 2–4 pods (HPA: CPU 70 %), dynamic batching |
| Load | Open loop at a fixed rate, 30 s per stage, 50 % of requests repeat a feature vector |
| Generator | Inside the cluster (same image as the gateway), so host port forwarding is not measured |

The generator records how far it falls behind its own schedule. A stage where that lag exceeds
1 s is marked `client_limited`: the client, not the service, was the bottleneck, and its latency
numbers are not a measurement of the service.

## Results

| Offered RPS | Achieved | Success | p50 | p95 | p99 | Valid |
| --- | --- | --- | --- | --- | --- | --- |
| 100 | 100 | 100 % | 2.2 ms | 7.6 ms | 9.1 ms | yes |
| 200 | 200 | 100 % | 2.0 ms | 7.1 ms | 8.9 ms | yes |
| 400 | 400 | 100 % | 2.2 ms | 6.6 ms | 8.4 ms | yes |
| 800 | 800 | 100 % | 4.0 ms | 7.3 ms | 8.8 ms | one run yes; a repeat was client-limited (p95 216 ms) |
| ~2,100 (3 generators) | ~2,085 | 99.0–99.9 % | 81–120 ms | 312–550 ms | 580–950 ms | no: generators 18–53 s behind schedule |

Supporting figures from Prometheus over the same window: Redis lookup p95 1.0 ms (p99 3.6 ms);
ONNX Runtime compute p95 0.5 ms per batch. Under the ~2,100 RPS run the gateway scaled to 4 pods
using about 2.1 CPU in total, model serving to 4 pods using about 0.5 CPU.

## Against the roadmap targets

| Target | Result |
| --- | --- |
| Synchronous p95 < 150 ms | Met: 6.6–7.6 ms up to 400 RPS (and 7.3 ms at 800 RPS in a valid run) |
| Success rate ≥ 99.5 % within the tested load | Met: 100 % in every valid stage |
| Cache read p95 < 10 ms | Met: 1.0 ms |

## Conclusions and limits

- Up to 400 RPS the path is stable and fast; latency is dominated by the HTTP hops, not the
  model (0.5 ms) or Redis (1 ms).
- The service ceiling was **not reached**. Above roughly 800–1,000 RPS one Python generator
  saturates a core, and with three generators the laptop's shared CPU limits client and service
  together. Treat the ~2,100 RPS row as "the service kept ≥ 99 % success while the host was
  saturated", not as a latency figure.
- The cache hit rate varies between runs (49–78 %) because the cache stays warm across stages;
  this lowers latency slightly at low rates.
- An earlier host-side run that appeared to collapse at 400 RPS was a bug in the generator (it
  stopped yielding to the event loop when behind schedule). With the fix, the same run from the
  host through the kind NodePort gives 100 % success and p95 7.1 ms.

To measure the true ceiling, run several generators on machines separate from the cluster nodes.
The event (Kafka) path is characterised in the M3 and M9 notes in the [roadmap](roadmap.md): it is deliberately
throttled for the autoscaling demo (`CONSUMER_SIMULATED_WORK_MS=40`).
