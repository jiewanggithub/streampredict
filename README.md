# StreamPredict

StreamPredict is a portfolio-ready real-time machine-learning serving system. It combines an interactive web demo with asynchronous event processing, online features and caching, versioned model serving, observability, autoscaling, and safe rollback.

## Demo experience

Users open a React / Next.js dashboard and select **Run Demo** or **Start Traffic Spike**. The dashboard then shows the system responding in real time:

- requests per second and p95 latency
- Kafka consumer lag and throughput
- Kubernetes autoscaling activity
- success rate and Redis cache hit rate
- active model version and rollback status

## Planned architecture

- **Frontend:** React / Next.js dashboard
- **API:** FastAPI prediction, demo-control, and metrics endpoints
- **Streaming:** Kafka producer, topic, and consumer group
- **Online data:** Redis feature store and prediction cache
- **Inference:** TorchServe
- **Model lifecycle:** MLflow registry and S3 artifacts
- **Observability:** Prometheus metrics
- **Runtime:** Kubernetes deployments and HPA

The current architecture diagram is available at [`docs/architecture/streampredict-architecture-demo.pdf`](docs/architecture/streampredict-architecture-demo.pdf).

## Repository layout

```text
apps/                  User-facing applications
services/              API, workers, and model-serving components
infra/                 Local and Kubernetes infrastructure
docs/architecture/     Architecture diagrams and design notes
```

## Status

Initial repository setup. Implementation will proceed incrementally, beginning with a locally runnable end-to-end demo.

