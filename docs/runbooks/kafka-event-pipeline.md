# Runbook: Kafka event pipeline

## Topology

| Topic | Partitions | Retention | Written by | Read by |
| --- | --- | --- | --- | --- |
| `prediction-events` | 12 | 1 day | API (`POST /api/v1/events`, event-channel demos) | consumer group `streampredict-consumers` |
| `prediction-results` | 6 | 1 day | consumers | downstream readers |
| `prediction-events-dlq` | 3 | 7 days | consumers | replay tool |

Partitions cap consumer parallelism: at most 12 workers share `prediction-events`. Topics are
created by the `kafka-init` Compose service; broker data lives in the `kafka-data` volume.

Delivery is at-least-once. A worker commits offsets only after every result and dead letter of a
batch is acknowledged, and marks finished event IDs in Redis (`sp:v1:event-done:<event_id>`, 24 h
TTL) so redelivered events are skipped. If Redis is down the marker check fails open: an event may
be processed twice, but none is lost.

## Health signals

| Where | Signal |
| --- | --- |
| `GET /ready` | `checks.kafka` (`unavailable` degrades readiness; sync predictions keep working) |
| `GET /api/v1/metrics/overview` | `kafka.lag`, `incoming_rate`, `consumer_rate`, per-partition lag |
| API `/metrics` | `streampredict_kafka_consumer_lag`, `streampredict_events_published_total{outcome}` |
| Consumer `:9102/metrics` | `streampredict_consumer_events_total{outcome}`, `..._partition_lag`, `..._retries_total`, `..._dead_letters_total{reason}`, `..._batch_failures_total` |

```bash
cd infra/docker
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group streampredict-consumers
```

## Lag keeps growing

1. Check the overview: is `consumer_rate` near zero (consumers stuck) or just below `incoming_rate`
   (under-provisioned)?
2. Stuck: check consumer logs for `Kafka unavailable` or repeated `Batch not committed`. A worker
   that joined while a topic was missing picks it up within 15 s (metadata refresh).
3. Under-provisioned: add workers, up to the partition count:
   `CONSUMER_REPLICAS=4 make up` (Kubernetes autoscaling arrives in M9).

## Dead letters

Each dead letter keeps the original key and bytes, with headers `dlq.error_code`,
`dlq.error_message`, `dlq.source_topic`, `dlq.source_partition`, `dlq.source_offset`,
`dlq.attempts`, and `dlq.failed_at`.

| `dlq.error_code` | Meaning | Action |
| --- | --- | --- |
| `invalid_event` | Unparseable or unsupported `schema_version` | Fix the producer; replaying fails again |
| `inference_unavailable` | Model backend down after all retries | Replay once the backend is healthy |
| `internal_error` | Unexpected consumer bug | Fix, deploy, then replay |

Replay after fixing the cause (count first with `--dry-run`):

```bash
cd infra/docker
docker compose run --rm consumer python -m streampredict_consumer.replay --dry-run
docker compose run --rm consumer python -m streampredict_consumer.replay --error-code inference_unavailable
```

A replay run stops at the dead-letter offsets present when it starts, so an event that fails again
waits for the next run instead of looping. Progress is tracked by the `streampredict-dlq-replay`
group; pass `--group <new-name>` to replay records again.
