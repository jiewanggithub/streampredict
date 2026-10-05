"""Open-loop load test for the gateway's synchronous prediction path.

    python -m tests.load.run --url http://localhost:8000 --stages 50:30,100:30,200:30 \
        --repeat-ratio 0.5 --output docs/load-test-results.json

Requests are scheduled at a fixed rate regardless of how fast responses come back (open loop),
so queueing shows up as latency instead of silently lowering the offered load. `--repeat-ratio`
controls how many requests reuse a feature vector from a small pool, i.e. the cache hit rate.

The script only needs httpx, so it can also run inside the cluster from the gateway image, which
takes the host's port forwarding and the client machine out of the measurement:

    kubectl -n streampredict run loadtest --rm -i --restart=Never \
        --image=streampredict-api:latest --image-pull-policy=Never -- \
        python - --url http://api:8000 --stages 400:40 < tests/load/run.py
"""

import argparse
import asyncio
import json
import random
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx


@dataclass
class StageResult:
    target_rps: int
    duration_seconds: int
    sent: int = 0
    ok: int = 0
    errors: int = 0
    timeouts: int = 0
    status_counts: dict[str, int] = field(default_factory=dict)
    cache_hits: int = 0
    latencies_ms: list[float] = field(default_factory=list)
    # How far behind its schedule the generator fell: if this grows, the client (not the service)
    # is the bottleneck and the stage's numbers are not a valid measurement of the service.
    max_schedule_lag_ms: float = 0.0

    def summary(self) -> dict[str, object]:
        lat = sorted(self.latencies_ms) or [float("nan")]

        def pct(q: float) -> float:
            return round(lat[min(len(lat) - 1, int(q * len(lat)))], 1)

        return {
            "target_rps": self.target_rps,
            "achieved_rps": round(self.ok / self.duration_seconds, 1),
            "requests": self.sent,
            "success_rate": round(100 * self.ok / self.sent, 3) if self.sent else None,
            "p50_ms": pct(0.50),
            "p95_ms": pct(0.95),
            "p99_ms": pct(0.99),
            "max_ms": round(lat[-1], 1),
            "cache_hit_rate": round(100 * self.cache_hits / self.ok, 1) if self.ok else None,
            # Requests still in flight when the stage's grace period ended.
            "unfinished": self.sent - self.ok - self.errors,
            "errors": self.errors,
            "timeouts": self.timeouts,
            "max_schedule_lag_ms": round(self.max_schedule_lag_ms, 1),
            "client_limited": self.max_schedule_lag_ms > 1000,
            "status_counts": self.status_counts,
        }


def features(
    rng: random.Random, pool: list[dict[str, float]], repeat_ratio: float
) -> dict[str, float]:
    if rng.random() < repeat_ratio:
        return rng.choice(pool)
    return {
        "amount": round(min(rng.lognormvariate(5.5, 1.1), 999_999), 2),
        "events_per_hour": float(rng.randint(0, 40)),
        "distance_km": round(min(rng.expovariate(1 / 300), 19_999), 1),
    }


async def run_stage(
    client: httpx.AsyncClient,
    rps: int,
    seconds: int,
    rng: random.Random,
    pool: list[dict[str, float]],
    repeat_ratio: float,
) -> StageResult:
    result = StageResult(rps, seconds)
    tasks: set[asyncio.Task[None]] = set()

    async def one(body: dict[str, Any]) -> None:
        started = time.perf_counter()
        try:
            response = await client.post("/api/v1/predict", json=body)
        except httpx.TimeoutException:
            result.timeouts += 1
            result.errors += 1
            return
        except httpx.HTTPError:
            result.errors += 1
            return
        elapsed = (time.perf_counter() - started) * 1000
        key = str(response.status_code)
        result.status_counts[key] = result.status_counts.get(key, 0) + 1
        if response.status_code == 200:
            result.ok += 1
            result.latencies_ms.append(elapsed)
            result.cache_hits += response.json().get("cache") == "hit"
        else:
            result.errors += 1

    start = time.perf_counter()
    interval = 1 / rps
    for i in range(rps * seconds):
        # Fixed schedule: request i is due at start + i * interval.
        delay = start + i * interval - time.perf_counter()
        result.max_schedule_lag_ms = max(result.max_schedule_lag_ms, -delay * 1000)
        # Always yield, even when behind schedule, so in-flight requests keep making progress.
        await asyncio.sleep(max(0.0, delay))
        body = {"features": features(rng, pool, repeat_ratio)}
        task = asyncio.create_task(one(body))
        tasks.add(task)
        task.add_done_callback(tasks.discard)
        result.sent += 1
    if tasks:
        await asyncio.wait(tasks, timeout=15)
    return result


async def main_async(args: argparse.Namespace) -> list[dict[str, object]]:
    rng = random.Random(args.seed)
    pool = [features(rng, [], 0.0) for _ in range(200)]
    stages = [tuple(int(x) for x in stage.split(":")) for stage in args.stages.split(",")]
    limits = httpx.Limits(max_connections=args.max_connections, max_keepalive_connections=200)
    summaries = []
    async with httpx.AsyncClient(base_url=args.url, timeout=args.timeout, limits=limits) as client:
        for rps, seconds in stages:
            stage = await run_stage(client, rps, seconds, rng, pool, args.repeat_ratio)
            summary = stage.summary()
            summaries.append(summary)
            print(json.dumps(summary))
            await asyncio.sleep(args.pause)
    return summaries


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--stages", default="50:30,100:30,200:30", help="rps:seconds,...")
    parser.add_argument("--repeat-ratio", type=float, default=0.5)
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--max-connections", type=int, default=400)
    parser.add_argument("--pause", type=float, default=10.0, help="seconds between stages")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    summaries = asyncio.run(main_async(args))
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(summaries, indent=2) + "\n")


if __name__ == "__main__":
    main()
