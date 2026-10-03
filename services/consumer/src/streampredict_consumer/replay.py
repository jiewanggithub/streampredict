"""Replay dead-lettered events onto their source topic once the cause is fixed.

    python -m streampredict_consumer.replay --dry-run
    python -m streampredict_consumer.replay --error-code inference_unavailable --max 500

Records are read with a dedicated consumer group, so each dead letter is replayed at most once per
group; pass --group to replay again. The original bytes and key are republished unchanged, so
events keep their event_id and stay idempotent downstream.

Each run stops at the dead-letter end offsets captured when it starts. A record that fails again
during the run is dead-lettered past that point and waits for the next run, so a poison event can
never loop between the topics.
"""

import argparse
import asyncio
import json
import logging
from collections import Counter
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

from streampredict_api.logs import configure_logging

from .config import ConsumerSettings, get_consumer_settings

logger = logging.getLogger("streampredict.replay")

DEFAULT_GROUP = "streampredict-dlq-replay"


def header(record: Any, name: str) -> str | None:
    for key, value in record.headers or ():
        if key == name:
            return bytes(value).decode()
    return None


async def replay(
    settings: ConsumerSettings,
    *,
    group: str,
    error_code: str | None,
    max_records: int,
    dry_run: bool,
    idle_seconds: float = 3.0,
) -> Counter[str]:
    topic = settings.kafka_dead_letter_topic
    consumer = AIOKafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=group,
        client_id="streampredict-dlq-replay",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id="streampredict-dlq-replay",
        acks="all",
        enable_idempotence=True,
    )
    stats: Counter[str] = Counter()
    await consumer.start()
    await producer.start()
    try:
        partitions = [TopicPartition(topic, p) for p in await producer.partitions_for(topic)]
        consumer.assign(partitions)
        stop_at: dict[Any, int] = await consumer.end_offsets(partitions)
        pending = {tp for tp in partitions if await consumer.position(tp) < stop_at[tp]}
        while pending and stats["replayed"] + stats["skipped"] < max_records:
            batch = await consumer.getmany(
                *pending, timeout_ms=round(idle_seconds * 1000), max_records=100
            )
            if not batch:
                break
            commits: dict[Any, int] = {}
            for tp, messages in batch.items():
                for record in messages:
                    if record.offset >= stop_at[tp]:
                        pending.discard(tp)
                        break
                    commits[tp] = record.offset + 1
                    code = header(record, "dlq.error_code") or "unknown"
                    if error_code is not None and code != error_code:
                        stats["skipped"] += 1
                        continue
                    target = header(record, "dlq.source_topic") or settings.kafka_prediction_topic
                    if not dry_run:
                        await producer.send_and_wait(target, value=record.value, key=record.key)
                    stats["replayed"] += 1
                    stats[f"code:{code}"] += 1
                if commits.get(tp, -1) >= stop_at[tp]:
                    pending.discard(tp)
            if commits and not dry_run:
                await consumer.commit(commits)
    finally:
        await consumer.stop()
        await producer.stop()
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--group", default=DEFAULT_GROUP, help="consumer group tracking progress")
    parser.add_argument("--error-code", help="only replay dead letters with this dlq.error_code")
    parser.add_argument("--max", type=int, default=10_000, help="stop after this many records")
    parser.add_argument("--dry-run", action="store_true", help="count without publishing")
    args = parser.parse_args()

    settings = get_consumer_settings()
    configure_logging(settings.log_level)
    stats = asyncio.run(
        replay(
            settings,
            group=args.group,
            error_code=args.error_code,
            max_records=args.max,
            dry_run=args.dry_run,
        )
    )
    print(json.dumps({"dry_run": args.dry_run, **stats}, sort_keys=True))


if __name__ == "__main__":
    main()
