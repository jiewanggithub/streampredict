"""Consumer-group lag and throughput derived from Kafka offsets.

The gateway reads end offsets and the group's committed offsets once per interval, so the
dashboard can show incoming rate, consumer throughput, and lag without talking to consumers.
Lag is also exported as a Prometheus gauge for alerting and autoscaling.
"""

import asyncio
import contextlib
import logging
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.coordinator.protocol import ConsumerProtocolMemberAssignment
from aiokafka.errors import KafkaError

from .metrics import ApiMetrics
from .schemas import ConsumerMember, InfrastructureMetrics, KafkaMetrics, PartitionLag

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GroupMember:
    member_id: str
    host: str
    partitions: tuple[int, ...]


@dataclass(frozen=True)
class OffsetSnapshot:
    end: dict[int, int]
    committed: dict[int, int]
    group_state: str | None = None
    members: tuple[GroupMember, ...] = ()

    def lag_by_partition(self) -> dict[int, int]:
        # Partitions without a committed offset are fully unconsumed (auto_offset_reset=earliest).
        return {p: max(0, end - self.committed.get(p, 0)) for p, end in self.end.items()}


class OffsetSource(Protocol):
    async def read(self) -> OffsetSnapshot: ...

    async def close(self) -> None: ...


class KafkaOffsetSource:
    def __init__(
        self, bootstrap_servers: str, topic: str, group_id: str, timeout_seconds: float
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._group_id = group_id
        self._timeout = timeout_seconds
        self._consumer: Any = None
        self._admin: AIOKafkaAdminClient | None = None

    async def _clients(self) -> tuple[Any, AIOKafkaAdminClient]:
        if self._consumer is None or self._admin is None:
            timeout_ms = round(self._timeout * 1000)
            consumer = AIOKafkaConsumer(
                bootstrap_servers=self._bootstrap_servers,
                client_id="streampredict-api-monitor",
                enable_auto_commit=False,
                request_timeout_ms=max(timeout_ms, 1000),
            )
            admin = AIOKafkaAdminClient(
                bootstrap_servers=self._bootstrap_servers,
                client_id="streampredict-api-monitor",
                request_timeout_ms=max(timeout_ms, 1000),
            )
            try:
                await consumer.start()
                await admin.start()
            except BaseException:
                await consumer.stop()
                await admin.close()
                raise
            self._consumer, self._admin = consumer, admin
        return self._consumer, self._admin

    async def read(self) -> OffsetSnapshot:
        consumer, admin = await self._clients()
        # Ask the broker each time: the consumer's cached metadata may not know new topics yet.
        (topic,) = await admin.describe_topics([self._topic])
        if topic["error_code"] or not topic["partitions"]:
            raise KafkaError(f"topic {self._topic} is unavailable (error {topic['error_code']})")
        partitions = [TopicPartition(self._topic, p["partition"]) for p in topic["partitions"]]
        end = await consumer.end_offsets(partitions)
        committed = await admin.list_consumer_group_offsets(self._group_id, partitions=partitions)
        state, members = await self._describe_group(admin)
        return OffsetSnapshot(
            end={tp.partition: offset for tp, offset in end.items()},
            committed={
                tp.partition: meta.offset for tp, meta in committed.items() if meta.offset >= 0
            },
            group_state=state,
            members=members,
        )

    async def _describe_group(
        self, admin: AIOKafkaAdminClient
    ) -> tuple[str | None, tuple[GroupMember, ...]]:
        (response,) = await admin.describe_consumer_groups([self._group_id])
        group = response.to_object()["groups"][0]
        if group["error_code"]:
            return None, ()
        members = []
        for member in group["members"]:
            # Members have an empty assignment while the group is rebalancing.
            raw = member["member_assignment"]
            assignment = ConsumerProtocolMemberAssignment.decode(raw).partitions() if raw else []
            partitions = sorted(tp.partition for tp in assignment if tp.topic == self._topic)
            members.append(
                GroupMember(
                    # Member IDs are client_id + UUID; the last UUID group tells them apart.
                    member_id=member["member_id"].rsplit("-", 1)[-1][:12],
                    host=member["client_host"].lstrip("/"),
                    partitions=tuple(partitions),
                )
            )
        return group["state"], tuple(sorted(members, key=lambda m: m.partitions[:1] or (1 << 31,)))

    async def close(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()
        if self._admin is not None:
            await self._admin.close()
        self._consumer = self._admin = None


@dataclass(frozen=True)
class _Sample:
    timestamp: float
    end_total: int
    committed_total: int
    lag: int
    replicas: int


class KafkaMonitor:
    def __init__(
        self,
        source: OffsetSource,
        metrics: ApiMetrics,
        *,
        topic: str,
        consumer_group: str,
        interval_seconds: float = 1.0,
        timeout_seconds: float = 2.0,
        history_seconds: int = 60,
        rate_seconds: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._source = source
        self._metrics = metrics
        self._topic = topic
        self._group = consumer_group
        self._interval = interval_seconds
        self._timeout = timeout_seconds
        self._history_seconds = history_seconds
        self._rate_seconds = rate_seconds
        self._clock = clock
        self._samples: deque[_Sample] = deque(maxlen=history_seconds)
        self._latest: OffsetSnapshot | None = None
        self._available = False
        self._task: asyncio.Task[None] | None = None
        self._stopping = False

    def start(self) -> None:
        self._task = asyncio.create_task(self._loop(), name="kafka-monitor")

    async def stop(self) -> None:
        # Python 3.11's wait_for can swallow a cancellation that races with a finished read, so
        # the loop also checks this flag instead of relying on cancellation alone.
        self._stopping = True
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        await self._source.close()

    @property
    def available(self) -> bool:
        return self._available

    async def _loop(self) -> None:
        while not self._stopping:
            try:
                await self.sample()
            except Exception:
                # Never let one bad sample stop monitoring for the life of the process.
                logger.exception("Kafka monitor sample failed")
                self._available = False
                with contextlib.suppress(Exception):
                    await self._source.close()
            await asyncio.sleep(self._interval)

    async def sample(self) -> None:
        try:
            snapshot = await asyncio.wait_for(self._source.read(), self._timeout)
        except (KafkaError, OSError, TimeoutError) as exc:
            if self._available:
                logger.warning("Kafka offsets unavailable", extra={"error": repr(exc)})
            self._available = False
            # Drop the clients so the next attempt reconnects from scratch.
            with contextlib.suppress(Exception):
                await self._source.close()
            return
        lag = sum(snapshot.lag_by_partition().values())
        self._latest = snapshot
        self._available = True
        self._samples.append(
            _Sample(
                self._clock(),
                sum(snapshot.end.values()),
                sum(snapshot.committed.values()),
                lag,
                len(snapshot.members),
            )
        )
        self._metrics.kafka_consumer_lag.set(lag)

    def _rate(self, field: Callable[[_Sample], int]) -> float | None:
        if len(self._samples) < 2:
            return None
        newest = self._samples[-1]
        oldest = next(
            (s for s in self._samples if newest.timestamp - s.timestamp <= self._rate_seconds),
            newest,
        )
        elapsed = newest.timestamp - oldest.timestamp
        if elapsed <= 0:
            return None
        return round(max(0, field(newest) - field(oldest)) / elapsed, 2)

    def overview(self) -> KafkaMetrics:
        latest = self._latest if self._available else None
        return KafkaMetrics(
            status="ok" if latest is not None else "unavailable",
            topic=self._topic,
            consumer_group=self._group,
            partitions=len(latest.end) if latest else None,
            lag=self._samples[-1].lag if latest and self._samples else None,
            lag_history=[sample.lag for sample in self._samples],
            incoming_rate=self._rate(lambda s: s.end_total) if latest else None,
            consumer_rate=self._rate(lambda s: s.committed_total) if latest else None,
            lag_by_partition=[
                PartitionLag(partition=p, lag=lag)
                for p, lag in sorted(latest.lag_by_partition().items())
            ]
            if latest
            else [],
        )

    def current_lag(self) -> int | None:
        return self._samples[-1].lag if self._available and self._samples else None

    def current_replicas(self) -> int | None:
        return self._samples[-1].replicas if self._available and self._samples else None

    def infrastructure(self, platform: str) -> InfrastructureMetrics:
        latest = self._latest if self._available else None
        return InfrastructureMetrics(
            platform=platform,
            status="ok" if latest is not None else "unavailable",
            consumer_group=self._group,
            group_state=latest.group_state if latest else None,
            consumer_replicas=len(latest.members) if latest else None,
            replicas_history=[sample.replicas for sample in self._samples],
            members=[
                ConsumerMember(member_id=m.member_id, host=m.host, partitions=list(m.partitions))
                for m in latest.members
            ]
            if latest
            else [],
        )
