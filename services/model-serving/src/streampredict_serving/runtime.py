"""ONNX Runtime sessions with dynamic batching, one per loaded model version.

Concurrent requests for the same version are queued and merged into one batch, up to
`max_batch_size` rows or until the oldest request has waited `max_queue_delay`. Each version
executes on its own single worker thread, so the event loop never blocks on inference and batches
for one version run in order.
"""

import asyncio
import contextlib
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import onnxruntime as ort

from .metrics import ServingMetrics
from .repository import ModelConfig, VersionSource

logger = logging.getLogger(__name__)


class InferenceFailed(Exception):
    pass


@dataclass
class _Pending:
    rows: np.ndarray
    future: asyncio.Future[np.ndarray]
    enqueued: float = field(default_factory=time.perf_counter)


class LoadedVersion:
    def __init__(
        self,
        config: ModelConfig,
        source: VersionSource,
        metrics: ServingMetrics,
        *,
        max_batch_size: int,
        max_queue_delay_seconds: float,
        intra_op_threads: int,
    ) -> None:
        self.config = config
        self.version = source.version
        self.metadata = source.metadata
        self._metrics = metrics
        self._labels = (config.name, source.version)
        self._max_batch_size = max_batch_size
        self._max_delay = max_queue_delay_seconds
        options = ort.SessionOptions()
        options.intra_op_num_threads = intra_op_threads
        options.inter_op_num_threads = 1
        self._session = ort.InferenceSession(
            str(source.model_path), sess_options=options, providers=["CPUExecutionProvider"]
        )
        self._input_name = self._session.get_inputs()[0].name
        self._output_name = self._session.get_outputs()[0].name
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix=f"ort-{config.name}-{source.version}"
        )
        self._queue: asyncio.Queue[_Pending] = asyncio.Queue()
        self._worker: asyncio.Task[None] | None = None
        # A request that did not fit the previous batch; it opens the next one, keeping FIFO order.
        self._carry: _Pending | None = None
        self.ready = False

    @property
    def feature_count(self) -> int:
        return self.config.inputs[0].shape[-1]

    async def start(self) -> None:
        # Warm-up: the first run allocates buffers and is noticeably slower than steady state.
        warmup = np.ones((min(self._max_batch_size, 8), self.feature_count), dtype=np.float32)
        await asyncio.get_running_loop().run_in_executor(self._executor, self._run, warmup)
        self._worker = asyncio.create_task(self._batch_loop(), name=f"batcher-{self._labels}")
        self.ready = True
        self._metrics.version_ready.labels(*self._labels).set(1)
        logger.info(
            "Model version ready", extra={"model": self._labels[0], "version": self.version}
        )

    async def stop(self) -> None:
        self.ready = False
        self._metrics.version_ready.labels(*self._labels).set(0)
        if self._worker is not None:
            self._worker.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._worker
        leftovers = [self._carry] if self._carry else []
        while not self._queue.empty():
            leftovers.append(self._queue.get_nowait())
        for pending in leftovers:
            if not pending.future.done():
                pending.future.set_exception(InferenceFailed("model version unloaded"))
        self._executor.shutdown(wait=False)

    async def infer(self, rows: np.ndarray) -> np.ndarray:
        if not self.ready:
            raise InferenceFailed("model version is not ready")
        future: asyncio.Future[np.ndarray] = asyncio.get_running_loop().create_future()
        await self._queue.put(_Pending(rows, future))
        self._metrics.queue_depth.labels(*self._labels).set(self._queue.qsize())
        return await future

    def _run(self, batch: np.ndarray) -> np.ndarray:
        outputs: list[Any] = self._session.run([self._output_name], {self._input_name: batch})
        return np.asarray(outputs[0], dtype=np.float32)

    async def _batch_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            first = self._carry or await self._queue.get()
            self._carry = None
            batch = [first]
            rows = len(first.rows)
            deadline = first.enqueued + self._max_delay
            while rows < self._max_batch_size:
                remaining = deadline - time.perf_counter()
                if remaining <= 0 and self._queue.empty():
                    break
                try:
                    pending = (
                        self._queue.get_nowait()
                        if remaining <= 0
                        else await asyncio.wait_for(self._queue.get(), remaining)
                    )
                except (TimeoutError, asyncio.QueueEmpty):
                    break
                if rows + len(pending.rows) > self._max_batch_size:
                    # Never split a request; it starts the next batch instead.
                    self._carry = pending
                    break
                batch.append(pending)
                rows += len(pending.rows)
            self._metrics.queue_depth.labels(*self._labels).set(self._queue.qsize())
            await self._execute(loop, batch, rows)

    async def _execute(
        self, loop: asyncio.AbstractEventLoop, batch: list[_Pending], rows: int
    ) -> None:
        started = time.perf_counter()
        for pending in batch:
            self._metrics.queue_duration.labels(*self._labels).observe(started - pending.enqueued)
        try:
            output = await loop.run_in_executor(
                self._executor, self._run, np.concatenate([p.rows for p in batch])
            )
        except Exception as exc:
            logger.exception("Batch inference failed", extra={"rows": rows})
            for pending in batch:
                if not pending.future.done():
                    pending.future.set_exception(InferenceFailed(str(exc)))
            return
        self._metrics.compute_duration.labels(*self._labels).observe(time.perf_counter() - started)
        self._metrics.batch_size.labels(*self._labels).observe(rows)
        offset = 0
        for pending in batch:
            size = len(pending.rows)
            if not pending.future.done():  # the caller may have disconnected
                pending.future.set_result(output[offset : offset + size])
            offset += size
