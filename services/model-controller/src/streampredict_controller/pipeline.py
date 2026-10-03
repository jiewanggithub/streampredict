"""Release state machine: validate -> deploy -> switch traffic -> verify -> promote | roll back.

One release runs at a time. Pre-deploy gates (serving contract, offline metrics) reject a
candidate without touching traffic. After the switch, the previous champion stays loaded, so a
failed post-deploy gate rolls back by flipping the default version: no reload, no cold start.
"""

import asyncio
import logging
import shutil
import time
from dataclasses import asdict, dataclass, field
from typing import Literal

from .config import ControllerSettings
from .gates import ScoreProfile, contract_problems, offline_gate, reference_batch, runtime_gate
from .registry import Registry, ReleaseRecord, read_bundle, scratch_dir
from .serving import ServingAdmin

logger = logging.getLogger(__name__)

Stage = Literal["validating", "deploying", "verifying", "promoted", "rejected", "rolled_back"]
HISTORY_LIMIT = 20


class ReleaseConflict(Exception):
    pass


@dataclass
class ActiveRelease:
    version: str
    previous: str | None
    stage: Stage
    started_at: float
    detail: str = ""


@dataclass
class ControllerEvent:
    at: float
    level: Literal["info", "success", "warning"]
    message: str


@dataclass
class ReleaseManager:
    settings: ControllerSettings
    registry: Registry
    serving: ServingAdmin
    active: ActiveRelease | None = None
    history: list[ReleaseRecord] = field(default_factory=list)
    events: list[ControllerEvent] = field(default_factory=list)
    reconciled: bool = False
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _task: asyncio.Task[None] | None = None

    # --- startup ---------------------------------------------------------------------------

    async def reconcile(self) -> None:
        """Make the deployed repository match the registry's champion (idempotent)."""
        champion = await asyncio.to_thread(self.registry.champion)
        if champion is None:
            raise RuntimeError("the registry has no champion; run the seed job first")
        if champion not in await self.serving.installed():
            await self._install(champion)
        if await self.serving.default() != champion:
            await self.serving.set_default(champion)
        self.history = await asyncio.to_thread(self.registry.history, HISTORY_LIMIT)
        self.reconciled = True
        self._event("info", f"Serving champion v{champion}")

    # --- releases ---------------------------------------------------------------------------

    def start_release(self, version: str) -> ActiveRelease:
        if self._task is not None and not self._task.done():
            raise ReleaseConflict("a release is already in progress")
        self.active = ActiveRelease(version, None, "validating", time.time())
        self._task = asyncio.create_task(self._release(version), name=f"release-{version}")
        return self.active

    def start_rollback(self) -> ActiveRelease:
        if self._task is not None and not self._task.done():
            raise ReleaseConflict("a release is already in progress")
        previous = next((r.previous for r in self.history if r.outcome == "promoted"), None)
        if previous is None:
            raise ReleaseConflict("no previous stable version to roll back to")
        self.active = ActiveRelease(previous, None, "deploying", time.time(), "manual rollback")
        self._task = asyncio.create_task(self._manual_rollback(previous), name="rollback")
        return self.active

    async def wait(self) -> None:
        if self._task is not None:
            await asyncio.shield(self._task)

    async def _release(self, version: str) -> None:
        async with self._lock:
            record = ReleaseRecord(version, None, "rejected", "validating", started_at=time.time())
            try:
                await self._run_release(version, record)
            except Exception as exc:
                logger.exception("Release failed unexpectedly", extra={"version": version})
                record.reasons.append(f"internal error: {exc!r}")
                await self._restore(record)
            finally:
                record.finished_at = time.time()
                self.history.insert(0, record)
                del self.history[HISTORY_LIMIT:]
                self.active = None
                await asyncio.to_thread(self.registry.record_release, record)

    async def _run_release(self, version: str, record: ReleaseRecord) -> None:
        settings = self.settings
        champion = await asyncio.to_thread(self.registry.champion)
        record.previous = champion
        assert self.active is not None
        self.active.previous = champion
        if version == champion:
            record.reasons.append(f"v{version} is already the champion")
            return self._finish(record, "rejected", "validating")
        versions = {v.version: v for v in await asyncio.to_thread(self.registry.versions)}
        if version not in versions:
            record.reasons.append(f"v{version} is not registered")
            return self._finish(record, "rejected", "validating")
        self._event("info", f"Release v{version} started (champion v{champion})")

        # 1. Pre-deploy gates.
        bundle = await asyncio.to_thread(self.registry.download, version, scratch_dir())
        try:
            config, model_file, metadata = read_bundle(bundle)
            expected = ScoreProfile.from_metadata(metadata)
            contract = await self.serving.contract()
            problems = contract_problems(config, contract) if contract else []
            if problems:
                record.reasons.extend(problems)
                await asyncio.to_thread(
                    self.registry.set_status, version, "rejected", "; ".join(problems)
                )
                return self._finish(record, "rejected", "validating")
            offline = offline_gate(
                versions[version].metrics,
                versions[champion].metrics if champion in versions else None,
                min_auc=settings.min_auc,
                max_regression=settings.max_auc_regression,
            )
            record.metrics.update({f"offline_{k}": v for k, v in offline.metrics.items()})
            if not offline.passed:
                record.reasons.extend(offline.reasons)
                await asyncio.to_thread(
                    self.registry.set_status, version, "rejected", "; ".join(offline.reasons)
                )
                return self._finish(record, "rejected", "validating")

            # 2. Deploy next to the champion, then switch traffic.
            self._stage("deploying", "loading the candidate next to the champion")
            await self.serving.install(version, config, model_file)
        finally:
            shutil.rmtree(bundle.parent, ignore_errors=True)
        errors_before, requests_before = await self.serving.counters(version)
        await self.serving.set_default(version)
        self._stage("verifying", f"v{version} is serving traffic; running the health gate")
        self._event("info", f"Traffic switched to v{version}; verifying")

        # 3. Post-deploy health gate.
        batch = reference_batch(settings.reference_rows, settings.reference_seed)
        candidate_probe = await self.serving.probe(version, batch)
        await asyncio.sleep(settings.soak_seconds)
        errors_after, requests_after = await self.serving.counters(version)
        live_requests = requests_after - requests_before - candidate_probe.requests
        live_errors = errors_after - errors_before - candidate_probe.errors
        gate = runtime_gate(
            candidate_probe,
            expected,
            live_errors=max(0, live_errors),
            live_requests=max(0, live_requests),
            max_psi=settings.max_psi,
            max_high_risk_rate_drop=settings.max_high_risk_rate_drop,
            max_p95_latency_ms=settings.max_p95_latency_ms,
            max_error_rate=settings.max_error_rate,
        )
        record.metrics.update(gate.metrics)
        if not gate.passed:
            record.reasons.extend(gate.reasons)
            await self._restore(record)
            return None

        # 4. Promote: the candidate becomes champion; older versions are unloaded, the previous
        #    champion stays as a warm standby for manual rollback.
        await asyncio.to_thread(self.registry.set_champion, version)
        await asyncio.to_thread(self.registry.set_status, version, "champion")
        if champion:
            await asyncio.to_thread(self.registry.set_status, champion, "superseded")
        for installed in await self.serving.installed():
            if installed not in {version, champion}:
                await self.serving.uninstall(installed)
        self._finish(record, "promoted", "promoted")
        return None

    async def _restore(self, record: ReleaseRecord) -> None:
        """Roll traffic back to the previous champion and unload the failed candidate."""
        previous = record.previous
        if previous is None:
            return
        if await self.serving.default() != previous:
            await self.serving.set_default(previous)
        if record.version != previous and record.version in await self.serving.installed():
            await self.serving.uninstall(record.version)
        await asyncio.to_thread(
            self.registry.set_status, record.version, "rolled_back", "; ".join(record.reasons)
        )
        self._finish(record, "rolled_back", "rolled_back")

    async def _manual_rollback(self, target: str) -> None:
        async with self._lock:
            current = await asyncio.to_thread(self.registry.champion)
            record = ReleaseRecord(
                target, current, "manual_rollback", "promoted", started_at=time.time()
            )
            try:
                if target not in await self.serving.installed():
                    await self._install(target)
                await self.serving.set_default(target)
                await asyncio.to_thread(self.registry.set_champion, target)
                await asyncio.to_thread(self.registry.set_status, target, "champion")
                if current and current != target:
                    await asyncio.to_thread(
                        self.registry.set_status, current, "rolled_back", "manual rollback"
                    )
                    await self.serving.uninstall(current)
                record.reasons.append(f"manual rollback from v{current}")
                self._event("warning", f"Manual rollback: v{current} -> v{target}")
            finally:
                record.finished_at = time.time()
                self.history.insert(0, record)
                del self.history[HISTORY_LIMIT:]
                self.active = None
                await asyncio.to_thread(self.registry.record_release, record)

    # --- helpers ----------------------------------------------------------------------------

    async def _install(self, version: str) -> None:
        bundle = await asyncio.to_thread(self.registry.download, version, scratch_dir())
        try:
            config, model_file, _ = read_bundle(bundle)
            await self.serving.install(version, config, model_file)
        finally:
            shutil.rmtree(bundle.parent, ignore_errors=True)

    def _stage(self, stage: Stage, detail: str) -> None:
        if self.active is not None:
            self.active.stage = stage
            self.active.detail = detail

    def _finish(self, record: ReleaseRecord, outcome: str, stage: str) -> None:
        record.outcome = outcome
        record.stage = stage
        reasons = "; ".join(record.reasons)
        if outcome == "promoted":
            self._event("success", f"v{record.version} promoted to champion")
        elif outcome == "rolled_back":
            self._event(
                "warning", f"v{record.version} rolled back to v{record.previous}: {reasons}"
            )
        else:
            self._event("warning", f"v{record.version} rejected before deploy: {reasons}")
        logger.info(
            "Release finished",
            extra={"version": record.version, "outcome": outcome, "reasons": record.reasons},
        )

    def _event(self, level: Literal["info", "success", "warning"], message: str) -> None:
        self.events.insert(0, ControllerEvent(time.time(), level, message))
        del self.events[50:]

    def snapshot(self) -> dict[str, object]:
        return {
            "active": asdict(self.active) if self.active else None,
            "history": [asdict(r) for r in self.history],
            "events": [asdict(e) for e in self.events[:20]],
        }
