"""Durable PostgreSQL ownership for run-level recovery coordination.

This deliberately does not replace task worker leases.  A coordinator lease only
fences run-level coordination decisions; workers continue to fence task attempts.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import and_, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.execution import WorkflowRunRecord
from app.engine.status import WorkflowStatus
from app.observability.metrics import (
    record_run_coordinator_claim,
    record_run_coordinator_pass,
    record_run_coordinator_release,
    record_run_coordinator_renewal,
)
from app.services.recovery import WorkflowRecoveryService

logger = logging.getLogger(__name__)

_COORDINATABLE_STATUSES = (WorkflowStatus.PENDING.value, WorkflowStatus.RUNNING.value)


@dataclass(frozen=True)
class RunCoordinatorLease:
    """Internal ownership capability.  It is never serialized publicly."""

    run_id: str
    workflow_id: str
    workflow_revision: int
    coordinator_id: str
    token: str
    expires_at: datetime
    last_heartbeat_at: datetime


@dataclass(frozen=True)
class RunCoordinatorPassResult:
    claimed: tuple[RunCoordinatorLease, ...]
    renewed: int
    coordinated: int


class RunCoordinatorRepository:
    """Atomic PostgreSQL lease persistence for workflow-run coordinators."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def claim_available(
        self,
        coordinator_id: str,
        *,
        lease_seconds: float,
        limit: int,
        now: datetime | None = None,
    ) -> tuple[RunCoordinatorLease, ...]:
        if lease_seconds <= 0 or limit <= 0:
            raise ValueError("lease_seconds and limit must be positive.")
        now = now or datetime.now(UTC)
        expires_at = now + timedelta(seconds=lease_seconds)
        claimed: list[RunCoordinatorLease] = []
        async with self._session.begin():
            query = (
                select(WorkflowRunRecord)
                .where(WorkflowRunRecord.status.in_(_COORDINATABLE_STATUSES))
                .where(
                    or_(
                        WorkflowRunRecord.coordinator_lease_expires_at.is_(None),
                        WorkflowRunRecord.coordinator_lease_expires_at <= now,
                    )
                )
                .order_by(
                    WorkflowRunRecord.coordinator_lease_expires_at,
                    WorkflowRunRecord.run_id,
                )
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
            rows = (await self._session.execute(query)).scalars().all()
            for row in rows:
                takeover = row.coordinator_id is not None
                token = str(uuid.uuid4())
                row.coordinator_id = coordinator_id
                row.coordinator_lease_token = token
                row.coordinator_lease_expires_at = expires_at
                row.coordinator_last_heartbeat_at = now
                claimed.append(
                    RunCoordinatorLease(
                        run_id=row.run_id,
                        workflow_id=row.workflow_id,
                        workflow_revision=row.workflow_revision,
                        coordinator_id=coordinator_id,
                        token=token,
                        expires_at=expires_at,
                        last_heartbeat_at=now,
                    )
                )
                record_run_coordinator_claim("takeover" if takeover else "claimed")
                logger.info(
                    "run.coordinator.%s run_id=%s coordinator_id=%s",
                    "takeover" if takeover else "claim",
                    row.run_id,
                    coordinator_id,
                )
        return tuple(claimed)

    async def renew(
        self,
        lease: RunCoordinatorLease,
        *,
        lease_seconds: float,
        now: datetime | None = None,
    ) -> RunCoordinatorLease | None:
        now = now or datetime.now(UTC)
        expires_at = now + timedelta(seconds=lease_seconds)
        async with self._session.begin():
            result = await self._session.execute(
                update(WorkflowRunRecord)
                .where(WorkflowRunRecord.run_id == lease.run_id)
                .where(WorkflowRunRecord.status.in_(_COORDINATABLE_STATUSES))
                .where(WorkflowRunRecord.coordinator_id == lease.coordinator_id)
                .where(WorkflowRunRecord.coordinator_lease_token == lease.token)
                .where(WorkflowRunRecord.coordinator_lease_expires_at > now)
                .values(
                    coordinator_lease_expires_at=expires_at,
                    coordinator_last_heartbeat_at=now,
                )
            )
        if result.rowcount != 1:
            record_run_coordinator_renewal("fenced")
            logger.info(
                "run.coordinator.fenced run_id=%s coordinator_id=%s",
                lease.run_id,
                lease.coordinator_id,
            )
            return None
        record_run_coordinator_renewal("success")
        return RunCoordinatorLease(
            run_id=lease.run_id,
            workflow_id=lease.workflow_id,
            workflow_revision=lease.workflow_revision,
            coordinator_id=lease.coordinator_id,
            token=lease.token,
            expires_at=expires_at,
            last_heartbeat_at=now,
        )

    async def release(self, lease: RunCoordinatorLease) -> bool:
        async with self._session.begin():
            result = await self._session.execute(
                update(WorkflowRunRecord)
                .where(WorkflowRunRecord.run_id == lease.run_id)
                .where(WorkflowRunRecord.coordinator_id == lease.coordinator_id)
                .where(WorkflowRunRecord.coordinator_lease_token == lease.token)
                .values(
                    coordinator_id=None,
                    coordinator_lease_token=None,
                    coordinator_lease_expires_at=None,
                    coordinator_last_heartbeat_at=None,
                )
            )
        outcome = "success" if result.rowcount == 1 else "fenced"
        record_run_coordinator_release(outcome)
        if result.rowcount == 1:
            logger.info(
                "run.coordinator.release run_id=%s coordinator_id=%s",
                lease.run_id,
                lease.coordinator_id,
            )
        return result.rowcount == 1

    async def current(self, lease: RunCoordinatorLease) -> bool:
        """Check a token within a coordinator-owned persistence boundary."""
        async with self._session.begin():
            result = await self._session.execute(
                select(WorkflowRunRecord.run_id).where(
                    and_(
                        WorkflowRunRecord.run_id == lease.run_id,
                        WorkflowRunRecord.coordinator_id == lease.coordinator_id,
                        WorkflowRunRecord.coordinator_lease_token == lease.token,
                        WorkflowRunRecord.coordinator_lease_expires_at
                        > datetime.now(UTC),
                    )
                )
            )
            return result.scalar_one_or_none() is not None


class RunCoordinator:
    """Coordinates bounded run ownership without scheduling or executing tasks."""

    def __init__(
        self,
        repository: RunCoordinatorRepository,
        *,
        coordinator_id: str,
        lease_seconds: float,
        batch_size: int,
        recovery_service: WorkflowRecoveryService | None = None,
    ) -> None:
        self._repository = repository
        self._coordinator_id = coordinator_id
        self._lease_seconds = lease_seconds
        self._batch_size = batch_size
        self._recovery_service = recovery_service
        self._leases: dict[str, RunCoordinatorLease] = {}

    async def coordinate_once(self) -> RunCoordinatorPassResult:
        renewed = 0
        for run_id, lease in tuple(self._leases.items()):
            replacement = await self._repository.renew(
                lease,
                lease_seconds=self._lease_seconds,
            )
            if replacement is None:
                self._leases.pop(run_id, None)
            else:
                self._leases[run_id] = replacement
                renewed += 1
        claimed = await self._repository.claim_available(
            self._coordinator_id,
            lease_seconds=self._lease_seconds,
            limit=self._batch_size,
        )
        self._leases.update({lease.run_id: lease for lease in claimed})
        coordinated = 0
        if self._recovery_service is not None:
            for lease in tuple(self._leases.values()):
                await self._recovery_service.recover_run(
                    lease.run_id,
                    lease.workflow_id,
                    lease.workflow_revision,
                    coordinator_id=lease.coordinator_id,
                    coordinator_lease_token=lease.token,
                )
                coordinated += 1
        record_run_coordinator_pass(len(claimed))
        logger.info(
            "run.coordinator.pass coordinator_id=%s claimed=%s renewed=%s "
            "coordinated=%s",
            self._coordinator_id,
            len(claimed),
            renewed,
            coordinated,
        )
        return RunCoordinatorPassResult(
            claimed=claimed,
            renewed=renewed,
            coordinated=coordinated,
        )

    async def release_all(self) -> None:
        for run_id, lease in tuple(self._leases.items()):
            await self._repository.release(lease)
            self._leases.pop(run_id, None)
