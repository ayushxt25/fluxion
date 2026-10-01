import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from app.services.coordinator import RunCoordinator, RunCoordinatorLease
from tests.support.faults import InjectedFault


class _Repository:
    def __init__(self) -> None:
        self.claimed = False
        self.released: list[str] = []

    async def claim_available(self, coordinator_id, **_kwargs):
        if self.claimed:
            return ()
        self.claimed = True
        now = datetime.now(UTC)
        return (
            RunCoordinatorLease(
                run_id="run-1",
                workflow_id="workflow-1",
                workflow_revision=1,
                coordinator_id=coordinator_id,
                token="opaque-token",
                expires_at=now + timedelta(seconds=30),
                last_heartbeat_at=now,
            ),
        )

    async def renew(self, lease, **_kwargs):
        return lease

    async def release(self, lease):
        self.released.append(lease.run_id)
        return True


def test_coordinator_renews_then_releases_its_internal_leases() -> None:
    async def scenario() -> None:
        repository = _Repository()
        coordinator = RunCoordinator(
            repository,
            coordinator_id="coordinator-a",
            lease_seconds=30,
            batch_size=10,
        )
        first = await coordinator.coordinate_once()
        second = await coordinator.coordinate_once()
        await coordinator.release_all()

        assert len(first.claimed) == 1
        assert second.renewed == 1
        assert repository.released == ["run-1"]

    asyncio.run(scenario())


def test_coordinator_pass_uses_fenced_recovery_for_claimed_runs() -> None:
    class Recovery:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str]] = []

        async def recover_run(self, run_id, _workflow_id, _revision, **kwargs):
            self.calls.append((run_id, kwargs["coordinator_lease_token"]))

    async def scenario() -> None:
        repository = _Repository()
        recovery = Recovery()
        coordinator = RunCoordinator(
            repository,
            coordinator_id="coordinator-a",
            lease_seconds=30,
            batch_size=10,
            recovery_service=recovery,
        )
        result = await coordinator.coordinate_once()

        assert result.coordinated == 1
        assert recovery.calls == [("run-1", "opaque-token")]

    asyncio.run(scenario())


@pytest.mark.chaos
def test_stale_coordinator_token_is_not_a_public_result() -> None:
    """The service result deliberately contains only internal lease objects."""
    lease = RunCoordinatorLease(
        run_id="run-1",
        workflow_id="workflow-1",
        workflow_revision=1,
        coordinator_id="coordinator-a",
        token="opaque-token",
        expires_at=datetime.now(UTC),
        last_heartbeat_at=datetime.now(UTC),
    )

    assert "opaque-token" not in repr({"run_id": lease.run_id})


@pytest.mark.chaos
def test_heartbeat_failure_keeps_the_previous_durable_lease_for_retry() -> None:
    class FailingRepository(_Repository):
        def __init__(self) -> None:
            super().__init__()
            self.fail_heartbeat = True

        async def renew(self, lease, **_kwargs):
            if self.fail_heartbeat:
                self.fail_heartbeat = False
                raise InjectedFault("before.run_coordinator.heartbeat.commit")
            return lease

    async def scenario() -> None:
        repository = FailingRepository()
        coordinator = RunCoordinator(
            repository,
            coordinator_id="coordinator-a",
            lease_seconds=30,
            batch_size=1,
        )
        await coordinator.coordinate_once()
        with pytest.raises(InjectedFault, match="heartbeat"):
            await coordinator.coordinate_once()
        assert (await coordinator.coordinate_once()).renewed == 1

    asyncio.run(scenario())


@pytest.mark.chaos
def test_release_failure_keeps_ownership_for_expiry_or_later_release() -> None:
    class FailingRepository(_Repository):
        def __init__(self) -> None:
            super().__init__()
            self.fail_release = True

        async def release(self, lease):
            if self.fail_release:
                self.fail_release = False
                raise InjectedFault("before.run_coordinator.release.commit")
            return await super().release(lease)

    async def scenario() -> None:
        repository = FailingRepository()
        coordinator = RunCoordinator(
            repository,
            coordinator_id="coordinator-a",
            lease_seconds=30,
            batch_size=1,
        )
        await coordinator.coordinate_once()
        with pytest.raises(InjectedFault, match="release"):
            await coordinator.release_all()
        await coordinator.release_all()
        assert repository.released == ["run-1"]

    asyncio.run(scenario())
