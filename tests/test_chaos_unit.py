import asyncio

import pytest

from tests.support.faults import FaultPlan, InjectedFault

pytestmark = pytest.mark.chaos


def test_fault_plan_fails_only_the_configured_checkpoint() -> None:
    async def scenario() -> None:
        plan = FaultPlan()
        plan.fail_next("before.redis.publish")

        await plan.checkpoint("before.worker.claim")
        with pytest.raises(InjectedFault, match="before.redis.publish"):
            await plan.checkpoint("before.redis.publish")
        await plan.checkpoint("before.redis.publish")

    asyncio.run(scenario())


def test_fault_plan_barrier_is_released_deterministically() -> None:
    async def scenario() -> None:
        plan = FaultPlan()
        plan.block("after.worker.claim")
        checkpoint = asyncio.create_task(plan.checkpoint("after.worker.claim"))

        await plan.wait_until_entered("after.worker.claim")
        assert not checkpoint.done()
        plan.release("after.worker.claim")
        await checkpoint

    asyncio.run(scenario())
