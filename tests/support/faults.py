from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field


class InjectedFault(RuntimeError):
    """A deterministic, test-only failure at a named engine boundary."""


@dataclass
class FaultPlan:
    """Coordinates explicit faults and barriers without wall-clock sleeps."""

    _failures: dict[str, list[Exception]] = field(
        default_factory=lambda: defaultdict(list)
    )
    _entered: dict[str, asyncio.Event] = field(
        default_factory=lambda: defaultdict(asyncio.Event)
    )
    _release: dict[str, asyncio.Event] = field(
        default_factory=lambda: defaultdict(asyncio.Event)
    )
    _blocked: set[str] = field(default_factory=set)

    def fail_next(self, point: str, error: Exception | None = None) -> None:
        self._failures[point].append(error or InjectedFault(point))

    def block(self, point: str) -> None:
        self._blocked.add(point)
        self._release[point].clear()

    def release(self, point: str) -> None:
        self._release[point].set()

    async def wait_until_entered(self, point: str) -> None:
        await self._entered[point].wait()

    async def checkpoint(self, point: str) -> None:
        self._entered[point].set()
        if point in self._blocked:
            await self._release[point].wait()
        if self._failures[point]:
            raise self._failures[point].pop(0)


async def at_checkpoint(
    plan: FaultPlan,
    point: str,
    operation: Callable[[], Awaitable[object]],
) -> object:
    """Run a test collaborator operation after an explicit named checkpoint."""

    await plan.checkpoint(point)
    return await operation()
