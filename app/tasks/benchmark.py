"""Small deterministic task implementations used only by the benchmark harness."""

from collections.abc import Callable


async def bench_noop() -> None:
    """Exercise the normal worker path with negligible callable cost."""


def bench_transform(*, value: int = 1) -> dict[str, int]:
    return {"value": value + 1}


def build_benchmark_tasks() -> dict[str, Callable[..., object]]:
    return {
        "bench.noop": bench_noop,
        "bench.root": bench_noop,
        "bench.final": bench_noop,
        **{f"bench.step.{index}": bench_noop for index in range(64)},
        **{f"bench.branch.{index}": bench_noop for index in range(64)},
        "bench.transform": bench_transform,
    }
