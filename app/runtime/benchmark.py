import argparse
import asyncio
import json
import sys
from pathlib import Path

from app.benchmarking import BenchmarkConfig, require_benchmark_database, run_benchmark
from app.core.config import get_settings


def _parse(argv: list[str] | None) -> tuple[argparse.Namespace, BenchmarkConfig]:
    parser = argparse.ArgumentParser(prog="fluxion benchmark")
    parser.add_argument(
        "--workload", choices=("single", "linear", "fanout"), default="single"
    )
    parser.add_argument("--runs", type=int, default=1_000)
    parser.add_argument("--tasks-per-run", type=int, default=4)
    parser.add_argument("--fanout", type=int, default=4)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--worker-concurrency", type=int, default=1)
    parser.add_argument("--warmup-runs", type=int, default=0)
    parser.add_argument("--timeout", type=float, default=300.0)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        config = BenchmarkConfig(
            workload=args.workload,
            runs=args.runs,
            tasks_per_run=args.tasks_per_run,
            fanout=args.fanout,
            workers=args.workers,
            worker_concurrency=args.worker_concurrency,
            warmup_runs=args.warmup_runs,
            timeout_seconds=args.timeout,
        )
    except ValueError as exc:
        parser.error(str(exc))
    return args, config


async def _run(config: BenchmarkConfig, output: Path | None) -> int:
    settings = get_settings()
    require_benchmark_database(settings.database_url)
    result = await run_benchmark(config, settings)
    payload = result.as_dict()
    print("Fluxion benchmark")
    print("-----------------")
    print(f"Workload: {config.workload}")
    print(f"Runs: {config.runs}")
    print(f"Task executions: {config.expected_executions}")
    print(f"Workers: {config.workers}")
    print(f"Duration: {result.duration_seconds:.3f}s")
    print(f"Throughput: {result.results['tasks_per_second']:.3f} tasks/s")
    print(f"Correctness: {'PASS' if result.correctness['valid'] else 'INVALID'}")
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return 0 if result.correctness["valid"] else 1


def main(argv: list[str] | None = None) -> int:
    args, config = _parse(argv)
    try:
        return asyncio.run(_run(config, args.output))
    except (TimeoutError, ValueError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except Exception:
        print("Benchmark could not reach PostgreSQL or Redis.", file=sys.stderr)
        return 1


def cli(argv: list[str] | None = None) -> None:
    raise SystemExit(main(argv))
