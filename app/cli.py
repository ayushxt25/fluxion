import argparse

from app.version import __version__


def main() -> None:
    parser = argparse.ArgumentParser(prog="fluxion")
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {__version__}"
    )
    subcommands = parser.add_subparsers(dest="command", required=True)
    for command in (
        "api",
        "scheduler",
        "publisher",
        "reconciler",
        "coordinator",
        "reaper",
        "worker",
        "webhook",
        "webhooks",
        "retention",
        "schedule-runner",
        "demo",
        "benchmark",
    ):
        subcommands.add_parser(command, help=f"Run the Fluxion {command} runtime.")
    args, remaining = parser.parse_known_args()
    from app.runtime import (
        api,
        benchmark,
        coordinator,
        demo,
        publisher,
        reaper,
        reconciler,
        retention,
        schedule_runner,
        scheduler,
        webhooks,
        worker,
    )

    runtimes = {
        "api": api.main,
        "scheduler": scheduler.main,
        "publisher": publisher.main,
        "reconciler": reconciler.main,
        "coordinator": coordinator.main,
        "reaper": reaper.main,
        "worker": worker.main,
        "webhooks": webhooks.main,
        "webhook": webhooks.main,
        "retention": retention.main,
        "schedule-runner": schedule_runner.main,
        "demo": lambda: demo.cli(remaining),
        "benchmark": lambda: benchmark.cli(remaining),
    }
    if remaining and args.command not in {"benchmark", "demo"}:
        parser.error("unrecognized arguments: " + " ".join(remaining))
    runtimes[args.command]()
