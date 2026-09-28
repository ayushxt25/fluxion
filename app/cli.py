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
        "reaper",
        "worker",
        "webhook",
        "webhooks",
        "retention",
        "schedule-runner",
        "demo",
    ):
        subcommands.add_parser(command, help=f"Run the Fluxion {command} runtime.")
    args = parser.parse_args()
    from app.runtime import (
        api,
        demo,
        publisher,
        reaper,
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
        "reaper": reaper.main,
        "worker": worker.main,
        "webhooks": webhooks.main,
        "webhook": webhooks.main,
        "retention": retention.main,
        "schedule-runner": schedule_runner.main,
        "demo": demo.cli,
    }
    runtimes[args.command]()
