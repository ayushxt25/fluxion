import argparse

from app.runtime import api, demo, publisher, reaper, scheduler, worker


def main() -> None:
    parser = argparse.ArgumentParser(prog="fluxion")
    subcommands = parser.add_subparsers(dest="command", required=True)
    for command in ("api", "scheduler", "publisher", "reaper", "worker", "demo"):
        subcommands.add_parser(command)
    args = parser.parse_args()
    {
        "api": api.main,
        "scheduler": scheduler.main,
        "publisher": publisher.main,
        "reaper": reaper.main,
        "worker": worker.main,
        "demo": demo.cli,
    }[args.command]()
