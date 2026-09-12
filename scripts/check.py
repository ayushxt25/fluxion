"""Portable local validation commands used by contributors and CI."""

import argparse
import subprocess
import sys

COMMANDS = {
    "lint": [
        ["-m", "ruff", "check", "."],
        ["-m", "compileall", "app", "tests", "alembic", "scripts"],
    ],
    "unit": [
        ["-m", "pytest", "-m", "not integration and not chaos and not stress", "-v"]
    ],
    "integration": [["-m", "pytest", "-m", "integration", "-v"]],
    "chaos": [["-m", "pytest", "-m", "chaos", "-v"]],
    "stress": [["-m", "pytest", "-m", "stress", "-v"]],
    "build": [["-m", "build", "--no-isolation"]],
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Fluxion validation commands.")
    parser.add_argument("command", choices=[*COMMANDS, "ci"])
    args = parser.parse_args()
    commands = (
        [
            *COMMANDS["lint"],
            *COMMANDS["unit"],
            *COMMANDS["integration"],
            *COMMANDS["chaos"],
        ]
        if args.command == "ci"
        else COMMANDS[args.command]
    )
    for command in commands:
        result = subprocess.run([sys.executable, *command], check=False)
        if result.returncode:
            return result.returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
