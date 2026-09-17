import argparse
import time
from urllib.error import URLError
from urllib.request import urlopen


def main() -> int:
    parser = argparse.ArgumentParser(description="Wait for Fluxion API readiness.")
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--interval", type=float, default=1)
    args = parser.parse_args()
    if args.timeout <= 0 or args.interval <= 0:
        parser.error("--timeout and --interval must be positive")

    deadline = time.monotonic() + args.timeout
    endpoint = args.api_url.rstrip("/") + "/ready"
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urlopen(endpoint, timeout=2) as response:  # noqa: S310
                if response.status == 200:
                    print("Fluxion API is ready.")
                    return 0
        except (ConnectionRefusedError, ConnectionResetError, OSError, URLError) as exc:
            last_error = exc
        time.sleep(args.interval)
    print(f"Fluxion API did not become ready in time: {last_error!r}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
