# Release Checklist

Fluxion releases are prepared from a clean Git worktree. Do not publish an
artifact until every applicable validation below has passed.

1. Confirm the version with `fluxion --version` and update
   `app/version.py` for the intended release.
2. Run `python scripts/check.py ci` and, when infrastructure is available,
   `python scripts/check.py stress`.
3. Run `alembic upgrade head`, `alembic downgrade -1`, and `alembic upgrade
   head` against an expendable database.
4. Build with `python -m build --no-isolation`; install the wheel into a clean
   virtual environment and run `fluxion --help` plus every runtime `--help`.
5. Build the container with `docker build -t fluxion:release .`, then run
   `docker run --rm fluxion:release fluxion --help` and `docker compose config`.
6. For a local cluster smoke check, set a non-production `JWT_SECRET`, run
   `docker compose up -d`, wait for `/ready`, run `fluxion-demo`, and tear the
   stack down with `docker compose down -v`.
7. Review migration revision history for one head and confirm no `.env`,
   credentials, or generated database files are staged.
8. Tag and publish only after the GitHub Actions CI and scheduled/manual stress
   workflow have the expected green results.
