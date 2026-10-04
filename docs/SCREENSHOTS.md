# Dashboard screenshots

The current dashboard is deployed at
[Frontend](https://fluxion-m5h1l4q1e-valerian1.vercel.app/). The checked-in
images are real captures of that interface; they do not use mocked browser
data. Do not capture tokens, developer tools, private URLs, or environment
variables.

## Included captures

- `docs/assets/dashboard.png` — Dashboard overview.
- `docs/assets/demo-playground.png` — constrained predefined workflow launcher.
- `docs/assets/document-run.png` — completed document fan-out/fan-in run.

## Optional future capture

An ETL retry-history image may be added later as `docs/assets/etl-retry-run.png`
only after it is captured from a real run. It is intentionally not referenced in
the README until then.

## Capture process

1. Open the public dashboard in a normal browser window at a consistent desktop
   width (for example, 1440 px).
2. Use the dashboard's real Demo Playground to create any required run, then
   wait for durable completion before capturing a final state.
3. Crop browser chrome and unrelated tabs. Confirm task labels, state badges,
   and DAG edges remain readable.
4. Inspect each image for bearer tokens, URLs with credentials, request headers,
   workflow inputs that should remain private, or failed UI/error states.
5. Save optimized PNGs under `docs/assets/` using these names:
   `dashboard.png`, `demo-playground.png`, `document-run.png`, and
   `etl-retry-run.png`.
6. Add README image links only after the corresponding real files are present.

Avoid recording screenshots that claim a capacity number, reveal infrastructure
credentials, or imply exactly-once execution.
