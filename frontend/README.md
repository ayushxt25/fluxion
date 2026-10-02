# Fluxion dashboard

This Next.js application is the read-only visual control-plane dashboard for
Fluxion. It uses the existing FastAPI API and run-event SSE stream; it does not
change scheduler, worker, or execution semantics.

## Local development

Prerequisites: Node.js 20+ and a running Fluxion API, PostgreSQL, Redis, and
the relevant Fluxion runtime processes. Copy the example configuration:

```bash
cp .env.example .env.local
npm install
npm run dev
```

Open `http://localhost:3000`. Set `FLUXION_API_URL` to the FastAPI base URL
(for example `http://127.0.0.1:8000`). The dashboard proxies browser requests
through its own `/api/fluxion/...` route, so the backend does not need a broad
browser CORS policy for local development.

If Fluxion authentication is enabled, set the server-only `FLUXION_API_TOKEN`
to a JWT with the existing `viewer` (or higher) role. It is deliberately not a
`NEXT_PUBLIC_` value and is never sent to browser JavaScript. The proxy adds it
as the upstream `Authorization: Bearer` header, including for the streamed SSE
endpoint.

## Checks

```bash
npm run typecheck
npm run lint
npm run build
```

## Scope

The first dashboard milestone provides Dashboard, Workflows, and Runs views;
workflow/run DAG inspection; task attempt details; and live run updates. It is
an observability UI, not a workflow editor or an administration interface.
