"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { EmptyState, ErrorState, LoadingState } from "@/components/data-state";
import { StatusBadge } from "@/components/status-badge";
import { fluxionApi } from "@/lib/api";
import type { RunListItem } from "@/lib/types";

export default function RunsPage() { const [items, setItems] = useState<RunListItem[]>(); const [error, setError] = useState<string>(); useEffect(() => { void fluxionApi.listRuns().then((response) => setItems(response.items)).catch((cause: unknown) => setError(cause instanceof Error ? cause.message : "Unable to load runs.")); }, []); if (error) return <ErrorState message={error} />; if (!items) return <LoadingState label="Loading workflow runs…" />; return <section><p className="text-sm text-cyan-300">Execution history</p><h1 className="mt-1 text-3xl font-semibold text-white">Runs</h1><div className="mt-7 rounded-lg border border-slate-800 bg-slate-900/60">{items.length === 0 ? <div className="p-5"><EmptyState label="No workflow runs are available." /></div> : items.map((run) => <Link key={run.run_id} href={`/runs/${encodeURIComponent(run.run_id)}`} className="flex items-center justify-between gap-4 border-b border-slate-800 px-5 py-4 last:border-0 hover:bg-slate-800/50"><div className="min-w-0"><p className="truncate font-mono text-sm text-slate-200">{run.run_id}</p><p className="mt-1 text-xs text-slate-500">{run.workflow_id} · revision {run.workflow_revision} · {new Date(run.created_at).toLocaleString()}</p></div><StatusBadge status={run.status} /></Link>)}</div></section>; }
