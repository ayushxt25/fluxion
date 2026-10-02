"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { ErrorState, LoadingState, EmptyState } from "@/components/data-state";
import { StatusBadge } from "@/components/status-badge";
import { fluxionApi } from "@/lib/api";
import type { RunListItem, WorkflowDefinition } from "@/lib/types";

export default function DashboardPage() {
  const [workflows, setWorkflows] = useState<WorkflowDefinition[]>(); const [runs, setRuns] = useState<RunListItem[]>(); const [error, setError] = useState<string>();
  useEffect(() => { void Promise.all([fluxionApi.listWorkflows(), fluxionApi.listRuns()]).then(([workflowResponse, runResponse]) => { setWorkflows(workflowResponse.items); setRuns(runResponse.items); }).catch((cause: unknown) => setError(cause instanceof Error ? cause.message : "Unable to load Fluxion.")); }, []);
  if (error) return <ErrorState message={error} />; if (!workflows || !runs) return <LoadingState />;
  const statusCount = (status: string) => runs.filter((run) => run.status === status).length;
  return <section><p className="text-sm text-cyan-300">Control plane overview</p><h1 className="mt-1 text-3xl font-semibold text-white">Fluxion dashboard</h1><p className="mt-2 text-sm text-slate-400">Bounded summary from the latest {runs.length} runs.</p><div className="mt-7 grid gap-4 sm:grid-cols-2 xl:grid-cols-5">{[["Workflows", workflows.length], ["Running", statusCount("RUNNING")], ["Succeeded", statusCount("SUCCEEDED")], ["Failed", statusCount("FAILED")], ["Pending", statusCount("PENDING")]].map(([label, value]) => <div key={String(label)} className="rounded-lg border border-slate-800 bg-slate-900/60 p-4"><p className="text-sm text-slate-400">{label}</p><p className="mt-2 text-2xl font-semibold text-white">{value}</p></div>)}</div><div className="mt-8 rounded-lg border border-slate-800 bg-slate-900/60"><div className="flex items-center justify-between border-b border-slate-800 px-5 py-4"><h2 className="font-medium text-white">Recent runs</h2><Link className="text-sm text-cyan-300" href="/runs">View all</Link></div>{runs.length === 0 ? <div className="p-5"><EmptyState label="No workflow runs have been created." /></div> : <ul>{runs.slice(0, 10).map((run) => <li key={run.run_id} className="flex items-center justify-between gap-4 border-b border-slate-800 px-5 py-3 last:border-0"><Link href={`/runs/${encodeURIComponent(run.run_id)}`} className="min-w-0"><p className="truncate font-mono text-sm text-slate-200">{run.run_id}</p><p className="text-xs text-slate-500">{run.workflow_id} · {new Date(run.created_at).toLocaleString()}</p></Link><StatusBadge status={run.status} /></li>)}</ul>}</div></section>;
}
