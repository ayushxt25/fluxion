"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useEffect, useState } from "react";
import { Dag } from "@/components/dag";
import { EmptyState, ErrorState, LoadingState } from "@/components/data-state";
import { StatusBadge } from "@/components/status-badge";
import { fluxionApi } from "@/lib/api";
import type { RunListItem, WorkflowDefinition } from "@/lib/types";

export default function WorkflowDetailPage() {
  const { id } = useParams<{ id: string }>(); const [workflow, setWorkflow] = useState<WorkflowDefinition>(); const [runs, setRuns] = useState<RunListItem[]>(); const [error, setError] = useState<string>();
  useEffect(() => { if (!id) return; void Promise.all([fluxionApi.getWorkflow(id), fluxionApi.listRuns({ workflowId: id, limit: 20 })]).then(([definition, response]) => { setWorkflow(definition); setRuns(response.items); }).catch((cause: unknown) => setError(cause instanceof Error ? cause.message : "Unable to load workflow.")); }, [id]);
  if (error) return <ErrorState message={error} />; if (!workflow || !runs) return <LoadingState label="Loading workflow definition…" />;
  return <section><Link href="/workflows" className="text-sm text-cyan-300">← Workflows</Link><div className="mt-4 flex flex-wrap items-start justify-between gap-3"><div><p className="font-mono text-sm text-slate-500">{workflow.id}</p><h1 className="mt-1 text-3xl font-semibold text-white">{workflow.name}</h1></div><span className="rounded bg-slate-800 px-2 py-1 text-sm text-slate-300">Revision {workflow.revision}</span></div><h2 className="mt-8 mb-3 text-lg font-medium text-white">Workflow DAG</h2><Dag tasks={workflow.tasks} /><div className="mt-8 rounded-lg border border-slate-800 bg-slate-900/60"><div className="border-b border-slate-800 px-5 py-4"><h2 className="font-medium text-white">Recent runs</h2></div>{runs.length === 0 ? <div className="p-5"><EmptyState label="No runs for this workflow." /></div> : runs.map((run) => <Link key={run.run_id} href={`/runs/${encodeURIComponent(run.run_id)}`} className="flex items-center justify-between gap-4 border-b border-slate-800 px-5 py-3 last:border-0 hover:bg-slate-800/50"><div><p className="font-mono text-sm text-slate-200">{run.run_id}</p><p className="text-xs text-slate-500">{new Date(run.created_at).toLocaleString()}</p></div><StatusBadge status={run.status} /></Link>)}</div></section>;
}
