"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { EmptyState, ErrorState, LoadingState } from "@/components/data-state";
import { fluxionApi } from "@/lib/api";
import type { WorkflowDefinition } from "@/lib/types";

export default function WorkflowsPage() { const [items, setItems] = useState<WorkflowDefinition[]>(); const [error, setError] = useState<string>(); useEffect(() => { void fluxionApi.listWorkflows().then((response) => setItems(response.items)).catch((cause: unknown) => setError(cause instanceof Error ? cause.message : "Unable to load workflows.")); }, []); if (error) return <ErrorState message={error} />; if (!items) return <LoadingState label="Loading workflows…" />; return <section><p className="text-sm text-cyan-300">Definitions</p><h1 className="mt-1 text-3xl font-semibold text-white">Workflows</h1><div className="mt-7 rounded-lg border border-slate-800 bg-slate-900/60">{items.length === 0 ? <div className="p-5"><EmptyState label="No workflow definitions are available." /></div> : items.map((workflow) => <Link key={workflow.id} href={`/workflows/${encodeURIComponent(workflow.id)}`} className="block border-b border-slate-800 p-5 last:border-0 hover:bg-slate-800/50"><div className="flex items-center justify-between gap-4"><div><p className="font-medium text-white">{workflow.name}</p><p className="mt-1 font-mono text-xs text-slate-500">{workflow.id}</p></div><p className="text-sm text-slate-400">r{workflow.revision} · {workflow.tasks.length} tasks</p></div></Link>)}</div></section>; }
