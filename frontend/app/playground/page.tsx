"use client";

import { useRouter } from "next/navigation";
import { useState } from "react";

const demos = [
  { id: "document", title: "Document Processing Pipeline", description: "Processes a sample document through validation, parallel extraction, aggregation, summarization, and persistence.", graph: "Ingest → Validate → Text + Metadata → Aggregate → Summary → Persist" },
  { id: "etl", title: "Resilient ETL Pipeline", description: "Runs a multi-stage ETL workflow with a simulated transient failure to demonstrate durable retries and recovery.", graph: "Fetch → Validate (retries once) → Clean + Features → Merge → Persist" },
] as const;

export default function PlaygroundPage() {
  const router = useRouter();
  const [starting, setStarting] = useState<string>();
  const [error, setError] = useState<string>();
  const start = async (id: "document" | "etl") => {
    setStarting(id); setError(undefined);
    try {
      const response = await fetch(`/api/playground/${id}`, { method: "POST" });
      const body = await response.json() as { run_id?: string; error?: string };
      if (!response.ok || !body.run_id) throw new Error(body.error ?? "Unable to start demo.");
      router.push(`/runs/${encodeURIComponent(body.run_id)}`);
    } catch (cause) { setError(cause instanceof Error ? cause.message : "Unable to start demo."); setStarting(undefined); }
  };
  return <section><p className="text-sm text-cyan-300">Public, predefined workflows</p><h1 className="mt-1 text-3xl font-semibold text-white">Demo Playground</h1><p className="mt-2 max-w-2xl text-sm text-slate-400">Each demo runs through Fluxion’s real scheduler, durable outbox, Redis transport, and workers. Visitor input cannot define tasks or workflow JSON.</p>{error && <p className="mt-5 rounded border border-rose-900 bg-rose-950/30 p-3 text-sm text-rose-200">{error}</p>}<div className="mt-7 grid gap-5 lg:grid-cols-2">{demos.map((demo) => <article key={demo.id} className="rounded-lg border border-slate-800 bg-slate-900/60 p-5"><h2 className="text-lg font-medium text-white">{demo.title}</h2><p className="mt-3 text-sm leading-6 text-slate-400">{demo.description}</p><p className="mt-5 rounded bg-slate-950 p-3 font-mono text-xs leading-5 text-cyan-200">{demo.graph}</p><button type="button" disabled={Boolean(starting)} onClick={() => void start(demo.id)} className="mt-6 rounded bg-cyan-400 px-4 py-2 text-sm font-medium text-slate-950 disabled:cursor-wait disabled:opacity-60">{starting === demo.id ? "Starting…" : "Run demo"}</button></article>)}</div></section>;
}
