"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useCallback, useEffect, useRef, useState } from "react";
import { Dag } from "@/components/dag";
import { ErrorState, LoadingState } from "@/components/data-state";
import { StatusBadge } from "@/components/status-badge";
import { fluxionApi, streamRunEvents } from "@/lib/api";
import type { TaskAttemptListResponse, WorkflowDefinition, WorkflowRun } from "@/lib/types";

const REFRESH_DEBOUNCE_MS = 750;

export default function RunDetailPage() {
  const { runId } = useParams<{ runId: string }>();
  const [run, setRun] = useState<WorkflowRun>();
  const [workflow, setWorkflow] = useState<WorkflowDefinition>();
  const [attempts, setAttempts] = useState<Record<string, TaskAttemptListResponse>>({});
  const [connection, setConnection] = useState("connecting");
  const [error, setError] = useState<string>();
  const refreshing = useRef(false);
  const refreshQueued = useRef(false);
  const refreshTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  const refresh = useCallback(async () => {
    if (refreshing.current) {
      refreshQueued.current = true;
      return;
    }
    refreshing.current = true;
    try {
      const currentRun = await fluxionApi.getRun(runId);
      const definition = await fluxionApi.getWorkflow(currentRun.workflow_id);
      const taskAttempts: [string, TaskAttemptListResponse][] = [];
      for (const task of currentRun.tasks) {
        taskAttempts.push([
          task.task_id,
          await fluxionApi.getAttempts(currentRun.run_id, task.task_id),
        ]);
      }
      setRun(currentRun);
      setWorkflow(definition);
      setAttempts(Object.fromEntries(taskAttempts));
      setError(undefined);
    } catch (cause: unknown) {
      setError(cause instanceof Error ? cause.message : "Unable to load run.");
    } finally {
      refreshing.current = false;
      if (refreshQueued.current) {
        refreshQueued.current = false;
        void refresh();
      }
    }
  }, [runId]);

  useEffect(() => {
    if (!runId) return;
    void refresh();
  }, [runId, refresh]);

  useEffect(() => {
    if (!runId) return;
    const stopStream = streamRunEvents(runId, () => {
      if (refreshTimer.current) return;
      refreshTimer.current = setTimeout(() => {
        refreshTimer.current = undefined;
        void refresh();
      }, REFRESH_DEBOUNCE_MS);
    }, setConnection);
    return () => {
      stopStream();
      if (refreshTimer.current) {
        clearTimeout(refreshTimer.current);
        refreshTimer.current = undefined;
      }
    };
  }, [runId, refresh]);
  if (error) return <ErrorState message={error} />; if (!run || !workflow) return <LoadingState label="Loading durable run state…" />;
  const statuses = Object.fromEntries(run.tasks.map((task) => [task.task_id, task.status]));
  return <section><Link href="/runs" className="text-sm text-cyan-300">← Runs</Link><div className="mt-4 flex flex-wrap items-start justify-between gap-3"><div><p className="font-mono text-sm text-slate-500">{run.run_id}</p><h1 className="mt-1 text-3xl font-semibold text-white">{workflow.name}</h1><Link className="mt-1 block text-sm text-cyan-300" href={`/workflows/${encodeURIComponent(run.workflow_id)}`}>{run.workflow_id} · revision {run.workflow_revision}</Link></div><div className="flex items-center gap-2"><span className="text-xs text-slate-500">SSE {connection}</span><StatusBadge status={run.status} /></div></div><p className="mt-3 text-sm text-slate-400">Created {new Date(run.created_at).toLocaleString()}</p><h2 className="mt-8 mb-3 text-lg font-medium text-white">Live task state</h2><Dag tasks={workflow.tasks} statuses={statuses} /><div className="mt-8 grid gap-4 lg:grid-cols-2">{run.tasks.map((task) => <article key={task.task_id} className="rounded-lg border border-slate-800 bg-slate-900/60 p-4"><div className="flex items-start justify-between gap-3"><div><p className="font-medium text-white">{task.task_id}</p><p className="mt-1 text-xs text-slate-500">{task.attempt_count} attempt{task.attempt_count === 1 ? "" : "s"}</p></div><StatusBadge status={task.status} /></div>{task.next_retry_at && <p className="mt-3 text-xs text-amber-300">Retry due {new Date(task.next_retry_at).toLocaleString()}</p>}{attempts[task.task_id]?.items.map((attempt) => <div key={attempt.attempt_number} className="mt-3 border-t border-slate-800 pt-3 text-xs text-slate-400"><p>Attempt {attempt.attempt_number} · {attempt.status}</p>{attempt.error_message && <p className="mt-1 break-words text-rose-300">{attempt.error_type}: {attempt.error_message}</p>}</div>)}{task.has_result && <pre className="mt-3 overflow-x-auto border-t border-slate-800 pt-3 text-xs text-emerald-200">{JSON.stringify(task.result, null, 2)}</pre>}</article>)}</div></section>;
}
