import type { TaskStatus, WorkflowStatus } from "@/lib/types";

const colors: Record<string, string> = {
  PENDING: "bg-slate-700 text-slate-200", BLOCKED: "bg-slate-800 text-slate-400", READY: "bg-sky-950 text-sky-300", RETRY_WAITING: "bg-violet-950 text-violet-300", DISPATCHED: "bg-indigo-950 text-indigo-300", RUNNING: "bg-amber-950 text-amber-300", SUCCEEDED: "bg-emerald-950 text-emerald-300", FAILED: "bg-rose-950 text-rose-300", CANCELLED: "bg-slate-700 text-slate-300", INTERRUPTED: "bg-orange-950 text-orange-300",
};

export function StatusBadge({ status }: { status: TaskStatus | WorkflowStatus }) { return <span className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${colors[status] ?? "bg-slate-800 text-slate-300"}`}>{status}</span>; }
