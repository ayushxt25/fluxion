import type { RunEvent, RunListResponse, TaskAttemptListResponse, WorkflowDefinition, WorkflowListResponse, WorkflowRun } from "@/lib/types";

const API_ROOT = "/api/fluxion/api/v1";

export class FluxionApiError extends Error {
  constructor(public readonly status: number, message: string) { super(message); }
}

async function request<T>(path: string): Promise<T> {
  const response = await fetch(`${API_ROOT}${path}`, { cache: "no-store" });
  if (!response.ok) {
    const body = await response.json().catch(() => null) as { error?: { message?: string } } | null;
    throw new FluxionApiError(response.status, body?.error?.message ?? `Fluxion API request failed (${response.status}).`);
  }
  return response.json() as Promise<T>;
}

export const fluxionApi = {
  listWorkflows: (limit = 50) => request<WorkflowListResponse>(`/workflows?limit=${limit}`),
  getWorkflow: (workflowId: string) => request<WorkflowDefinition>(`/workflows/${encodeURIComponent(workflowId)}`),
  listRuns: (options: { workflowId?: string; limit?: number } = {}) => {
    const query = new URLSearchParams({ limit: String(options.limit ?? 50) });
    if (options.workflowId) query.set("workflow_id", options.workflowId);
    return request<RunListResponse>(`/runs?${query}`);
  },
  getRun: (runId: string) => request<WorkflowRun>(`/runs/${encodeURIComponent(runId)}`),
  getAttempts: (runId: string, taskId: string) => request<TaskAttemptListResponse>(`/runs/${encodeURIComponent(runId)}/tasks/${encodeURIComponent(taskId)}/attempts`),
};

export function streamRunEvents(runId: string, onEvent: (event: RunEvent) => void, onState: (state: "connecting" | "connected" | "reconnecting") => void): () => void {
  const controller = new AbortController();
  let cursor: number | undefined;
  let stopped = false;

  const connect = async () => {
    while (!stopped) {
      onState(cursor === undefined ? "connecting" : "reconnecting");
      try {
        const query = cursor === undefined ? "" : `?after=${cursor}`;
        const response = await fetch(`${API_ROOT}/runs/${encodeURIComponent(runId)}/events${query}`, { headers: { Accept: "text/event-stream" }, signal: controller.signal, cache: "no-store" });
        if (!response.ok || !response.body) throw new Error(`SSE request failed (${response.status}).`);
        onState("connected");
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        let terminalEvent = false;
        while (!stopped) {
          const { value, done } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          const frames = buffer.split("\n\n");
          buffer = frames.pop() ?? "";
          for (const frame of frames) {
            const data = frame.split("\n").find((line) => line.startsWith("data:"));
            if (!data) continue;
            const event = JSON.parse(data.slice(5)) as RunEvent;
            cursor = event.id;
            onEvent(event);
            if (["run.succeeded", "run.failed", "run.cancelled"].includes(event.event_type)) {
              terminalEvent = true;
            }
          }
          if (terminalEvent) return;
        }
      } catch {
        if (controller.signal.aborted) return;
      }
      if (!stopped) await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  };
  void connect();
  return () => { stopped = true; controller.abort(); };
}
