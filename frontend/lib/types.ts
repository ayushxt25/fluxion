export type WorkflowStatus = "PENDING" | "RUNNING" | "SUCCEEDED" | "FAILED" | "CANCELLED";
export type TaskStatus =
  | "BLOCKED"
  | "PENDING"
  | "RETRY_WAITING"
  | "READY"
  | "DISPATCHED"
  | "RUNNING"
  | "SUCCEEDED"
  | "FAILED"
  | "CANCELLED"
  | "INTERRUPTED";

export interface WorkflowTask {
  id: string;
  name: string | null;
  depends_on: string[];
  retry_policy: { max_attempts: number; initial_backoff_seconds: number; backoff_multiplier: number; max_backoff_seconds: number | null };
}

export interface WorkflowDefinition {
  id: string;
  name: string;
  revision: number;
  created_at: string | null;
  tasks: WorkflowTask[];
}

export interface WorkflowListResponse { items: WorkflowDefinition[]; limit: number; offset: number; count: number }
export interface RunListItem { run_id: string; workflow_id: string; workflow_revision: number; status: WorkflowStatus; created_at: string }
export interface RunListResponse { items: RunListItem[]; limit: number; offset: number; count: number }
export interface TaskRun { task_id: string; status: TaskStatus; next_retry_at: string | null; idempotency_key: string; attempt_count: number; latest_attempt_status: string | null; dependencies: string[] }
export interface WorkflowRun { run_id: string; workflow_id: string; workflow_revision: number; status: WorkflowStatus; created_at: string; tasks: TaskRun[] }
export interface TaskAttempt { attempt_number: number; status: TaskStatus; created_at: string | null; started_at: string | null; finished_at: string | null; worker_id: string | null; error_type: string | null; error_message: string | null; attempt_key: string }
export interface TaskAttemptListResponse { items: TaskAttempt[]; count: number }
export interface RunEvent { id: number; event_type: string; workflow_id: string; run_id: string; task_id: string | null; attempt_number: number | null; created_at: string; payload: Record<string, unknown> | null }
