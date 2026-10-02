"use client";

import {
  Background,
  Controls,
  Handle,
  Position,
  ReactFlow,
  type Edge,
  type Node,
  type NodeProps,
} from "@xyflow/react";
import type { TaskStatus, WorkflowTask } from "@/lib/types";

type TaskNodeData = { label: string; status?: TaskStatus; dependencies: number };
type TaskFlowNode = Node<TaskNodeData, "task">;

const statusColor: Record<string, string> = {
  PENDING: "border-slate-600",
  RETRY_WAITING: "border-violet-500",
  BLOCKED: "border-slate-700",
  READY: "border-sky-500",
  DISPATCHED: "border-indigo-500",
  RUNNING: "border-amber-500",
  SUCCEEDED: "border-emerald-500",
  FAILED: "border-rose-500",
  CANCELLED: "border-slate-500",
  INTERRUPTED: "border-orange-500",
};

function TaskNode({ data }: NodeProps<TaskFlowNode>) {
  return (
    <div
      className={`min-w-36 rounded-md border bg-slate-900 px-3 py-2 shadow-lg ${statusColor[data.status ?? "PENDING"] ?? "border-slate-600"}`}
    >
      <Handle type="target" position={Position.Left} />
      <p className="text-sm font-medium text-slate-100">{data.label}</p>
      <p className="mt-1 text-[11px] text-slate-400">
        {data.status ?? `${data.dependencies} dependencies`}
      </p>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}
const nodeTypes = { task: TaskNode };

function layout(tasks: WorkflowTask[], statuses?: Record<string, TaskStatus>): { nodes: Node<TaskNodeData>[]; edges: Edge[] } {
  const depth = new Map<string, number>();
  const visit = (task: WorkflowTask): number => {
    if (depth.has(task.id)) return depth.get(task.id)!;
    const dependencies = task.depends_on.map((id) => tasks.find((item) => item.id === id));
    const value = dependencies.length
      ? Math.max(...dependencies.map((dependency) => (dependency ? visit(dependency) : 0))) + 1
      : 0;
    depth.set(task.id, value);
    return value;
  };
  tasks.forEach(visit);
  const lanes = new Map<number, number>();
  return { nodes: tasks.map((task) => { const column = depth.get(task.id) ?? 0; const row = lanes.get(column) ?? 0; lanes.set(column, row + 1); return { id: task.id, type: "task", position: { x: column * 250, y: row * 130 }, data: { label: task.name ?? task.id, status: statuses?.[task.id], dependencies: task.depends_on.length } }; }), edges: tasks.flatMap((task) => task.depends_on.map((source) => ({ id: `${source}-${task.id}`, source, target: task.id, animated: statuses?.[task.id] === "RUNNING" }))) };
}

export function Dag({ tasks, statuses }: { tasks: WorkflowTask[]; statuses?: Record<string, TaskStatus> }) {
  const { nodes, edges } = layout(tasks, statuses);
  return (
    <div className="h-[420px] overflow-hidden rounded-lg border border-slate-800 bg-slate-950">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        fitView
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={false}
      >
        <Background color="#1e293b" gap={20} />
        <Controls showInteractive={false} />
      </ReactFlow>
    </div>
  );
}
