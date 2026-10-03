import { NextRequest } from "next/server";

type DemoId = "document" | "etl";

const documentInput = {
  document: {
    id: "sample-document-001",
    title: "Fluxion Demonstration Document",
    author: "Fluxion",
    tags: ["demo", "workflow"],
    body: "Fluxion executes durable workflows through a real distributed stack.",
  },
};

const etlInput = {
  dataset: [
    { id: 1, value: 3 },
    { id: 2, value: 5 },
    { id: 3, value: 8 },
    { id: 4, value: 13 },
  ],
};

function parameter(source: "workflow_input" | "dependency_result", path: string[], taskId?: string) {
  return taskId ? { source, task_id: taskId, path } : { source, path };
}

function workflow(demo: DemoId, id: string) {
  if (demo === "document") {
    return {
      id,
      name: "Document Processing Pipeline",
      tasks: [
        { id: "demo.document.ingest", name: "Ingest Document", parameters: { document: parameter("workflow_input", ["document"]) } },
        { id: "demo.document.validate", name: "Validate Document", depends_on: ["demo.document.ingest"], parameters: { document: parameter("dependency_result", ["document"], "demo.document.ingest") } },
        { id: "demo.document.extract_text", name: "Extract Text", depends_on: ["demo.document.validate"], parameters: { document: parameter("dependency_result", ["document"], "demo.document.validate") } },
        { id: "demo.document.extract_metadata", name: "Extract Metadata", depends_on: ["demo.document.validate"], parameters: { document: parameter("dependency_result", ["document"], "demo.document.validate") } },
        { id: "demo.document.aggregate", name: "Aggregate", depends_on: ["demo.document.extract_text", "demo.document.extract_metadata"], parameters: { text: parameter("dependency_result", ["text"], "demo.document.extract_text"), metadata: parameter("dependency_result", [], "demo.document.extract_metadata") } },
        { id: "demo.document.summarize", name: "Generate Summary", depends_on: ["demo.document.aggregate"], parameters: { text: parameter("dependency_result", ["text"], "demo.document.aggregate"), title: parameter("dependency_result", ["metadata", "title"], "demo.document.aggregate") } },
        { id: "demo.document.persist", name: "Persist Result", depends_on: ["demo.document.summarize"], parameters: { summary: parameter("dependency_result", [], "demo.document.summarize") } },
      ],
    };
  }
  return {
    id,
    name: "Resilient ETL Pipeline",
    tasks: [
      { id: "demo.etl.fetch", name: "Fetch Dataset", parameters: { dataset: parameter("workflow_input", ["dataset"]) } },
      { id: "demo.etl.validate", name: "Validate Schema", depends_on: ["demo.etl.fetch"], retry_policy: { max_attempts: 2, initial_backoff_seconds: 1, backoff_multiplier: 2 }, parameters: { records: parameter("dependency_result", ["records"], "demo.etl.fetch") } },
      { id: "demo.etl.clean", name: "Clean Records", depends_on: ["demo.etl.validate"], parameters: { records: parameter("dependency_result", ["records"], "demo.etl.validate") } },
      { id: "demo.etl.features", name: "Compute Features", depends_on: ["demo.etl.validate"], parameters: { records: parameter("dependency_result", ["records"], "demo.etl.validate") } },
      { id: "demo.etl.merge", name: "Merge", depends_on: ["demo.etl.clean", "demo.etl.features"], parameters: { records: parameter("dependency_result", ["records"], "demo.etl.clean"), features: parameter("dependency_result", ["features"], "demo.etl.features") } },
      { id: "demo.etl.persist", name: "Persist Dataset", depends_on: ["demo.etl.merge"], parameters: { merged: parameter("dependency_result", [], "demo.etl.merge") } },
    ],
  };
}

async function apiRequest(path: string, body: object) {
  const apiUrl = process.env.FLUXION_API_URL;
  if (!apiUrl) throw new Error("Fluxion API is not configured.");
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (process.env.FLUXION_API_TOKEN) headers.Authorization = `Bearer ${process.env.FLUXION_API_TOKEN}`;
  const response = await fetch(`${apiUrl.replace(/\/$/, "")}${path}`, { method: "POST", headers, body: JSON.stringify(body), cache: "no-store" });
  if (!response.ok) throw new Error(`Fluxion API request failed (${response.status}).`);
  return response.json() as Promise<{ run_id?: string }>;
}

export async function POST(_: NextRequest, context: { params: Promise<{ demo: string }> }) {
  const { demo } = await context.params;
  if (demo !== "document" && demo !== "etl") return Response.json({ error: "Unknown demo." }, { status: 404 });
  const id = `playground-${demo}-${crypto.randomUUID()}`;
  try {
    await apiRequest("/api/v1/workflows", workflow(demo, id));
    const run = await apiRequest(`/api/v1/workflows/${encodeURIComponent(id)}/runs`, { run_id: `${id}-run`, input: demo === "document" ? documentInput : etlInput });
    return Response.json({ workflow_id: id, run_id: run.run_id });
  } catch {
    return Response.json({ error: "Unable to start the Fluxion demo." }, { status: 502 });
  }
}
