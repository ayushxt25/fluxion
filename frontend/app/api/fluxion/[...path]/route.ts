import { NextRequest } from "next/server";

export const dynamic = "force-dynamic";

type RouteContext = { params: Promise<{ path: string[] }> };

export async function GET(request: NextRequest, context: RouteContext) {
  const apiUrl = process.env.FLUXION_API_URL;
  if (!apiUrl) {
    return Response.json(
      { error: { code: "frontend_configuration", message: "FLUXION_API_URL is not configured." } },
      { status: 500 },
    );
  }

  const { path } = await context.params;
  const target = new URL(`${apiUrl.replace(/\/$/, "")}/${path.join("/")}`);
  target.search = request.nextUrl.search;
  const headers = new Headers({ Accept: request.headers.get("accept") ?? "application/json" });
  const token = process.env.FLUXION_API_TOKEN;
  if (token) headers.set("Authorization", `Bearer ${token}`);

  let upstream: Response;
  try {
    upstream = await fetch(target, { headers, cache: "no-store" });
  } catch {
    return Response.json(
      {
        error: {
          code: "fluxion_unavailable",
          message: "The Fluxion API is unavailable.",
        },
      },
      { status: 502 },
    );
  }
  const responseHeaders = new Headers();
  const contentType = upstream.headers.get("content-type");
  if (contentType) responseHeaders.set("content-type", contentType);
  responseHeaders.set("cache-control", "no-store");
  return new Response(upstream.body, { status: upstream.status, headers: responseHeaders });
}
