import "@xyflow/react/dist/style.css";
import "./globals.css";
import type { Metadata } from "next";
import { Sidebar } from "@/components/sidebar";

export const metadata: Metadata = { title: "Fluxion Dashboard", description: "Fluxion workflow observability dashboard" };

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) { return <html lang="en"><body><div className="min-h-screen bg-slate-950 md:flex"><Sidebar /><main className="min-w-0 flex-1 p-5 md:p-10">{children}</main></div></body></html>; }
