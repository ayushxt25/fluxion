"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const links = [{ href: "/", label: "Dashboard" }, { href: "/workflows", label: "Workflows" }, { href: "/runs", label: "Runs" }];

export function Sidebar() {
  const pathname = usePathname();
  return <aside className="border-b border-slate-800 bg-slate-950 px-5 py-4 md:min-h-screen md:w-60 md:border-b-0 md:border-r">
    <Link href="/" className="flex items-center gap-3 text-lg font-semibold text-white"><span className="grid h-8 w-8 place-items-center rounded bg-cyan-400 font-black text-slate-950">F</span>Fluxion</Link>
    <nav className="mt-4 flex gap-2 md:mt-10 md:flex-col">{links.map((link) => <Link key={link.href} href={link.href} className={`rounded px-3 py-2 text-sm ${pathname === link.href || (link.href !== "/" && pathname.startsWith(link.href)) ? "bg-slate-800 text-white" : "text-slate-400 hover:bg-slate-900 hover:text-white"}`}>{link.label}</Link>)}</nav>
    <p className="mt-6 hidden text-xs leading-5 text-slate-500 md:block">PostgreSQL canonical state<br />Redis transport</p>
  </aside>;
}
