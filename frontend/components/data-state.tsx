export function LoadingState({ label = "Loading Fluxion data…" }: { label?: string }) { return <div className="rounded-lg border border-slate-800 bg-slate-900/60 p-8 text-sm text-slate-400">{label}</div>; }
export function EmptyState({ label }: { label: string }) { return <div className="rounded-lg border border-dashed border-slate-700 p-8 text-sm text-slate-400">{label}</div>; }
export function ErrorState({ message }: { message: string }) { return <div className="rounded-lg border border-rose-900 bg-rose-950/30 p-4 text-sm text-rose-200">{message}</div>; }
