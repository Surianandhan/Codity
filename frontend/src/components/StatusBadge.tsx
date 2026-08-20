import type { ExecutionStatus, JobStatus, WorkerStatus } from '../types'
import { humanize } from '../lib/format'

const TONES: Record<string, string> = {
  scheduled: 'bg-amber-500/10 text-amber-300 ring-amber-500/30',
  queued: 'bg-sky-500/10 text-sky-300 ring-sky-500/30',
  claimed: 'bg-indigo-500/10 text-indigo-300 ring-indigo-500/30',
  running: 'bg-violet-500/10 text-violet-300 ring-violet-500/30',
  completed: 'bg-emerald-500/10 text-emerald-300 ring-emerald-500/30',
  succeeded: 'bg-emerald-500/10 text-emerald-300 ring-emerald-500/30',
  failed: 'bg-rose-500/10 text-rose-300 ring-rose-500/30',
  timed_out: 'bg-rose-500/10 text-rose-300 ring-rose-500/30',
  dead_letter: 'bg-red-600/15 text-red-300 ring-red-500/40',
  cancelled: 'bg-ink-500/10 text-ink-300 ring-ink-500/30',
  lost: 'bg-orange-500/10 text-orange-300 ring-orange-500/30',
  active: 'bg-emerald-500/10 text-emerald-300 ring-emerald-500/30',
  starting: 'bg-sky-500/10 text-sky-300 ring-sky-500/30',
  draining: 'bg-amber-500/10 text-amber-300 ring-amber-500/30',
  stopped: 'bg-ink-500/10 text-ink-300 ring-ink-500/30',
  dead: 'bg-red-600/15 text-red-300 ring-red-500/40',
}

interface Props {
  status: JobStatus | ExecutionStatus | WorkerStatus | string
  title?: string
}

export function StatusBadge({ status, title }: Props) {
  const tone = TONES[status] ?? 'bg-ink-500/10 text-ink-300 ring-ink-500/30'
  return (
    <span
      title={title ?? status}
      className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ring-1 ring-inset whitespace-nowrap ${tone}`}
    >
      {humanize(status)}
    </span>
  )
}
