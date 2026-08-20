import type { ReactNode } from 'react'

interface Props {
  label: string
  value: ReactNode
  hint?: string
  tone?: 'default' | 'good' | 'bad' | 'warn'
  loading?: boolean
}

const TONES = {
  default: 'text-ink-50',
  good: 'text-emerald-300',
  bad: 'text-rose-300',
  warn: 'text-amber-300',
} as const

export function StatCard({ label, value, hint, tone = 'default', loading = false }: Props) {
  return (
    <div className="rounded-lg border border-ink-800 bg-ink-900/60 p-4">
      <div className="text-xs uppercase tracking-wide text-ink-400">{label}</div>
      {loading ? (
        <div className="mt-2 h-8 w-20 animate-pulse rounded bg-ink-800" />
      ) : (
        <div className={`mt-1 text-3xl font-semibold tabular-nums ${TONES[tone]}`}>{value}</div>
      )}
      {hint ? <div className="mt-1 text-xs text-ink-400">{hint}</div> : null}
    </div>
  )
}
