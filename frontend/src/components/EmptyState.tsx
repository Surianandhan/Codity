import type { ReactNode } from 'react'
import { ApiError } from '../api/client'

interface Props {
  title: string
  description?: ReactNode
  action?: ReactNode
}

export function EmptyState({ title, description, action }: Props) {
  return (
    <div className="flex flex-col items-center justify-center gap-2 rounded-lg border border-dashed border-ink-700 bg-ink-900/30 px-6 py-12 text-center">
      <div className="text-sm font-medium text-ink-100">{title}</div>
      {description ? <div className="max-w-md text-sm text-ink-400">{description}</div> : null}
      {action}
    </div>
  )
}

export function ErrorState({ error, onRetry }: { error: unknown; onRetry?: () => void }) {
  const message = error instanceof Error ? error.message : 'Something went wrong'
  return (
    <div className="rounded-lg border border-rose-500/30 bg-rose-500/5 px-4 py-6 text-center">
      <div className="text-sm font-medium text-rose-200">{message}</div>
      <div className="mt-1 text-xs text-ink-400">
        Nothing executes without a running worker — check that the API, worker and scheduler
        processes are up.
      </div>
      {onRetry ? (
        <button type="button" className="btn-secondary mt-3" onClick={onRetry}>
          Retry
        </button>
      ) : null}
    </div>
  )
}

/**
 * A rejected write, rendered where the operator clicked.
 *
 * A mutation that fails silently is worse than one that fails loudly: the
 * button appears to do nothing, so the operator clicks it again. Pause, cancel
 * and replay all have legitimate rejections the server explains in words —
 * 409 "job is completed; replay is for terminal jobs only", 409 "queue is
 * paused" — and throwing that message away leaves only the mystery.
 */
export function MutationError({ error, onDismiss }: { error: unknown; onDismiss?: () => void }) {
  if (!error) return null
  const status = error instanceof ApiError ? error.status : null
  const message = error instanceof Error ? error.message : 'The request was rejected'
  return (
    <div
      role="alert"
      className="flex items-start gap-3 rounded-lg border border-rose-500/30 bg-rose-500/5 px-3 py-2 text-sm text-rose-200"
    >
      <span className="flex-1">
        {status ? <span className="font-mono text-xs text-rose-300/70">{status} </span> : null}
        {message}
      </span>
      {onDismiss ? (
        <button
          type="button"
          className="text-xs text-ink-400 underline underline-offset-2 hover:text-ink-200"
          onClick={onDismiss}
        >
          dismiss
        </button>
      ) : null}
    </div>
  )
}

export function TableSkeleton({ rows = 5 }: { rows?: number }) {
  return (
    <div className="space-y-2 p-4">
      {Array.from({ length: rows }).map((_, index) => (
        <div key={index} className="h-8 animate-pulse rounded bg-ink-800/70" />
      ))}
    </div>
  )
}
