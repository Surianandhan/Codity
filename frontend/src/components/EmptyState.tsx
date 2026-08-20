import type { ReactNode } from 'react'

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

export function TableSkeleton({ rows = 5 }: { rows?: number }) {
  return (
    <div className="space-y-2 p-4">
      {Array.from({ length: rows }).map((_, index) => (
        <div key={index} className="h-8 animate-pulse rounded bg-ink-800/70" />
      ))}
    </div>
  )
}
