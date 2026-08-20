import { useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { JobExplorer } from '../components/JobExplorer'
import { StatCard } from '../components/StatCard'
import { ErrorState } from '../components/EmptyState'
import { useQueue, useQueuePauseToggle, useQueueStats } from '../hooks/queries'
import { formatInstant } from '../lib/format'

type Tab = 'jobs' | 'config'

function ConfigRow({ label, value, note }: { label: string; value: string; note?: string }) {
  return (
    <div className="flex flex-wrap items-baseline justify-between gap-2 border-b border-ink-800/60 py-2 last:border-0">
      <div>
        <div className="text-sm text-ink-100">{label}</div>
        {note ? <div className="text-xs text-ink-400">{note}</div> : null}
      </div>
      <div className="font-mono text-sm text-ink-200">{value}</div>
    </div>
  )
}

export function QueuePage() {
  const { queueId = '' } = useParams()
  const [tab, setTab] = useState<Tab>('jobs')
  const queue = useQueue(queueId)
  const stats = useQueueStats(queueId)
  const toggle = useQueuePauseToggle(queue.data?.project_id)

  if (queue.error) return <ErrorState error={queue.error} onRetry={() => void queue.refetch()} />

  const inFlight = (stats.data?.claimed ?? 0) + (stats.data?.running ?? 0)

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center gap-3">
        <div>
          <h1 className="text-lg font-semibold text-ink-50">{queue.data?.name ?? 'Queue'}</h1>
          {queue.data ? (
            <Link
              to={`/projects/${queue.data.project_id}`}
              className="text-xs text-ink-400 underline underline-offset-2 hover:text-ink-200"
            >
              back to project
            </Link>
          ) : null}
        </div>

        {queue.data?.is_paused ? (
          <span className="rounded-full bg-amber-500/10 px-3 py-1 text-xs text-amber-300 ring-1 ring-inset ring-amber-500/30">
            paused{inFlight > 0 ? ` — ${inFlight} still running` : ''}
          </span>
        ) : null}

        <button
          type="button"
          className="btn-secondary ml-auto"
          disabled={!queue.data}
          onClick={() =>
            queue.data && toggle.mutate({ queueId: queue.data.id, pause: !queue.data.is_paused })
          }
        >
          {queue.data?.is_paused ? 'Resume admission' : 'Pause admission'}
        </button>
      </div>

      <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        <StatCard label="Scheduled" value={stats.data?.scheduled ?? 0} loading={stats.isLoading} />
        <StatCard label="Queued" value={stats.data?.queued ?? 0} loading={stats.isLoading} />
        <StatCard
          label="In flight"
          value={inFlight}
          loading={stats.isLoading}
          hint={queue.data ? `cap ${queue.data.max_concurrency}` : undefined}
        />
        <StatCard
          label="Dead letter"
          value={stats.data?.dead_letter ?? 0}
          tone="bad"
          loading={stats.isLoading}
        />
      </div>

      <div className="flex gap-1 border-b border-ink-800 text-sm">
        {(['jobs', 'config'] as Tab[]).map((name) => (
          <button
            key={name}
            type="button"
            onClick={() => setTab(name)}
            className={`-mb-px border-b-2 px-3 py-2 capitalize ${
              tab === name
                ? 'border-ink-100 text-ink-50'
                : 'border-transparent text-ink-400 hover:text-ink-200'
            }`}
          >
            {name}
          </button>
        ))}
      </div>

      {tab === 'jobs' ? (
        <JobExplorer scope={{ kind: 'queue', queueId }} />
      ) : (
        <div className="rounded-lg border border-ink-800 bg-ink-900/40 p-4">
          {queue.data ? (
            <>
              <ConfigRow
                label="max_concurrency"
                value={String(queue.data.max_concurrency)}
                note="Exact cap — the claim locks the queue row FOR NO KEY UPDATE"
              />
              <ConfigRow
                label="priority"
                value={String(queue.data.priority)}
                note="Polling weight across queues; higher runs first"
              />
              <ConfigRow
                label="default_priority"
                value={String(queue.data.default_priority)}
                note="Priority given to jobs created on this queue"
              />
              <ConfigRow
                label="visibility_timeout_sec"
                value={String(queue.data.visibility_timeout_sec)}
                note="Snapshotted onto each job as lease_seconds at enqueue"
              />
              <ConfigRow
                label="default_timeout_ms"
                value={String(queue.data.default_timeout_ms)}
                note="Must be < visibility_timeout_sec × 1000 (ck_queues_timeout_lt_lease)"
              />
              <ConfigRow label="is_paused" value={String(queue.data.is_paused)} />
              <ConfigRow label="paused_at" value={formatInstant(queue.data.paused_at)} />
              <ConfigRow label="created_at" value={formatInstant(queue.data.created_at)} />
            </>
          ) : (
            <div className="h-32 animate-pulse rounded bg-ink-800/60" />
          )}
        </div>
      )}
    </div>
  )
}
