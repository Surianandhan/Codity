import { useState } from 'react'
import { Link, useLocation, useParams } from 'react-router-dom'
import { JobExplorer } from '../components/JobExplorer'
import { StatCard } from '../components/StatCard'
import { ErrorState, MutationError } from '../components/EmptyState'
import { useQueuePauseToggle, useQueueStats } from '../hooks/queries'
import { formatDuration, shortId } from '../lib/format'

type Tab = 'jobs' | 'config'

/**
 * `GET /queues/{id}` does not exist, so the queue's project is not discoverable
 * from the queue id alone. Every in-app link into this screen already knows it
 * and hands it over in router state; on a cold deep-link it is simply absent,
 * and the affordances that need it are omitted rather than faked.
 */
function useOriginProjectId(): string | undefined {
  const state = useLocation().state as { projectId?: unknown } | null
  return typeof state?.projectId === 'string' ? state.projectId : undefined
}

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
  const projectId = useOriginProjectId()
  const stats = useQueueStats(queueId)
  const toggle = useQueuePauseToggle(projectId)

  if (stats.error) return <ErrorState error={stats.error} onRetry={() => void stats.refetch()} />

  const queue = stats.data
  const depth = queue?.depth

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center gap-3">
        <div>
          <h1 className="text-lg font-semibold text-ink-50">{queue?.name ?? 'Queue'}</h1>
          {projectId ? (
            <Link
              to={`/projects/${projectId}`}
              className="text-xs text-ink-400 underline underline-offset-2 hover:text-ink-200"
            >
              back to project
            </Link>
          ) : (
            <div className="font-mono text-xs text-ink-400">{shortId(queueId, 18)}</div>
          )}
        </div>

        {queue?.is_paused ? (
          <span className="rounded-full bg-amber-500/10 px-3 py-1 text-xs text-amber-300 ring-1 ring-inset ring-amber-500/30">
            paused{queue.inflight > 0 ? ` — ${queue.inflight} still running` : ''}
          </span>
        ) : null}

        <button
          type="button"
          className="btn-secondary ml-auto"
          disabled={!queue || toggle.isPending}
          onClick={() => queue && toggle.mutate({ queueId: queue.queue_id, pause: !queue.is_paused })}
        >
          {queue?.is_paused ? 'Resume admission' : 'Pause admission'}
        </button>
      </div>

      <MutationError error={toggle.error} onDismiss={() => toggle.reset()} />

      <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        <StatCard label="Scheduled" value={depth?.scheduled ?? 0} loading={stats.isLoading} />
        <StatCard
          label="Queued"
          value={depth?.queued ?? 0}
          loading={stats.isLoading}
          hint={
            queue?.oldest_queued_age_seconds == null
              ? undefined
              : `oldest waiting ${formatDuration(queue.oldest_queued_age_seconds * 1000)}`
          }
        />
        <StatCard
          label="In flight"
          value={queue?.inflight ?? 0}
          loading={stats.isLoading}
          hint={queue ? `cap ${queue.max_concurrency} · headroom ${queue.headroom}` : undefined}
        />
        <StatCard
          label="Dead lettered"
          value={queue?.dead_lettered ?? 0}
          tone="bad"
          loading={stats.isLoading}
          hint="attempt budget exhausted, last hour"
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
          {queue ? (
            <>
              <ConfigRow label="queue_id" value={queue.queue_id} />
              <ConfigRow
                label="max_concurrency"
                value={String(queue.max_concurrency)}
                note="Exact cap — the claim locks the queue row FOR NO KEY UPDATE"
              />
              <ConfigRow
                label="headroom"
                value={String(queue.headroom)}
                note="What a claim would be allowed to take right now: max_concurrency − inflight"
              />
              <ConfigRow
                label="inflight"
                value={String(queue.inflight)}
                note="Claimed + running, counted by the server against the same statuses the claim uses"
              />
              <ConfigRow
                label="is_paused"
                value={String(queue.is_paused)}
                note="Pause blocks admission only; in-flight work finishes"
              />
              <ConfigRow
                label="oldest_queued_age_seconds"
                value={
                  queue.oldest_queued_age_seconds == null
                    ? '—'
                    : queue.oldest_queued_age_seconds.toFixed(1)
                }
                note="Age of the longest-waiting queued job, measured by now() in Postgres"
              />
              <ConfigRow
                label="success_rate"
                value={
                  queue.completed + queue.failed + queue.dead_lettered === 0
                    ? '—'
                    : `${(
                        (queue.completed /
                          (queue.completed + queue.failed + queue.dead_lettered)) *
                        100
                      ).toFixed(1)}%`
                }
                note={`Over the ${queue.window} rollup window`}
              />
              <ConfigRow
                label="mean_duration_ms"
                value={queue.mean_duration_ms == null ? '—' : queue.mean_duration_ms.toFixed(0)}
                note="Mean, not p50 — the rollup stores sums"
              />
            </>
          ) : (
            <div className="h-32 animate-pulse rounded bg-ink-800/60" />
          )}
        </div>
      )}
    </div>
  )
}
