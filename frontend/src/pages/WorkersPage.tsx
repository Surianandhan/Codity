import { useParams } from 'react-router-dom'
import { DataTable } from '../components/DataTable'
import type { Column } from '../components/DataTable'
import { EmptyState } from '../components/EmptyState'
import { StatCard } from '../components/StatCard'
import { StatusBadge } from '../components/StatusBadge'
import { useSystemStatus, useWorkers } from '../hooks/queries'
import { formatInstant, relativeTo, shortId } from '../lib/format'
import type { WorkerRow } from '../types'

/** A worker whose heartbeat is older than this is presumed lost by the reaper. */
const STALE_HEARTBEAT_MS = 60_000

function heartbeatAgeMs(worker: WorkerRow): number | null {
  if (!worker.last_heartbeat_at) return null
  const then = new Date(worker.last_heartbeat_at).getTime()
  return Number.isNaN(then) ? null : Date.now() - then
}

export function WorkersPage() {
  const { orgId = '' } = useParams()
  const workers = useWorkers(orgId)
  const status = useSystemStatus()

  const columns: Column<WorkerRow>[] = [
    {
      key: 'name',
      header: 'Worker',
      render: (worker) => (
        <div>
          <div className="text-ink-100">{worker.name}</div>
          <div className="font-mono text-xs text-ink-400">{shortId(worker.id, 12)}</div>
        </div>
      ),
    },
    { key: 'status', header: 'Status', render: (worker) => <StatusBadge status={worker.status} /> },
    {
      key: 'liveness',
      header: 'Heartbeat',
      render: (worker) => {
        const age = heartbeatAgeMs(worker)
        const stale = age === null || age > STALE_HEARTBEAT_MS
        return (
          <span
            title={formatInstant(worker.last_heartbeat_at)}
            className={stale ? 'text-rose-300' : 'text-emerald-300'}
          >
            {relativeTo(worker.last_heartbeat_at)}
          </span>
        )
      },
    },
    {
      key: 'load',
      header: 'Load',
      className: 'tabular-nums',
      render: (worker) =>
        worker.active_jobs === null || worker.active_jobs === undefined
          ? '—'
          : `${worker.active_jobs}${worker.concurrency ? ` / ${worker.concurrency}` : ''}`,
    },
    {
      key: 'drain',
      header: 'Drain',
      render: (worker) =>
        worker.drain_requested ? <span className="text-amber-300">requested</span> : '—',
    },
    {
      key: 'started',
      header: 'Started',
      render: (worker) => <span className="text-ink-400">{formatInstant(worker.started_at)}</span>,
    },
  ]

  return (
    <div className="space-y-6">
      <h1 className="text-lg font-semibold text-ink-50">Worker fleet</h1>

      <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        <StatCard
          label="Live workers"
          value={status.data?.workers_live ?? workers.data?.length ?? 0}
          tone="good"
          loading={status.isLoading}
        />
        <StatCard
          label="Registered"
          value={status.data?.workers_total ?? workers.data?.length ?? 0}
          loading={status.isLoading}
        />
        <StatCard
          label="DLQ depth"
          value={status.data?.dlq_depth ?? 0}
          tone="bad"
          loading={status.isLoading}
        />
        <StatCard
          label="Scheduler tick"
          value={
            status.data?.scheduler_last_tick_age_seconds === null ||
            status.data?.scheduler_last_tick_age_seconds === undefined
              ? '—'
              : `${Math.round(status.data.scheduler_last_tick_age_seconds)}s`
          }
          tone={
            (status.data?.scheduler_last_tick_age_seconds ?? 0) > 30 ? 'bad' : 'default'
          }
          hint="a stale tick stalls every delayed job and every backoff retry"
          loading={status.isLoading}
        />
      </div>

      <DataTable
        columns={columns}
        rows={workers.data}
        rowKey={(worker) => worker.id}
        loading={workers.isLoading}
        error={workers.error}
        onRetry={() => void workers.refetch()}
        empty={
          <EmptyState
            title="No workers registered"
            description="Start one with: uv run python -m app.worker.main --name worker-1 --concurrency 4"
          />
        }
      />
    </div>
  )
}
