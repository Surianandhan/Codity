import { useParams } from 'react-router-dom'
import { DataTable } from '../components/DataTable'
import type { Column } from '../components/DataTable'
import { EmptyState } from '../components/EmptyState'
import { StatCard } from '../components/StatCard'
import { StatusBadge } from '../components/StatusBadge'
import { useSystemStatus, useWorkers } from '../hooks/queries'
import { formatDuration, formatInstant, shortId } from '../lib/format'
import { schedulerTickAgeSeconds } from '../types'
import type { WorkerRow } from '../types'

export function WorkersPage() {
  const { orgId = '' } = useParams()
  const workers = useWorkers(orgId)
  const status = useSystemStatus()

  const tickAge = schedulerTickAgeSeconds(status.data)

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
      // Both the age and the verdict come from the server, which measures every
      // worker against one clock — now() in Postgres — using the same 150s grace
      // the reaper does. Re-deriving either from the browser's clock made the
      // fleet's health depend on the operator's laptop being in NTP agreement
      // with the database, and disagreed with the backend besides.
      key: 'liveness',
      header: 'Heartbeat',
      render: (worker) => (
        <span
          title={formatInstant(worker.last_heartbeat_at)}
          className={worker.is_live ? 'text-emerald-300' : 'text-rose-300'}
        >
          {worker.heartbeat_age_seconds == null
            ? 'never'
            : `${formatDuration(worker.heartbeat_age_seconds * 1000)} ago`}
        </span>
      ),
    },
    {
      key: 'load',
      header: 'Load',
      className: 'tabular-nums',
      render: (worker) =>
        `${worker.inflight}${worker.concurrency ? ` / ${worker.concurrency}` : ''}`,
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
          value={status.data?.fleet.live ?? workers.data?.length ?? 0}
          tone="good"
          loading={status.isLoading}
        />
        <StatCard
          label="Registered"
          value={status.data?.fleet.total ?? workers.data?.length ?? 0}
          loading={status.isLoading}
          hint={status.data ? `${status.data.fleet.draining} draining` : undefined}
        />
        <StatCard
          label="DLQ depth"
          value={status.data?.dlq_depth ?? 0}
          tone="bad"
          loading={status.isLoading}
        />
        <StatCard
          label="Scheduler tick"
          // `== null` on purpose: the field is absent, not null, whenever the
          // status query has not resolved, and `=== null` let undefined through
          // to Math.round — which is where the literal "NaN" came from.
          value={tickAge == null ? '—' : `${Math.round(tickAge)}s`}
          tone={tickAge != null && tickAge > 30 ? 'bad' : 'default'}
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
