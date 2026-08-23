import { useNavigate, useParams } from 'react-router-dom'
import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { DataTable } from '../components/DataTable'
import type { Column } from '../components/DataTable'
import { EmptyState, MutationError } from '../components/EmptyState'
import { JobExplorer } from '../components/JobExplorer'
import { StatCard } from '../components/StatCard'
import { useMetricsSummary, useQueuePauseToggle, useQueues, useThroughput } from '../hooks/queries'
import { formatClock } from '../lib/format'
import { inflightOf } from '../types'
import type { Queue } from '../types'

export function ProjectPage() {
  const { projectId = '' } = useParams()
  const navigate = useNavigate()
  const queues = useQueues(projectId)
  const summary = useMetricsSummary(projectId)
  const throughput = useThroughput(projectId)
  const toggle = useQueuePauseToggle(projectId)

  // The queue screen cannot recover its project from the queue id — there is no
  // GET /queues/{id} — so hand it over on the way in.
  const openQueue = (queue: Queue) =>
    navigate(`/queues/${queue.id}`, { state: { projectId } })

  const columns: Column<Queue>[] = [
    {
      key: 'name',
      header: 'Queue',
      render: (queue) => (
        <div>
          <div className="text-ink-100">{queue.name}</div>
          <div className="text-xs text-ink-400">
            cap {queue.max_concurrency} · lease {queue.visibility_timeout_sec}s · weight{' '}
            {queue.priority}
          </div>
        </div>
      ),
    },
    {
      key: 'state',
      header: 'Admission',
      render: (queue) =>
        queue.is_paused ? (
          <span className="text-amber-300">paused</span>
        ) : (
          <span className="text-emerald-300">accepting</span>
        ),
    },
    {
      key: 'default_priority',
      header: 'Default priority',
      className: 'tabular-nums',
      render: (queue) => queue.default_priority,
    },
    {
      key: 'timeout',
      header: 'Job timeout',
      className: 'tabular-nums',
      render: (queue) => `${queue.default_timeout_ms} ms`,
    },
    {
      key: 'actions',
      header: '',
      className: 'text-right',
      render: (queue) => (
        <div className="flex justify-end gap-2">
          <button
            type="button"
            className="btn-secondary"
            onClick={(event) => {
              event.stopPropagation()
              // Pause blocks admission only; in-flight work finishes.
              toggle.mutate({ queueId: queue.id, pause: !queue.is_paused })
            }}
          >
            {queue.is_paused ? 'Resume' : 'Pause'}
          </button>
          <button
            type="button"
            className="btn-secondary"
            onClick={(event) => {
              event.stopPropagation()
              openQueue(queue)
            }}
          >
            Open
          </button>
        </div>
      ),
    },
  ]

  const chartData = (throughput.data ?? []).map((point) => ({
    ...point,
    label: formatClock(point.bucket_start),
  }))

  // Depth is a nested map of LIVE statuses only, keyed exactly as the partial
  // index that produces it; the windowed counts sit flat alongside it.
  const depth = summary.data?.depth

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 gap-3 lg:grid-cols-5">
        <StatCard
          label="Completed"
          value={summary.data?.completed ?? 0}
          tone="good"
          loading={summary.isLoading}
          hint="last hour"
        />
        <StatCard
          label="Running"
          value={inflightOf(depth)}
          loading={summary.isLoading}
          hint="claimed + running"
        />
        <StatCard label="Queued" value={depth?.queued ?? 0} loading={summary.isLoading} />
        <StatCard
          label="Failed"
          value={summary.data?.failed ?? 0}
          tone="warn"
          loading={summary.isLoading}
          hint="last hour"
        />
        <StatCard
          label="Dead letter"
          value={summary.data?.dlq_open ?? 0}
          tone="bad"
          loading={summary.isLoading}
          hint="unresolved DLQ entries"
        />
      </div>

      <section className="rounded-lg border border-ink-800 bg-ink-900/40 p-4">
        <div className="mb-3 flex items-baseline justify-between">
          <h2 className="text-sm font-medium text-ink-100">Throughput — last hour, per minute</h2>
          <span className="text-xs text-ink-400">refreshes every 30s</span>
        </div>
        {chartData.length === 0 ? (
          <EmptyState
            title="No throughput yet"
            description="Rollups appear once jobs start finishing. Run scripts/demo_load.py to generate load."
          />
        ) : (
          <div className="h-56">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={chartData}>
                <CartesianGrid stroke="#394051" strokeDasharray="3 3" vertical={false} />
                <XAxis dataKey="label" stroke="#8591aa" fontSize={11} tickLine={false} />
                <YAxis stroke="#8591aa" fontSize={11} tickLine={false} allowDecimals={false} />
                <Tooltip
                  contentStyle={{
                    background: '#141824',
                    border: '1px solid #394051',
                    borderRadius: 8,
                    fontSize: 12,
                  }}
                />
                <Area
                  type="monotone"
                  dataKey="completed"
                  stackId="1"
                  stroke="#34d399"
                  fill="#34d39955"
                />
                <Area type="monotone" dataKey="failed" stackId="1" stroke="#fb7185" fill="#fb718555" />
                <Area
                  type="monotone"
                  dataKey="dead_lettered"
                  stackId="1"
                  stroke="#f87171"
                  fill="#f8717155"
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        )}
      </section>

      <section className="space-y-3">
        <h2 className="text-sm font-medium text-ink-100">Queues</h2>
        <MutationError error={toggle.error} onDismiss={() => toggle.reset()} />
        <DataTable
          columns={columns}
          rows={queues.data}
          rowKey={(queue) => queue.id}
          onRowClick={openQueue}
          loading={queues.isLoading}
          error={queues.error}
          onRetry={() => void queues.refetch()}
          empty={
            <EmptyState
              title="No queues in this project"
              description="Create one with POST /api/v1/projects/{project_id}/queues, or run scripts/seed.py."
            />
          }
        />
      </section>

      <section className="space-y-3">
        <h2 className="text-sm font-medium text-ink-100">Jobs</h2>
        <JobExplorer scope={{ kind: 'project', projectId }} />
      </section>
    </div>
  )
}
