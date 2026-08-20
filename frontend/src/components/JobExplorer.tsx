import { useMemo, useState } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { DataTable } from './DataTable'
import type { Column } from './DataTable'
import { EmptyState } from './EmptyState'
import { StatusBadge } from './StatusBadge'
import { useProjectJobs, useQueueJobs } from '../hooks/queries'
import { formatInstant, relativeTo, shortId } from '../lib/format'
import { advance, canRetreat, initialCursor, retreat } from '../lib/cursor'
import type { CursorState } from '../lib/cursor'
import { JOB_STATUSES } from '../types'
import type { Job } from '../types'

/**
 * The saved filters. The dead-letter queue is `?status=dead_letter` on this
 * explorer, not a sixth route — same table, same replay affordance.
 */
const SAVED_FILTERS: { label: string; status: string[] }[] = [
  { label: 'All', status: [] },
  { label: 'In flight', status: ['claimed', 'running'] },
  { label: 'Waiting', status: ['scheduled', 'queued'] },
  { label: 'Failed', status: ['failed'] },
  { label: 'Dead letter', status: ['dead_letter'] },
]

const PAGE_SIZE = 25

interface Props {
  scope: { kind: 'project'; projectId: string } | { kind: 'queue'; queueId: string }
}

export function JobExplorer({ scope }: Props) {
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()
  const [cursor, setCursor] = useState<CursorState>(initialCursor)

  const statuses = useMemo(() => searchParams.getAll('status'), [searchParams])
  const handler = searchParams.get('handler') ?? ''

  const filters = useMemo(
    () => ({
      status: statuses.length > 0 ? statuses : undefined,
      handler: handler || undefined,
      limit: PAGE_SIZE,
      cursor: cursor.current,
    }),
    [statuses, handler, cursor],
  )

  const projectQuery = useProjectJobs(
    scope.kind === 'project' ? scope.projectId : undefined,
    filters,
  )
  const queueQuery = useQueueJobs(scope.kind === 'queue' ? scope.queueId : undefined, filters)
  const active = scope.kind === 'project' ? projectQuery : queueQuery

  const applyStatuses = (next: string[]) => {
    const params = new URLSearchParams(searchParams)
    params.delete('status')
    for (const status of next) params.append('status', status)
    setSearchParams(params, { replace: true })
    setCursor(initialCursor)
  }

  const columns: Column<Job>[] = [
    {
      key: 'id',
      header: 'Job',
      render: (job) => (
        <div>
          <div className="font-mono text-xs text-ink-200">{shortId(job.id, 12)}</div>
          <div className="text-xs text-ink-400">{job.handler}</div>
        </div>
      ),
    },
    { key: 'status', header: 'Status', render: (job) => <StatusBadge status={job.status} /> },
    { key: 'kind', header: 'Kind', render: (job) => <span className="text-ink-300">{job.kind}</span> },
    {
      key: 'priority',
      header: 'Priority',
      className: 'tabular-nums',
      render: (job) => job.priority,
    },
    {
      key: 'attempt',
      header: 'Attempts',
      className: 'tabular-nums',
      render: (job) => `${job.attempt}/${job.max_attempts}`,
    },
    {
      key: 'run_at',
      header: 'Run at',
      render: (job) => (
        <span title={formatInstant(job.run_at)} className="text-ink-300">
          {relativeTo(job.run_at)}
        </span>
      ),
    },
    {
      key: 'created_at',
      header: 'Created',
      render: (job) => <span className="text-ink-400">{formatInstant(job.created_at)}</span>,
    },
  ]

  const jobs = active.data?.jobs
  const nextCursor = active.data?.nextCursor ?? null

  return (
    <section className="space-y-3">
      <div className="flex flex-wrap items-center gap-2">
        {SAVED_FILTERS.map((filter) => {
          const isActive =
            filter.status.length === statuses.length &&
            filter.status.every((status) => statuses.includes(status))
          return (
            <button
              key={filter.label}
              type="button"
              onClick={() => applyStatuses(filter.status)}
              className={`rounded-full px-3 py-1 text-xs ring-1 ring-inset ${
                isActive
                  ? 'bg-ink-100 text-ink-950 ring-ink-100'
                  : 'bg-ink-900 text-ink-300 ring-ink-700 hover:text-ink-100'
              }`}
            >
              {filter.label}
            </button>
          )
        })}

        <select
          className="input ml-auto w-auto text-xs"
          value={statuses.length === 1 ? statuses[0] : ''}
          onChange={(event) => applyStatuses(event.target.value ? [event.target.value] : [])}
        >
          <option value="">any status</option>
          {JOB_STATUSES.map((status) => (
            <option key={status} value={status}>
              {status}
            </option>
          ))}
        </select>
      </div>

      <DataTable
        columns={columns}
        rows={jobs}
        rowKey={(job) => job.id}
        onRowClick={(job) => navigate(`/jobs/${job.id}`)}
        loading={active.isLoading}
        error={active.error}
        onRetry={() => void active.refetch()}
        empty={
          <EmptyState
            title="No jobs match this filter"
            description={
              statuses.includes('dead_letter')
                ? 'Nothing has exhausted its attempt budget. That is the good outcome.'
                : 'Enqueue one with POST /api/v1/queues/{queue_id}/jobs, or run scripts/demo_load.py.'
            }
          />
        }
      />

      <div className="flex items-center justify-between text-xs text-ink-400">
        <span>
          {jobs ? `${jobs.length} shown` : ''}
          {active.isFetching ? ' · refreshing' : ''}
        </span>
        <div className="flex gap-2">
          <button
            type="button"
            className="btn-secondary"
            disabled={!canRetreat(cursor)}
            onClick={() => setCursor(retreat(cursor))}
          >
            Previous
          </button>
          <button
            type="button"
            className="btn-secondary"
            disabled={!nextCursor}
            onClick={() => setCursor(advance(cursor, nextCursor))}
          >
            Next
          </button>
        </div>
      </div>
    </section>
  )
}
