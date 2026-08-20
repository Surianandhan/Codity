import { useMemo } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { EmptyState, ErrorState } from '../components/EmptyState'
import { StatusBadge } from '../components/StatusBadge'
import { Timeline } from '../components/Timeline'
import { useJob, useJobActions, useJobExecutions, useJobLogs } from '../hooks/queries'
import { formatDuration, formatInstant, prettyJson, shortId } from '../lib/format'
import { buildTimeline, creationEvent } from '../lib/timeline'
import { isTerminal } from '../types'

function Field({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <div className="text-xs uppercase tracking-wide text-ink-400">{label}</div>
      <div className="font-mono text-sm text-ink-100">{value}</div>
    </div>
  )
}

export function JobPage() {
  const { jobId = '' } = useParams()
  const navigate = useNavigate()
  const job = useJob(jobId)
  const live = !isTerminal(job.data?.status)
  const executions = useJobExecutions(jobId, live)
  const logs = useJobLogs(jobId, live)
  const { replay, cancel } = useJobActions(jobId)

  const attempts = useMemo(() => buildTimeline(executions.data ?? []), [executions.data])

  if (job.error) return <ErrorState error={job.error} onRetry={() => void job.refetch()} />
  if (!job.data) return <div className="h-64 animate-pulse rounded bg-ink-900/60" />

  const data = job.data
  const terminal = isTerminal(data.status)

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-start gap-3">
        <div>
          <div className="flex items-center gap-3">
            <h1 className="font-mono text-lg text-ink-50">{shortId(data.id, 18)}</h1>
            <StatusBadge status={data.status} />
          </div>
          <div className="mt-1 text-xs text-ink-400">
            {data.handler} · {data.kind} ·{' '}
            <Link
              to={`/queues/${data.queue_id}`}
              className="underline underline-offset-2 hover:text-ink-200"
            >
              queue {shortId(data.queue_id)}
            </Link>
          </div>
        </div>

        <div className="ml-auto flex gap-2">
          {!terminal ? (
            <button
              type="button"
              className="btn-secondary"
              disabled={cancel.isPending}
              onClick={() => cancel.mutate(data.id)}
            >
              Cancel
            </button>
          ) : null}
          <button
            type="button"
            className="btn-primary"
            disabled={replay.isPending}
            title="Replay inserts a new job with replay_of_job_id set; the terminal job is never resurrected."
            onClick={() =>
              replay.mutate(data.id, {
                onSuccess: (created) => navigate(`/jobs/${created.id}`),
              })
            }
          >
            {replay.isPending ? 'Replaying…' : 'Replay'}
          </button>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-4 rounded-lg border border-ink-800 bg-ink-900/40 p-4 md:grid-cols-4">
        <Field label="Attempts" value={`${data.attempt} / ${data.max_attempts}`} />
        <Field label="Priority" value={String(data.priority)} />
        <Field label="Run at" value={formatInstant(data.run_at)} />
        <Field label="Lease epoch" value={String(data.lease_epoch)} />
        <Field label="Worker" value={shortId(data.worker_id)} />
        <Field label="Claimed" value={formatInstant(data.claimed_at)} />
        <Field label="Started" value={formatInstant(data.started_at)} />
        <Field label="Finished" value={formatInstant(data.finished_at)} />
      </div>

      {data.last_error_class ? (
        <div className="rounded-lg border border-rose-500/30 bg-rose-500/5 p-4">
          <div className="text-sm font-medium text-rose-200">{data.last_error_class}</div>
          <div className="mt-1 whitespace-pre-wrap text-xs text-ink-300">
            {data.last_error_message}
          </div>
        </div>
      ) : null}

      <section className="space-y-3">
        <div className="flex items-baseline justify-between">
          <h2 className="text-sm font-medium text-ink-100">Lifecycle</h2>
          <span className="text-xs text-ink-400">
            {live ? 'polling every 2s' : 'terminal — polling stopped'}
          </span>
        </div>
        {attempts.length === 0 ? (
          <EmptyState
            title="No attempts yet"
            description="An execution row is created the moment a worker claims the job. Nothing executes without a running worker."
          />
        ) : (
          <Timeline attempts={attempts} head={creationEvent(data)} />
        )}
      </section>

      <div className="grid gap-4 lg:grid-cols-2">
        <section className="space-y-2">
          <h2 className="text-sm font-medium text-ink-100">Payload</h2>
          <pre className="max-h-72 overflow-auto rounded-lg border border-ink-800 bg-ink-900/60 p-3 font-mono text-xs text-ink-200">
            {prettyJson(data.payload)}
          </pre>
        </section>

        <section className="space-y-2">
          <h2 className="text-sm font-medium text-ink-100">Logs</h2>
          <div className="max-h-72 overflow-auto rounded-lg border border-ink-800 bg-ink-900/60 p-3 font-mono text-xs">
            {logs.isLoading ? (
              <div className="h-16 animate-pulse rounded bg-ink-800/60" />
            ) : (logs.data ?? []).length === 0 ? (
              <div className="text-ink-400">No log lines.</div>
            ) : (
              (logs.data ?? []).map((line) => (
                <div key={line.id} className="whitespace-pre-wrap text-ink-200">
                  <span className="text-ink-500">{formatInstant(line.logged_at)} </span>
                  <span className="text-ink-400">[{line.level}] </span>
                  {line.message}
                </div>
              ))
            )}
          </div>
        </section>
      </div>

      <section className="space-y-2">
        <h2 className="text-sm font-medium text-ink-100">Executions</h2>
        <div className="overflow-x-auto rounded-lg border border-ink-800 bg-ink-900/40">
          <table className="min-w-full text-sm">
            <thead>
              <tr className="border-b border-ink-800 text-left text-xs uppercase tracking-wide text-ink-400">
                <th className="px-3 py-2 font-medium">Attempt</th>
                <th className="px-3 py-2 font-medium">Status</th>
                <th className="px-3 py-2 font-medium">Worker</th>
                <th className="px-3 py-2 font-medium">Epoch</th>
                <th className="px-3 py-2 font-medium">Duration</th>
                <th className="px-3 py-2 font-medium">Next retry</th>
              </tr>
            </thead>
            <tbody>
              {(executions.data ?? []).map((execution) => (
                <tr key={execution.id} className="border-b border-ink-800/60 last:border-0">
                  <td className="px-3 py-2 tabular-nums">{execution.attempt_number}</td>
                  <td className="px-3 py-2">
                    <StatusBadge status={execution.status} />
                  </td>
                  <td className="px-3 py-2 font-mono text-xs">{shortId(execution.worker_id)}</td>
                  <td className="px-3 py-2 tabular-nums">{execution.lease_epoch}</td>
                  <td className="px-3 py-2">{formatDuration(execution.duration_ms)}</td>
                  <td className="px-3 py-2 text-ink-400">{formatInstant(execution.next_retry_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  )
}
