/**
 * buildTimeline turns job_executions into one group per attempt.
 *
 * The execution row is created at CLAIM time, so a worker that dies between
 * claim and start leaves a real `lost` row. That is what makes the timeline the
 * evidence for the reliability design: it renders
 * `claimed -> lease expired -> reclaimed by worker-2 -> completed`.
 */
import type { ExecutionStatus, Job, JobExecution } from '../types'

export type EventTone = 'neutral' | 'active' | 'good' | 'bad' | 'warn'

export interface TimelineEvent {
  key: string
  at: string | null
  label: string
  detail?: string
  tone: EventTone
}

export interface TimelineAttempt {
  attemptNumber: number
  executionId: number
  status: ExecutionStatus
  workerId: string | null
  leaseEpoch: number
  durationMs: number | null
  queueWaitMs: number | null
  nextRetryAt: string | null
  errorClass: string | null
  errorMessage: string | null
  events: TimelineEvent[]
}

const OUTCOME: Record<ExecutionStatus, { label: string; tone: EventTone }> = {
  claimed: { label: 'Holding lease', tone: 'active' },
  running: { label: 'Running', tone: 'active' },
  succeeded: { label: 'Succeeded', tone: 'good' },
  failed: { label: 'Failed', tone: 'bad' },
  timed_out: { label: 'Timed out', tone: 'bad' },
  cancelled: { label: 'Cancelled', tone: 'warn' },
  lost: { label: 'Lease expired — reclaimed by the reaper', tone: 'warn' },
}

export function buildTimeline(executions: readonly JobExecution[]): TimelineAttempt[] {
  const ordered = [...executions].sort((a, b) => {
    if (a.attempt_number !== b.attempt_number) return a.attempt_number - b.attempt_number
    return a.id - b.id
  })

  return ordered.map((execution) => {
    const events: TimelineEvent[] = []

    events.push({
      key: `${execution.id}-claimed`,
      at: execution.claimed_at,
      label: `Claimed by ${execution.worker_id ? execution.worker_id.slice(0, 8) : 'a worker'}`,
      detail: `lease epoch ${execution.lease_epoch}`,
      tone: 'neutral',
    })

    if (execution.started_at) {
      events.push({
        key: `${execution.id}-started`,
        at: execution.started_at,
        label: 'Started',
        detail: 'attempt incremented at claimed → running',
        tone: 'active',
      })
    }

    if (execution.finished_at) {
      const outcome = OUTCOME[execution.status] ?? { label: execution.status, tone: 'neutral' as const }
      events.push({
        key: `${execution.id}-finished`,
        at: execution.finished_at,
        label: outcome.label,
        detail: execution.error_class
          ? `${execution.error_class}: ${execution.error_message ?? ''}`.trim()
          : undefined,
        tone: outcome.tone,
      })
    }

    if (execution.next_retry_at) {
      events.push({
        key: `${execution.id}-retry`,
        at: execution.next_retry_at,
        label: 'Retry scheduled (full-jitter backoff)',
        detail: 'status → scheduled; the promoter re-queues it when run_at is reached',
        tone: 'warn',
      })
    }

    return {
      attemptNumber: execution.attempt_number,
      executionId: execution.id,
      status: execution.status,
      workerId: execution.worker_id,
      leaseEpoch: execution.lease_epoch,
      durationMs: execution.duration_ms,
      queueWaitMs: execution.queue_wait_ms,
      nextRetryAt: execution.next_retry_at,
      errorClass: execution.error_class,
      errorMessage: execution.error_message,
      events,
    }
  })
}

/** The first row of the timeline: creation, which predates any execution. */
export function creationEvent(job: Job): TimelineEvent {
  return {
    key: 'created',
    at: job.created_at,
    label: `Enqueued as ${job.kind}`,
    detail: `run_at ${job.run_at}`,
    tone: 'neutral',
  }
}
