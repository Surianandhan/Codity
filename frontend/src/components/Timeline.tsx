import { StatusBadge } from './StatusBadge'
import { formatDuration, formatInstant, shortId } from '../lib/format'
import type { TimelineAttempt, TimelineEvent } from '../lib/timeline'

const DOT: Record<TimelineEvent['tone'], string> = {
  neutral: 'bg-ink-400',
  active: 'bg-violet-400',
  good: 'bg-emerald-400',
  bad: 'bg-rose-400',
  warn: 'bg-amber-400',
}

function EventRow({ event }: { event: TimelineEvent }) {
  return (
    <li className="relative pl-6">
      <span
        className={`absolute left-[3px] top-[7px] h-2 w-2 rounded-full ring-4 ring-ink-950 ${DOT[event.tone]}`}
      />
      <div className="flex flex-wrap items-baseline gap-x-3">
        <span className="text-sm text-ink-100">{event.label}</span>
        <span className="font-mono text-xs text-ink-400">{formatInstant(event.at)}</span>
      </div>
      {event.detail ? <div className="text-xs text-ink-400">{event.detail}</div> : null}
    </li>
  )
}

interface Props {
  attempts: TimelineAttempt[]
  head?: TimelineEvent
}

export function Timeline({ attempts, head }: Props) {
  return (
    <div className="space-y-5">
      {head ? (
        <ol className="relative space-y-2 border-l border-ink-800 pl-1">
          <EventRow event={head} />
        </ol>
      ) : null}

      {attempts.map((attempt) => (
        <div
          key={attempt.executionId}
          className="rounded-lg border border-ink-800 bg-ink-900/40 p-4"
        >
          <div className="mb-3 flex flex-wrap items-center gap-3">
            <span className="text-sm font-medium text-ink-100">Attempt {attempt.attemptNumber}</span>
            <StatusBadge status={attempt.status} />
            <span className="text-xs text-ink-400">
              worker {shortId(attempt.workerId)} · lease epoch {attempt.leaseEpoch}
            </span>
            <span className="ml-auto text-xs text-ink-400">
              {attempt.durationMs !== null ? `ran ${formatDuration(attempt.durationMs)}` : ''}
              {attempt.queueWaitMs !== null
                ? ` · waited ${formatDuration(attempt.queueWaitMs)}`
                : ''}
            </span>
          </div>
          <ol className="relative space-y-3 border-l border-ink-800 pl-1">
            {attempt.events.map((event) => (
              <EventRow key={event.key} event={event} />
            ))}
          </ol>
        </div>
      ))}
    </div>
  )
}
