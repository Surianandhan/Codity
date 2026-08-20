/**
 * Canonical vocabulary, mirrored from backend/app/domain/enums.py and
 * docs/SCHEMA_NAMES.md. When `openapi.json` is available these become aliases
 * over the generated `api/schema.d.ts` (`npm run check:api`); until then they
 * are the single hand-maintained boundary, and nothing else in the app is
 * allowed to invent a status string.
 */

export const JOB_STATUSES = [
  'scheduled',
  'queued',
  'claimed',
  'running',
  'completed',
  'failed',
  'dead_letter',
  'cancelled',
] as const
export type JobStatus = (typeof JOB_STATUSES)[number]

export const EXECUTION_STATUSES = [
  'claimed',
  'running',
  'succeeded',
  'failed',
  'timed_out',
  'cancelled',
  'lost',
] as const
export type ExecutionStatus = (typeof EXECUTION_STATUSES)[number]

export const JOB_KINDS = ['immediate', 'delayed', 'scheduled', 'recurring', 'batch'] as const
export type JobKind = (typeof JOB_KINDS)[number]

export type WorkerStatus = 'starting' | 'active' | 'draining' | 'stopped' | 'dead'
export type BackoffStrategy = 'fixed' | 'linear' | 'exponential'

/** Terminal statuses have zero out-edges; the job-detail poll stops here. */
export const TERMINAL_STATUSES: readonly JobStatus[] = [
  'completed',
  'failed',
  'dead_letter',
  'cancelled',
]

export const LIVE_STATUSES: readonly JobStatus[] = ['scheduled', 'queued', 'claimed', 'running']

export function isTerminal(status: JobStatus | undefined): boolean {
  return status !== undefined && TERMINAL_STATUSES.includes(status)
}

// --- wire models ---

export interface Me {
  id: string
  email: string
  full_name: string | null
  organization_id: string
  role: string
}

export interface Tokens {
  access_token: string
  refresh_token: string
  token_type: string
}

export interface Project {
  id: string
  organization_id: string
  name: string
  slug: string
  created_at: string
}

export interface Queue {
  id: string
  project_id: string
  name: string
  max_concurrency: number
  priority: number
  default_priority: number
  visibility_timeout_sec: number
  default_timeout_ms: number
  dlq_enabled?: boolean
  /** Always written together with paused_at; ck_queues_pause rejects a mismatch. */
  is_paused: boolean
  paused_at: string | null
  created_at: string
}

export interface Job {
  id: string
  queue_id: string
  project_id: string
  kind: JobKind | string
  handler: string
  status: JobStatus
  /** smallint -100..100, HIGHER RUNS FIRST. */
  priority: number
  /** The only eligibility timestamp. */
  run_at: string
  /** Cron occurrence instant only; null for everything else. */
  scheduled_for?: string | null
  payload: Record<string, unknown>
  attempt: number
  max_attempts: number
  worker_id: string | null
  lease_epoch: number
  claimed_at: string | null
  started_at: string | null
  finished_at: string | null
  last_error_class: string | null
  last_error_message: string | null
  created_at: string
}

export interface JobExecution {
  id: number
  job_id: string
  attempt_number: number
  worker_id: string | null
  lease_epoch: number
  status: ExecutionStatus
  claimed_at: string
  started_at: string | null
  finished_at: string | null
  duration_ms: number | null
  queue_wait_ms: number | null
  next_retry_at: string | null
  error_class: string | null
  error_message: string | null
  result?: Record<string, unknown> | null
}

export interface JobLog {
  id: number
  execution_id: number
  seq: number
  level: string
  message: string
  logged_at: string
}

export interface WorkerRow {
  id: string
  name: string
  hostname?: string | null
  status: WorkerStatus | string
  concurrency?: number | null
  last_heartbeat_at: string | null
  drain_requested?: boolean
  started_at?: string | null
  /** Server-side count of claimed+running jobs, when the API supplies it. */
  active_jobs?: number | null
}

export interface QueueStats {
  queue_id?: string
  scheduled: number
  queued: number
  claimed: number
  running: number
  completed?: number
  failed?: number
  dead_letter?: number
}

export interface MetricsSummary {
  completed: number
  failed: number
  dead_letter: number
  running: number
  queued: number
  scheduled?: number
  /** Mean, not p50 — the rollup stores sums, percentiles need a second pass. */
  avg_duration_ms?: number | null
}

export interface ThroughputPoint {
  bucket: string
  completed: number
  failed: number
  dead_letter: number
}

export interface SystemStatus {
  workers_total: number
  workers_live: number
  dlq_depth: number
  oldest_overdue_seconds: number | null
  scheduler_last_tick_age_seconds: number | null
}

/** Cursor page envelope. `total` is deliberately absent — counts come from /stats. */
export interface PageInfo {
  limit: number
  has_more: boolean
  next_cursor: string | null
}

export interface Paged<T> {
  data: T[]
  page: PageInfo
  meta?: { request_id: string }
}
