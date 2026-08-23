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
  /**
   * Heartbeat age measured by `now()` in Postgres. Liveness is never re-derived
   * from the browser clock: that would make fleet health depend on NTP agreement
   * between the operator's laptop and the database.
   */
  heartbeat_age_seconds: number | null
  /** Server-computed against LIVENESS_GRACE_SECONDS (150s). */
  is_live: boolean
  drain_requested?: boolean
  started_at?: string | null
  /** Claimed + running jobs held by this worker. */
  inflight: number
}

/** The metrics window vocabulary; anything else is a 422 from the API. */
export const METRICS_WINDOWS = ['15m', '1h', '6h', '24h', '7d'] as const
export type MetricsWindow = (typeof METRICS_WINDOWS)[number]

export const METRICS_BUCKETS = ['1m', '5m', '15m', '1h'] as const
export type MetricsBucket = (typeof METRICS_BUCKETS)[number]

/**
 * Depth comes from `ix_jobs_depth`, a partial index over the LIVE statuses only,
 * so terminal counts — `dead_letter` above all — are structurally absent here.
 * Read those from the window rollup (`dead_lettered`) or from `dlq_open`.
 */
export type DepthCounts = Partial<Record<'scheduled' | 'queued' | 'claimed' | 'running', number>>

/** The window rollup fields shared by the summary and queue-stats responses. */
export interface WindowRollup {
  enqueued: number
  completed: number
  failed: number
  dead_lettered: number
  retried: number
  /** Mean, not p50 — the rollup stores sums, percentiles need a second pass. */
  mean_duration_ms: number | null
  max_duration_ms: number
}

export interface QueueStats extends WindowRollup {
  queue_id: string
  name: string
  is_paused: boolean
  max_concurrency: number
  depth: DepthCounts
  /** Server-computed claimed + running; do not recompute it client-side. */
  inflight: number
  /** What a claim would be allowed to take right now: max_concurrency - inflight. */
  headroom: number
  oldest_queued_age_seconds: number | null
  window: MetricsWindow
}

export interface MetricsSummary extends WindowRollup {
  project_id: string
  window: MetricsWindow
  depth: DepthCounts
  success_rate: number | null
  /** Unresolved dead-letter entries — a level, not a windowed count. */
  dlq_open: number
  oldest_overdue_seconds: number | null
}

export interface ThroughputPoint extends WindowRollup {
  bucket_start: string
}

export interface Fleet {
  total: number
  live: number
  stale: number
  draining: number
  inflight: number
}

export interface OverdueScheduled {
  job_id: string
  run_at: string
  age_seconds: number
}

export interface SchedulerLoop {
  name: string
  last_run_at: string
  age_seconds: number
  is_stale: boolean
  last_error: string | null
}

export interface SystemStatus {
  healthy: boolean
  fleet: Fleet
  /** null when nothing is overdue — the healthy case, not missing data. */
  oldest_overdue_scheduled: OverdueScheduled | null
  dlq_depth: number
  /** One row per scheduler loop (promoter, reaper, …), cluster-wide. */
  scheduler: SchedulerLoop[]
}

/**
 * The fleet's worst-case tick age: the API reports one row per scheduler loop,
 * and the health of the set is the health of its laggiest member. `null` means
 * no loop has ever reported — which is itself worth rendering as "unknown"
 * rather than as a number.
 */
export function schedulerTickAgeSeconds(status: SystemStatus | undefined): number | null {
  const loops = status?.scheduler
  if (!loops || loops.length === 0) return null
  return Math.max(...loops.map((loop) => loop.age_seconds))
}

/** Live depth that is actually executing or about to: claimed + running. */
export function inflightOf(depth: DepthCounts | undefined): number {
  return (depth?.claimed ?? 0) + (depth?.running ?? 0)
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
