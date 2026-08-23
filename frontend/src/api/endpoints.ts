/**
 * Typed wrappers over the REST surface the routers actually implement. One
 * function per endpoint the dashboard uses; no component builds a URL by hand.
 *
 * Every path and every literal here is checked against
 * backend/app/api/routers/*.py, not against docs/API.md — a path the docs
 * describe but no router serves is a 404 at runtime and a clean build at
 * compile time.
 */
import type {
  Job,
  JobExecution,
  JobLog,
  Me,
  MetricsBucket,
  MetricsSummary,
  MetricsWindow,
  Paged,
  Project,
  Queue,
  QueueStats,
  SystemStatus,
  ThroughputPoint,
  Tokens,
  WorkerRow,
} from '../types'
import { query, request, unwrapList } from './client'

// --- auth ---
export const login = (email: string, password: string) =>
  request<Tokens>('/auth/login', { method: 'POST', body: { email, password }, anonymous: true })

export const register = (body: {
  email: string
  password: string
  organization_name: string
  full_name?: string
}) => request<Tokens>('/auth/register', { method: 'POST', body, anonymous: true })

export const fetchMe = () => request<Me>('/auth/me')

// --- projects & queues ---
export const listProjects = async (orgId: string) =>
  unwrapList<Project>(await request<Project[] | Paged<Project>>(`/orgs/${orgId}/projects`))

export const listQueues = async (projectId: string) =>
  unwrapList<Queue>(await request<Queue[] | Paged<Queue>>(`/projects/${projectId}/queues`))

/**
 * There is no `GET /queues/{id}`. This endpoint carries the queue's identity
 * (queue_id, name, is_paused, max_concurrency) alongside its depth, so the queue
 * screen is one request rather than two — and the second one was a 404.
 */
export const fetchQueueStats = (queueId: string, window: MetricsWindow = '1h') =>
  request<QueueStats>(`/queues/${queueId}/stats${query({ window })}`)

export const pauseQueue = (queueId: string) =>
  request<Queue>(`/queues/${queueId}/pause`, { method: 'POST' })

export const resumeQueue = (queueId: string) =>
  request<Queue>(`/queues/${queueId}/resume`, { method: 'POST' })

// --- jobs ---
export interface JobFilters {
  status?: string[]
  kind?: string
  queue_id?: string
  handler?: string
  limit?: number
  cursor?: string | null
}

export const listProjectJobs = (projectId: string, filters: JobFilters = {}) =>
  request<Job[] | Paged<Job>>(`/projects/${projectId}/jobs${query({ ...filters })}`)

export const listQueueJobs = (queueId: string, filters: JobFilters = {}) =>
  request<Job[] | Paged<Job>>(`/queues/${queueId}/jobs${query({ ...filters })}`)

export const fetchJob = (jobId: string) => request<Job>(`/jobs/${jobId}`)

export const listJobExecutions = async (jobId: string) =>
  unwrapList<JobExecution>(
    await request<JobExecution[] | Paged<JobExecution>>(`/jobs/${jobId}/executions`),
  )

export const listJobLogs = async (jobId: string) =>
  unwrapList<JobLog>(await request<JobLog[] | Paged<JobLog>>(`/jobs/${jobId}/logs${query({ limit: 200 })}`))

export const cancelJob = (jobId: string) => request<Job>(`/jobs/${jobId}/cancel`, { method: 'POST' })

/** Replay never resurrects a terminal job — it inserts a new one. */
export const replayJob = (jobId: string) => request<Job>(`/jobs/${jobId}/replay`, { method: 'POST' })

// --- fleet & metrics ---
export const listWorkers = async (orgId: string) =>
  unwrapList<WorkerRow>(await request<WorkerRow[] | Paged<WorkerRow>>(`/orgs/${orgId}/workers`))

export const fetchMetricsSummary = (projectId: string, window: MetricsWindow = '1h') =>
  request<MetricsSummary>(`/projects/${projectId}/metrics/summary${query({ window })}`)

/**
 * `window` is a closed enum on the server (15m | 1h | 6h | 24h | 7d). "60m" is
 * arithmetically an hour and is still a 422, which is why the type is the enum
 * and not a number of minutes.
 */
export const fetchThroughput = async (
  projectId: string,
  window: MetricsWindow = '1h',
  bucket: MetricsBucket = '1m',
) =>
  unwrapList<ThroughputPoint>(
    await request<ThroughputPoint[] | Paged<ThroughputPoint>>(
      `/projects/${projectId}/metrics/throughput${query({ window, bucket })}`,
    ),
  )

export const fetchSystemStatus = () => request<SystemStatus>('/system/status')
