/**
 * Typed wrappers over the REST surface in docs/API.md. One function per
 * endpoint the dashboard uses; no component builds a URL by hand.
 */
import type {
  Job,
  JobExecution,
  JobLog,
  Me,
  MetricsSummary,
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

export const fetchQueue = (queueId: string) => request<Queue>(`/queues/${queueId}`)

export const fetchQueueStats = (queueId: string) =>
  request<QueueStats>(`/queues/${queueId}/stats`)

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

export const fetchMetricsSummary = (projectId: string) =>
  request<MetricsSummary>(`/projects/${projectId}/metrics/summary`)

export const fetchThroughput = async (projectId: string, windowMinutes = 60) =>
  unwrapList<ThroughputPoint>(
    await request<ThroughputPoint[] | Paged<ThroughputPoint>>(
      `/projects/${projectId}/metrics/throughput${query({ window: `${windowMinutes}m`, bucket: '1m' })}`,
    ),
  )

export const fetchSystemStatus = () => request<SystemStatus>('/system/status')
