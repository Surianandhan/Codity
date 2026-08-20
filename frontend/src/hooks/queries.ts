/**
 * TanStack Query hooks. Polling, not WebSockets: WebSockets are a listed bonus
 * and polling's cost is bounded and predictable.
 *
 * | Query                     | Interval |
 * | Job detail (non-terminal) | 2s, stops on terminal status |
 * | Job list                  | 5s |
 * | Queue health / workers    | 10s |
 * | Throughput chart          | 30s |
 * | Anything on a hidden tab  | paused (refetchIntervalInBackground: false) |
 */
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import type { UseQueryResult } from '@tanstack/react-query'
import {
  cancelJob,
  fetchJob,
  fetchMetricsSummary,
  fetchQueue,
  fetchQueueStats,
  fetchSystemStatus,
  fetchThroughput,
  listJobExecutions,
  listJobLogs,
  listProjectJobs,
  listProjects,
  listQueueJobs,
  listQueues,
  listWorkers,
  pauseQueue,
  replayJob,
  resumeQueue,
} from '../api/endpoints'
import type { JobFilters } from '../api/endpoints'
import { nextCursorOf, unwrapList } from '../api/client'
import type { Job, JobExecution, JobLog, Paged, Project, Queue, WorkerRow } from '../types'
import { isTerminal } from '../types'

export const POLL = {
  jobDetail: 2_000,
  list: 5_000,
  queues: 10_000,
  workers: 10_000,
  chart: 30_000,
} as const

const shared = { refetchIntervalInBackground: false, retry: 1 } as const

export function useProjects(orgId: string | undefined): UseQueryResult<Project[]> {
  return useQuery({
    queryKey: ['projects', orgId],
    queryFn: () => listProjects(orgId as string),
    enabled: Boolean(orgId),
    ...shared,
  })
}

export function useQueues(projectId: string | undefined) {
  return useQuery({
    queryKey: ['queues', projectId],
    queryFn: () => listQueues(projectId as string),
    enabled: Boolean(projectId),
    refetchInterval: POLL.queues,
    ...shared,
  })
}

export function useQueue(queueId: string | undefined) {
  return useQuery({
    queryKey: ['queue', queueId],
    queryFn: () => fetchQueue(queueId as string),
    enabled: Boolean(queueId),
    refetchInterval: POLL.queues,
    ...shared,
  })
}

export function useQueueStats(queueId: string | undefined) {
  return useQuery({
    queryKey: ['queue-stats', queueId],
    queryFn: () => fetchQueueStats(queueId as string),
    enabled: Boolean(queueId),
    refetchInterval: POLL.queues,
    ...shared,
  })
}

export interface JobList {
  jobs: Job[]
  nextCursor: string | null
}

function toJobList(payload: Job[] | Paged<Job>): JobList {
  return { jobs: unwrapList<Job>(payload), nextCursor: nextCursorOf(payload) }
}

export function useProjectJobs(projectId: string | undefined, filters: JobFilters) {
  return useQuery({
    queryKey: ['project-jobs', projectId, filters],
    queryFn: async () => toJobList(await listProjectJobs(projectId as string, filters)),
    enabled: Boolean(projectId),
    refetchInterval: POLL.list,
    placeholderData: (previous) => previous,
    ...shared,
  })
}

export function useQueueJobs(queueId: string | undefined, filters: JobFilters) {
  return useQuery({
    queryKey: ['queue-jobs', queueId, filters],
    queryFn: async () => toJobList(await listQueueJobs(queueId as string, filters)),
    enabled: Boolean(queueId),
    refetchInterval: POLL.list,
    placeholderData: (previous) => previous,
    ...shared,
  })
}

export function useJob(jobId: string | undefined) {
  return useQuery({
    queryKey: ['job', jobId],
    queryFn: () => fetchJob(jobId as string),
    enabled: Boolean(jobId),
    // Terminal states have zero out-edges, so there is nothing left to poll for.
    refetchInterval: (query) => (isTerminal(query.state.data?.status) ? false : POLL.jobDetail),
    ...shared,
  })
}

export function useJobExecutions(
  jobId: string | undefined,
  live: boolean,
): UseQueryResult<JobExecution[]> {
  return useQuery({
    queryKey: ['job-executions', jobId],
    queryFn: () => listJobExecutions(jobId as string),
    enabled: Boolean(jobId),
    refetchInterval: live ? POLL.jobDetail : false,
    ...shared,
  })
}

export function useJobLogs(jobId: string | undefined, live: boolean): UseQueryResult<JobLog[]> {
  return useQuery({
    queryKey: ['job-logs', jobId],
    queryFn: () => listJobLogs(jobId as string),
    enabled: Boolean(jobId),
    refetchInterval: live ? POLL.jobDetail : false,
    ...shared,
  })
}

export function useWorkers(orgId: string | undefined): UseQueryResult<WorkerRow[]> {
  return useQuery({
    queryKey: ['workers', orgId],
    queryFn: () => listWorkers(orgId as string),
    enabled: Boolean(orgId),
    refetchInterval: POLL.workers,
    ...shared,
  })
}

export function useMetricsSummary(projectId: string | undefined) {
  return useQuery({
    queryKey: ['metrics-summary', projectId],
    queryFn: () => fetchMetricsSummary(projectId as string),
    enabled: Boolean(projectId),
    refetchInterval: POLL.chart,
    ...shared,
  })
}

export function useThroughput(projectId: string | undefined) {
  return useQuery({
    queryKey: ['throughput', projectId],
    queryFn: () => fetchThroughput(projectId as string),
    enabled: Boolean(projectId),
    refetchInterval: POLL.chart,
    ...shared,
  })
}

export function useSystemStatus() {
  return useQuery({
    queryKey: ['system-status'],
    queryFn: fetchSystemStatus,
    refetchInterval: POLL.workers,
    ...shared,
  })
}

/**
 * Pause/resume is optimistic with rollback: the operator sees the badge flip
 * immediately, and a rejected write puts the old value back.
 */
export function useQueuePauseToggle(projectId: string | undefined) {
  const client = useQueryClient()
  return useMutation({
    mutationFn: ({ queueId, pause }: { queueId: string; pause: boolean }) =>
      pause ? pauseQueue(queueId) : resumeQueue(queueId),
    onMutate: async ({ queueId, pause }) => {
      await client.cancelQueries({ queryKey: ['queues', projectId] })
      const previous = client.getQueryData<Queue[]>(['queues', projectId])
      if (previous) {
        client.setQueryData<Queue[]>(
          ['queues', projectId],
          previous.map((queue) =>
            queue.id === queueId
              ? { ...queue, is_paused: pause, paused_at: pause ? new Date().toISOString() : null }
              : queue,
          ),
        )
      }
      return { previous }
    },
    onError: (_error, _variables, context) => {
      if (context?.previous) client.setQueryData(['queues', projectId], context.previous)
    },
    onSettled: (_data, _error, variables) => {
      void client.invalidateQueries({ queryKey: ['queues', projectId] })
      void client.invalidateQueries({ queryKey: ['queue', variables.queueId] })
    },
  })
}

export function useJobActions(jobId: string | undefined) {
  const client = useQueryClient()
  const invalidate = () => {
    void client.invalidateQueries({ queryKey: ['job', jobId] })
    void client.invalidateQueries({ queryKey: ['project-jobs'] })
    void client.invalidateQueries({ queryKey: ['queue-jobs'] })
  }

  const replay = useMutation({
    mutationFn: (id: string) => replayJob(id),
    onSuccess: invalidate,
  })

  const cancel = useMutation({
    mutationFn: (id: string) => cancelJob(id),
    onSuccess: invalidate,
  })

  return { replay, cancel }
}
