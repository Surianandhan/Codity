export function formatInstant(value: string | null | undefined): string {
  if (!value) return '—'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return '—'
  return date.toLocaleString(undefined, {
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}

export function formatClock(value: string | null | undefined): string {
  if (!value) return '—'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return '—'
  return date.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit' })
}

export function formatDuration(ms: number | null | undefined): string {
  if (ms === null || ms === undefined || Number.isNaN(ms)) return '—'
  if (ms < 1000) return `${Math.round(ms)}ms`
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`
  const minutes = Math.floor(ms / 60_000)
  const seconds = Math.round((ms % 60_000) / 1000)
  return `${minutes}m ${seconds}s`
}

export function relativeTo(value: string | null | undefined, now: number = Date.now()): string {
  if (!value) return '—'
  const then = new Date(value).getTime()
  if (Number.isNaN(then)) return '—'
  const delta = now - then
  const abs = Math.abs(delta)
  const suffix = delta >= 0 ? 'ago' : 'from now'
  if (abs < 1000) return 'just now'
  if (abs < 60_000) return `${Math.round(abs / 1000)}s ${suffix}`
  if (abs < 3_600_000) return `${Math.round(abs / 60_000)}m ${suffix}`
  if (abs < 86_400_000) return `${Math.round(abs / 3_600_000)}h ${suffix}`
  return `${Math.round(abs / 86_400_000)}d ${suffix}`
}

export function shortId(id: string | null | undefined, size = 8): string {
  if (!id) return '—'
  return id.slice(0, size)
}

export function prettyJson(value: unknown): string {
  try {
    return JSON.stringify(value ?? {}, null, 2)
  } catch {
    return String(value)
  }
}

/** 'dead_letter' -> 'dead letter'. Labels only; never used to build a filter value. */
export function humanize(value: string): string {
  return value.replace(/_/g, ' ')
}
