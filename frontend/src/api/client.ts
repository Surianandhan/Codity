/**
 * The one place that speaks HTTP. Everything above it deals in typed values.
 *
 * Session: the access token is persisted to localStorage and rehydrated on
 * mount. An in-memory-only token logs the operator out on every page reload,
 * which is the first thing anyone does.
 */

const TOKEN_KEY = 'codity.access_token'
const REFRESH_KEY = 'codity.refresh_token'

export const BASE_URL = '/api/v1'

/** The server closed the session outright. */
export const UNAUTHORIZED_EVENT = 'codity:unauthorized'
/** The server refused a request; it may or may not be the session. */
export const FORBIDDEN_EVENT = 'codity:forbidden'

export function getAccessToken(): string | null {
  try {
    return window.localStorage.getItem(TOKEN_KEY)
  } catch {
    return null
  }
}

export function setTokens(access: string, refresh?: string): void {
  try {
    window.localStorage.setItem(TOKEN_KEY, access)
    if (refresh) window.localStorage.setItem(REFRESH_KEY, refresh)
  } catch {
    /* storage disabled; session degrades to in-memory for this tab */
  }
}

export function clearTokens(): void {
  try {
    window.localStorage.removeItem(TOKEN_KEY)
    window.localStorage.removeItem(REFRESH_KEY)
  } catch {
    /* nothing to clear */
  }
}

export class ApiError extends Error {
  readonly status: number
  readonly code: string
  readonly requestId: string | null

  constructor(status: number, code: string, message: string, requestId: string | null) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.code = code
    this.requestId = requestId
  }
}

interface ErrorEnvelope {
  error?: { code?: string; message?: string; request_id?: string }
  detail?: unknown
}

function messageFrom(body: ErrorEnvelope, status: number): string {
  if (body.error?.message) return body.error.message
  if (typeof body.detail === 'string') return body.detail
  if (Array.isArray(body.detail) && body.detail.length > 0) {
    const first = body.detail[0] as { msg?: string }
    if (first?.msg) return first.msg
  }
  return `Request failed with status ${status}`
}

export interface RequestOptions {
  method?: string
  body?: unknown
  signal?: AbortSignal
  headers?: Record<string, string>
  /** Skip the Authorization header (login/register). */
  anonymous?: boolean
}

export async function request<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const headers: Record<string, string> = { Accept: 'application/json', ...options.headers }
  if (options.body !== undefined) headers['Content-Type'] = 'application/json'

  const token = options.anonymous ? null : getAccessToken()
  if (token) headers['Authorization'] = `Bearer ${token}`

  const response = await fetch(`${BASE_URL}${path}`, {
    method: options.method ?? 'GET',
    headers,
    credentials: 'include',
    signal: options.signal,
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
  })

  if (response.status === 204) return undefined as T

  const text = await response.text()
  let parsed: unknown = null
  if (text) {
    try {
      parsed = JSON.parse(text)
    } catch {
      parsed = null
    }
  }

  if (!response.ok) {
    const envelope = (parsed ?? {}) as ErrorEnvelope
    if (response.status === 401 && !options.anonymous) {
      clearTokens()
      window.dispatchEvent(new CustomEvent(UNAUTHORIZED_EVENT))
    }
    // The API raises PermissionDenied -> 403 for a missing, malformed, expired
    // or revoked bearer token; it never issues 401, and there is no
    // /auth/refresh to fall back on. But 403 is *also* the honest answer for a
    // legitimate authorization failure (owner-only route, wrong org), so this
    // deliberately does not clear the session. It only announces "the server
    // refused"; useAuth decides by re-checking /auth/me, and only that failing
    // ends the session.
    if (response.status === 403 && !options.anonymous) {
      window.dispatchEvent(new CustomEvent(FORBIDDEN_EVENT))
    }
    throw new ApiError(
      response.status,
      envelope.error?.code ?? `http_${response.status}`,
      messageFrom(envelope, response.status),
      envelope.error?.request_id ?? response.headers.get('x-request-id'),
    )
  }

  return parsed as T
}

/**
 * List endpoints are migrating from a bare array to the keyset envelope
 * `{data, page, meta}`. Accept both so the dashboard does not break mid-slice.
 */
export function unwrapList<T>(payload: T[] | { data?: T[] } | null | undefined): T[] {
  if (Array.isArray(payload)) return payload
  if (payload && Array.isArray(payload.data)) return payload.data
  return []
}

export function nextCursorOf(payload: unknown): string | null {
  if (payload && typeof payload === 'object' && 'page' in payload) {
    const page = (payload as { page?: { next_cursor?: string | null } }).page
    return page?.next_cursor ?? null
  }
  return null
}

export function query(params: Record<string, string | number | boolean | undefined | null | string[]>): string {
  const search = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === '') continue
    if (Array.isArray(value)) {
      for (const item of value) search.append(key, item)
    } else {
      search.append(key, String(value))
    }
  }
  const encoded = search.toString()
  return encoded ? `?${encoded}` : ''
}
