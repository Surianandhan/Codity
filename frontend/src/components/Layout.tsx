import { Link, NavLink, useNavigate } from 'react-router-dom'
import type { ReactNode } from 'react'
import { useAuth } from '../hooks/useAuth'
import { useProjects, useSystemStatus } from '../hooks/queries'
import { schedulerTickAgeSeconds } from '../types'

/** `== null` catches undefined as well as null; `=== null` did not, and
 * Math.round(undefined) is how a header renders the word "NaN". */
function healthTone(seconds: number | null | undefined): string {
  if (seconds == null) return 'text-ink-400'
  if (seconds > 30) return 'text-rose-300'
  if (seconds > 10) return 'text-amber-300'
  return 'text-emerald-300'
}

export function Layout({ children }: { children: ReactNode }) {
  const { me, signOut } = useAuth()
  const navigate = useNavigate()
  const projects = useProjects(me?.organization_id)
  const status = useSystemStatus()

  const firstProject = projects.data?.[0]
  // One row per scheduler loop; the set is as healthy as its laggiest member.
  const tickAge = schedulerTickAgeSeconds(status.data)

  return (
    <div className="min-h-screen bg-ink-950 text-ink-100">
      <header className="border-b border-ink-800 bg-ink-900/60">
        <div className="mx-auto flex max-w-7xl flex-wrap items-center gap-4 px-4 py-3">
          <Link to="/" className="text-sm font-semibold tracking-tight text-ink-50">
            Codity <span className="text-ink-400">· job scheduler</span>
          </Link>

          <nav className="flex items-center gap-1 text-sm">
            {firstProject ? (
              <NavLink
                to={`/projects/${firstProject.id}`}
                className={({ isActive }) =>
                  `rounded px-2 py-1 ${isActive ? 'bg-ink-800 text-ink-50' : 'text-ink-300 hover:text-ink-50'}`
                }
              >
                {firstProject.name}
              </NavLink>
            ) : null}
            {me ? (
              <NavLink
                to={`/orgs/${me.organization_id}/workers`}
                className={({ isActive }) =>
                  `rounded px-2 py-1 ${isActive ? 'bg-ink-800 text-ink-50' : 'text-ink-300 hover:text-ink-50'}`
                }
              >
                Workers
              </NavLink>
            ) : null}
          </nav>

          <div className="ml-auto flex items-center gap-4 text-xs text-ink-400">
            {status.data ? (
              <>
                <span>
                  fleet{' '}
                  <span className="text-ink-100">
                    {status.data.fleet.live}/{status.data.fleet.total}
                  </span>
                </span>
                <span>
                  DLQ <span className="text-ink-100">{status.data.dlq_depth}</span>
                </span>
                <span className={healthTone(tickAge)}>
                  scheduler tick {tickAge == null ? 'unknown' : `${Math.round(tickAge)}s ago`}
                </span>
              </>
            ) : null}
            <span className="hidden sm:inline">{me?.email}</span>
            <button
              type="button"
              className="btn-secondary"
              onClick={() => {
                signOut()
                navigate('/login', { replace: true })
              }}
            >
              Sign out
            </button>
          </div>
        </div>
      </header>
      <main className="mx-auto max-w-7xl px-4 py-6">{children}</main>
    </div>
  )
}
