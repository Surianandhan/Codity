import { useState } from 'react'
import type { FormEvent } from 'react'
import { Navigate, useNavigate } from 'react-router-dom'
import { useAuth } from '../hooks/useAuth'

export function LoginPage() {
  const { me, bootstrapping, signIn, signUp } = useAuth()
  const navigate = useNavigate()
  const [mode, setMode] = useState<'signin' | 'signup'>('signin')
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [organization, setOrganization] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  if (bootstrapping) {
    return <div className="grid min-h-screen place-items-center bg-ink-950 text-ink-400">…</div>
  }
  if (me) return <Navigate to="/" replace />

  const onSubmit = async (event: FormEvent) => {
    event.preventDefault()
    setBusy(true)
    setError(null)
    try {
      if (mode === 'signin') {
        await signIn(email, password)
      } else {
        await signUp(email, password, organization)
      }
      navigate('/', { replace: true })
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : 'Sign-in failed')
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="grid min-h-screen place-items-center bg-ink-950 px-4 text-ink-100">
      <div className="w-full max-w-sm">
        <h1 className="text-lg font-semibold">Codity</h1>
        <p className="mt-1 text-sm text-ink-400">
          Distributed job scheduler. Sign in to see queues, jobs and the worker fleet.
        </p>

        <form onSubmit={onSubmit} className="mt-6 space-y-3">
          <label className="block">
            <span className="text-xs uppercase tracking-wide text-ink-400">Email</span>
            <input
              className="input mt-1"
              type="email"
              autoComplete="username"
              required
              value={email}
              onChange={(event) => setEmail(event.target.value)}
            />
          </label>

          <label className="block">
            <span className="text-xs uppercase tracking-wide text-ink-400">Password</span>
            <input
              className="input mt-1"
              type="password"
              autoComplete={mode === 'signin' ? 'current-password' : 'new-password'}
              required
              minLength={8}
              value={password}
              onChange={(event) => setPassword(event.target.value)}
            />
          </label>

          {mode === 'signup' ? (
            <label className="block">
              <span className="text-xs uppercase tracking-wide text-ink-400">Organization</span>
              <input
                className="input mt-1"
                required
                value={organization}
                onChange={(event) => setOrganization(event.target.value)}
              />
            </label>
          ) : null}

          {error ? (
            <div className="rounded border border-rose-500/30 bg-rose-500/10 px-3 py-2 text-sm text-rose-200">
              {error}
            </div>
          ) : null}

          <button type="submit" className="btn-primary w-full" disabled={busy}>
            {busy ? 'Working…' : mode === 'signin' ? 'Sign in' : 'Create organization'}
          </button>
        </form>

        <button
          type="button"
          className="mt-4 text-xs text-ink-400 underline underline-offset-2 hover:text-ink-200"
          onClick={() => {
            setMode(mode === 'signin' ? 'signup' : 'signin')
            setError(null)
          }}
        >
          {mode === 'signin' ? 'Need an organization? Register' : 'Already registered? Sign in'}
        </button>
      </div>
    </div>
  )
}
