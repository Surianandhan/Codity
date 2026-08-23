import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react'
import type { ReactNode } from 'react'
import {
  FORBIDDEN_EVENT,
  UNAUTHORIZED_EVENT,
  clearTokens,
  getAccessToken,
  setTokens,
} from '../api/client'
import { fetchMe, login as loginRequest, register as registerRequest } from '../api/endpoints'
import type { Me } from '../types'

interface AuthValue {
  me: Me | null
  /** True until the stored token has been checked against /auth/me on mount. */
  bootstrapping: boolean
  signIn: (email: string, password: string) => Promise<void>
  signUp: (email: string, password: string, organizationName: string) => Promise<void>
  signOut: () => void
}

const AuthContext = createContext<AuthValue | null>(null)

export function AuthProvider({ children }: { children: ReactNode }) {
  const [me, setMe] = useState<Me | null>(null)
  const [bootstrapping, setBootstrapping] = useState(true)

  // Rehydrate on mount. Without this a page reload — the very first thing
  // anyone does — logs the operator out even though the token is still valid.
  useEffect(() => {
    let cancelled = false
    const token = getAccessToken()
    if (!token) {
      setBootstrapping(false)
      return
    }
    fetchMe()
      .then((principal) => {
        if (!cancelled) setMe(principal)
      })
      .catch(() => {
        clearTokens()
      })
      .finally(() => {
        if (!cancelled) setBootstrapping(false)
      })
    return () => {
      cancelled = true
    }
  }, [])

  useEffect(() => {
    const onUnauthorized = () => setMe(null)
    window.addEventListener(UNAUTHORIZED_EVENT, onUnauthorized)
    return () => window.removeEventListener(UNAUTHORIZED_EVENT, onUnauthorized)
  }, [])

  /**
   * Access tokens expire (15 min) and there is no refresh endpoint, so a session
   * that outlives its token would otherwise sit on a dashboard of red boxes
   * until the operator thought to reload — every poll failing, nothing saying
   * why.
   *
   * A 403 alone is not proof of that: the same status is the correct answer to a
   * legitimate authorization failure, and treating it as logout would eject an
   * operator for clicking one button they lack the role for. So a 403 only
   * triggers the one question that distinguishes the two — GET /auth/me. If it
   * answers, the token is fine and the 403 was about the resource; if it fails,
   * the session is genuinely over and Protected redirects to /login.
   *
   * The in-flight guard matters because a dashboard is many concurrent polls:
   * an expiry fires a 403 from every one of them at once, and this must ask
   * once, not once per query.
   */
  useEffect(() => {
    let revalidating = false
    const onForbidden = () => {
      if (revalidating || !getAccessToken()) return
      revalidating = true
      fetchMe()
        .then((principal) => setMe(principal))
        .catch(() => {
          clearTokens()
          setMe(null)
        })
        .finally(() => {
          revalidating = false
        })
    }
    window.addEventListener(FORBIDDEN_EVENT, onForbidden)
    return () => window.removeEventListener(FORBIDDEN_EVENT, onForbidden)
  }, [])

  const signIn = useCallback(async (email: string, password: string) => {
    const tokens = await loginRequest(email, password)
    setTokens(tokens.access_token, tokens.refresh_token)
    setMe(await fetchMe())
  }, [])

  const signUp = useCallback(async (email: string, password: string, organizationName: string) => {
    const tokens = await registerRequest({ email, password, organization_name: organizationName })
    setTokens(tokens.access_token, tokens.refresh_token)
    setMe(await fetchMe())
  }, [])

  const signOut = useCallback(() => {
    clearTokens()
    setMe(null)
  }, [])

  const value = useMemo<AuthValue>(
    () => ({ me, bootstrapping, signIn, signUp, signOut }),
    [me, bootstrapping, signIn, signUp, signOut],
  )

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
}

export function useAuth(): AuthValue {
  const value = useContext(AuthContext)
  if (!value) throw new Error('useAuth must be used inside <AuthProvider>')
  return value
}
