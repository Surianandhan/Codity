import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react'
import type { ReactNode } from 'react'
import { clearTokens, getAccessToken, setTokens } from '../api/client'
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
    window.addEventListener('codity:unauthorized', onUnauthorized)
    return () => window.removeEventListener('codity:unauthorized', onUnauthorized)
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
