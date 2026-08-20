import { Navigate, Route, Routes, useLocation } from 'react-router-dom'
import type { ReactElement } from 'react'
import { Layout } from './components/Layout'
import { EmptyState } from './components/EmptyState'
import { useAuth } from './hooks/useAuth'
import { useProjects } from './hooks/queries'
import { JobPage } from './pages/JobPage'
import { LoginPage } from './pages/LoginPage'
import { ProjectPage } from './pages/ProjectPage'
import { QueuePage } from './pages/QueuePage'
import { WorkersPage } from './pages/WorkersPage'

function Protected({ children }: { children: ReactElement }) {
  const { me, bootstrapping } = useAuth()
  const location = useLocation()

  // Wait for the stored token to be checked; redirecting first would log the
  // operator out on every reload.
  if (bootstrapping) {
    return <div className="grid min-h-screen place-items-center bg-ink-950 text-ink-400">…</div>
  }
  if (!me) return <Navigate to="/login" replace state={{ from: location.pathname }} />
  return <Layout>{children}</Layout>
}

/** `/` has no screen of its own — it lands on the first project. */
function Home() {
  const { me } = useAuth()
  const projects = useProjects(me?.organization_id)

  if (projects.isLoading) return <div className="h-40 animate-pulse rounded bg-ink-900/60" />
  const first = projects.data?.[0]
  if (!first) {
    return (
      <EmptyState
        title="No projects yet"
        description="Create one with POST /api/v1/orgs/{org_id}/projects, or run scripts/seed.py."
      />
    )
  }
  return <Navigate to={`/projects/${first.id}`} replace />
}

export function App() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route
        path="/"
        element={
          <Protected>
            <Home />
          </Protected>
        }
      />
      <Route
        path="/projects/:projectId"
        element={
          <Protected>
            <ProjectPage />
          </Protected>
        }
      />
      <Route
        path="/queues/:queueId"
        element={
          <Protected>
            <QueuePage />
          </Protected>
        }
      />
      <Route
        path="/jobs/:jobId"
        element={
          <Protected>
            <JobPage />
          </Protected>
        }
      />
      <Route
        path="/orgs/:orgId/workers"
        element={
          <Protected>
            <WorkersPage />
          </Protected>
        }
      />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  )
}
