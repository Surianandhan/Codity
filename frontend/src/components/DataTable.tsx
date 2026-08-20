import type { ReactNode } from 'react'
import { EmptyState, ErrorState, TableSkeleton } from './EmptyState'

export interface Column<T> {
  key: string
  header: ReactNode
  render: (row: T) => ReactNode
  className?: string
}

interface Props<T> {
  columns: Column<T>[]
  rows: T[] | undefined
  rowKey: (row: T) => string
  onRowClick?: (row: T) => void
  loading?: boolean
  error?: unknown
  onRetry?: () => void
  empty?: ReactNode
}

export function DataTable<T>({
  columns,
  rows,
  rowKey,
  onRowClick,
  loading = false,
  error,
  onRetry,
  empty,
}: Props<T>) {
  if (error) {
    return (
      <div className="rounded-lg border border-ink-800 bg-ink-900/40">
        <ErrorState error={error} onRetry={onRetry} />
      </div>
    )
  }
  if (loading && !rows) {
    return (
      <div className="rounded-lg border border-ink-800 bg-ink-900/40">
        <TableSkeleton />
      </div>
    )
  }
  if (!rows || rows.length === 0) {
    return <>{empty ?? <EmptyState title="Nothing here yet" />}</>
  }

  return (
    <div className="overflow-x-auto rounded-lg border border-ink-800 bg-ink-900/40">
      <table className="min-w-full text-sm">
        <thead>
          <tr className="border-b border-ink-800 text-left text-xs uppercase tracking-wide text-ink-400">
            {columns.map((column) => (
              <th key={column.key} className={`px-3 py-2 font-medium ${column.className ?? ''}`}>
                {column.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr
              key={rowKey(row)}
              onClick={onRowClick ? () => onRowClick(row) : undefined}
              className={`border-b border-ink-800/60 last:border-0 ${
                onRowClick ? 'cursor-pointer hover:bg-ink-800/40' : ''
              }`}
            >
              {columns.map((column) => (
                <td key={column.key} className={`px-3 py-2 align-middle ${column.className ?? ''}`}>
                  {column.render(row)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
