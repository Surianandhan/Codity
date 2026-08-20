/**
 * Keyset cursors are opaque base64 `(created_at, id)` tuples minted by the API.
 * The client never constructs one; it only carries the last page's
 * `next_cursor` forward and keeps a stack so "previous" works without offsets.
 */
export interface CursorState {
  /** Cursor for the page currently displayed (null = first page). */
  current: string | null
  /** Cursors of the pages behind us, most recent last. */
  history: string[]
}

export const initialCursor: CursorState = { current: null, history: [] }

export function advance(state: CursorState, nextCursor: string | null): CursorState {
  if (!nextCursor) return state
  return { current: nextCursor, history: [...state.history, state.current ?? ''] }
}

export function retreat(state: CursorState): CursorState {
  if (state.history.length === 0) return initialCursor
  const history = state.history.slice(0, -1)
  const previous = state.history[state.history.length - 1]
  return { current: previous === '' ? null : previous, history }
}

export function canRetreat(state: CursorState): boolean {
  return state.history.length > 0
}
