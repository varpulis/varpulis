import { create } from 'zustand'
import type { ConnectorDef, EventDef, PipelineState, StreamDef, StreamOperation } from '@/types/pipeline'

let nextId = 1
const genId = (prefix: string) => `${prefix}_${nextId++}`

const MAX_HISTORY = 50

interface PipelineStore extends PipelineState {
  // Stream actions
  addStream: (name?: string) => string
  removeStream: (id: string) => void
  updateStream: (id: string, patch: Partial<StreamDef>) => void
  addOperation: (streamId: string, type: string, value?: string) => void
  removeOperation: (streamId: string, opId: string) => void
  updateOperation: (streamId: string, opId: string, patch: Partial<StreamOperation>) => void
  reorderOperations: (streamId: string, fromIndex: number, toIndex: number) => void

  // Connector actions
  addConnector: (name: string, type: string) => string
  removeConnector: (id: string) => void
  updateConnector: (id: string, patch: Partial<ConnectorDef>) => void

  // Event actions
  addEvent: (name: string) => string
  removeEvent: (id: string) => void
  updateEvent: (id: string, patch: Partial<EventDef>) => void

  // Bulk actions
  loadPipeline: (state: PipelineState) => void
  clear: () => void

  // Undo/redo
  undo: () => void
  redo: () => void
  canUndo: boolean
  canRedo: boolean
  _history: PipelineState[]
  _future: PipelineState[]
}

const INITIAL_STATE: PipelineState = {
  connectors: [],
  events: [],
  streams: [],
}

function snap(s: PipelineStore): PipelineState {
  return { connectors: s.connectors, events: s.events, streams: s.streams }
}

export const usePipelineStore = create<PipelineStore>((set, get) => ({
  ...INITIAL_STATE,
  _history: [],
  _future: [],
  canUndo: false,
  canRedo: false,

  addStream: (name?: string) => {
    const id = genId('stream')
    const streamName = name || `Stream${id.split('_')[1]}`
    const prev = snap(get())
    set((s) => ({
      streams: [...s.streams, {
        id,
        name: streamName,
        source: '',
        sourceType: 'event' as const,
        sourceRefs: [],
        operations: [],
      }],
      _history: [...s._history, prev].slice(-MAX_HISTORY),
      _future: [],
      canUndo: true,
      canRedo: false,
    }))
    return id
  },

  removeStream: (id) => {
    const prev = snap(get())
    set((s) => ({
      streams: s.streams.filter((st) => st.id !== id),
      _history: [...s._history, prev].slice(-MAX_HISTORY),
      _future: [],
      canUndo: true,
      canRedo: false,
    }))
  },

  updateStream: (id, patch) => {
    const prev = snap(get())
    set((s) => ({
      streams: s.streams.map((st) => (st.id === id ? { ...st, ...patch } : st)),
      _history: [...s._history, prev].slice(-MAX_HISTORY),
      _future: [],
      canUndo: true,
      canRedo: false,
    }))
  },

  addOperation: (streamId, type, value = '') => {
    const opId = genId('op')
    const prev = snap(get())
    set((s) => ({
      streams: s.streams.map((st) =>
        st.id === streamId
          ? { ...st, operations: [...st.operations, { id: opId, type, value, expanded: true }] }
          : st
      ),
      _history: [...s._history, prev].slice(-MAX_HISTORY),
      _future: [],
      canUndo: true,
      canRedo: false,
    }))
  },

  removeOperation: (streamId, opId) => {
    const prev = snap(get())
    set((s) => ({
      streams: s.streams.map((st) =>
        st.id === streamId
          ? { ...st, operations: st.operations.filter((op) => op.id !== opId) }
          : st
      ),
      _history: [...s._history, prev].slice(-MAX_HISTORY),
      _future: [],
      canUndo: true,
      canRedo: false,
    }))
  },

  updateOperation: (streamId, opId, patch) => {
    const prev = snap(get())
    set((s) => ({
      streams: s.streams.map((st) =>
        st.id === streamId
          ? {
              ...st,
              operations: st.operations.map((op) =>
                op.id === opId ? { ...op, ...patch } : op
              ),
            }
          : st
      ),
      _history: [...s._history, prev].slice(-MAX_HISTORY),
      _future: [],
      canUndo: true,
      canRedo: false,
    }))
  },

  reorderOperations: (streamId, fromIndex, toIndex) => {
    const prev = snap(get())
    set((s) => ({
      streams: s.streams.map((st) => {
        if (st.id !== streamId) return st
        const ops = [...st.operations]
        const [moved] = ops.splice(fromIndex, 1)
        ops.splice(toIndex, 0, moved)
        return { ...st, operations: ops }
      }),
      _history: [...s._history, prev].slice(-MAX_HISTORY),
      _future: [],
      canUndo: true,
      canRedo: false,
    }))
  },

  addConnector: (name, type) => {
    const id = genId('conn')
    const prev = snap(get())
    set((s) => ({
      connectors: [...s.connectors, { id, name, type, config: {} }],
      _history: [...s._history, prev].slice(-MAX_HISTORY),
      _future: [],
      canUndo: true,
      canRedo: false,
    }))
    return id
  },

  removeConnector: (id) => {
    const prev = snap(get())
    set((s) => ({
      connectors: s.connectors.filter((c) => c.id !== id),
      _history: [...s._history, prev].slice(-MAX_HISTORY),
      _future: [],
      canUndo: true,
      canRedo: false,
    }))
  },

  updateConnector: (id, patch) => {
    const prev = snap(get())
    set((s) => ({
      connectors: s.connectors.map((c) => (c.id === id ? { ...c, ...patch } : c)),
      _history: [...s._history, prev].slice(-MAX_HISTORY),
      _future: [],
      canUndo: true,
      canRedo: false,
    }))
  },

  addEvent: (name) => {
    const id = genId('evt')
    const prev = snap(get())
    set((s) => ({
      events: [...s.events, { id, name, fields: [] }],
      _history: [...s._history, prev].slice(-MAX_HISTORY),
      _future: [],
      canUndo: true,
      canRedo: false,
    }))
    return id
  },

  removeEvent: (id) => {
    const prev = snap(get())
    set((s) => ({
      events: s.events.filter((e) => e.id !== id),
      _history: [...s._history, prev].slice(-MAX_HISTORY),
      _future: [],
      canUndo: true,
      canRedo: false,
    }))
  },

  updateEvent: (id, patch) => {
    const prev = snap(get())
    set((s) => ({
      events: s.events.map((e) => (e.id === id ? { ...e, ...patch } : e)),
      _history: [...s._history, prev].slice(-MAX_HISTORY),
      _future: [],
      canUndo: true,
      canRedo: false,
    }))
  },

  loadPipeline: (state) => set({
    ...state,
    _history: [],
    _future: [],
    canUndo: false,
    canRedo: false,
  }),

  clear: () => {
    const prev = snap(get())
    const isEmpty = prev.connectors.length === 0 && prev.events.length === 0 && prev.streams.length === 0
    set({
      ...INITIAL_STATE,
      _history: isEmpty ? [] : [...get()._history, prev].slice(-MAX_HISTORY),
      _future: [],
      canUndo: !isEmpty,
      canRedo: false,
    })
  },

  undo: () => {
    const { _history, _future } = get()
    if (_history.length === 0) return
    const current = snap(get())
    const prev = _history[_history.length - 1]
    set({
      ...prev,
      _history: _history.slice(0, -1),
      _future: [current, ..._future],
      canUndo: _history.length > 1,
      canRedo: true,
    })
  },

  redo: () => {
    const { _history, _future } = get()
    if (_future.length === 0) return
    const current = snap(get())
    const next = _future[0]
    set({
      ...next,
      _history: [..._history, current],
      _future: _future.slice(1),
      canUndo: true,
      canRedo: _future.length > 1,
    })
  },
}))
