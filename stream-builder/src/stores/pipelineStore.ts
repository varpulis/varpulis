import { create } from 'zustand'
import type { ConnectorDef, EventDef, PipelineState, StreamDef, StreamOperation } from '@/types/pipeline'

let nextId = 1
const genId = (prefix: string) => `${prefix}_${nextId++}`

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
}

const INITIAL_STATE: PipelineState = {
  connectors: [],
  events: [],
  streams: [],
}

export const usePipelineStore = create<PipelineStore>((set) => ({
  ...INITIAL_STATE,

  addStream: (name?: string) => {
    const id = genId('stream')
    const streamName = name || `Stream${id.split('_')[1]}`
    set((s) => ({
      streams: [...s.streams, {
        id,
        name: streamName,
        source: '',
        sourceType: 'event' as const,
        sourceRefs: [],
        operations: [],
      }],
    }))
    return id
  },

  removeStream: (id) =>
    set((s) => ({ streams: s.streams.filter((st) => st.id !== id) })),

  updateStream: (id, patch) =>
    set((s) => ({
      streams: s.streams.map((st) => (st.id === id ? { ...st, ...patch } : st)),
    })),

  addOperation: (streamId, type, value = '') => {
    const opId = genId('op')
    set((s) => ({
      streams: s.streams.map((st) =>
        st.id === streamId
          ? { ...st, operations: [...st.operations, { id: opId, type, value, expanded: true }] }
          : st
      ),
    }))
  },

  removeOperation: (streamId, opId) =>
    set((s) => ({
      streams: s.streams.map((st) =>
        st.id === streamId
          ? { ...st, operations: st.operations.filter((op) => op.id !== opId) }
          : st
      ),
    })),

  updateOperation: (streamId, opId, patch) =>
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
    })),

  reorderOperations: (streamId, fromIndex, toIndex) =>
    set((s) => ({
      streams: s.streams.map((st) => {
        if (st.id !== streamId) return st
        const ops = [...st.operations]
        const [moved] = ops.splice(fromIndex, 1)
        ops.splice(toIndex, 0, moved)
        return { ...st, operations: ops }
      }),
    })),

  addConnector: (name, type) => {
    const id = genId('conn')
    set((s) => ({
      connectors: [...s.connectors, { id, name, type, config: {} }],
    }))
    return id
  },

  removeConnector: (id) =>
    set((s) => ({ connectors: s.connectors.filter((c) => c.id !== id) })),

  updateConnector: (id, patch) =>
    set((s) => ({
      connectors: s.connectors.map((c) => (c.id === id ? { ...c, ...patch } : c)),
    })),

  addEvent: (name) => {
    const id = genId('evt')
    set((s) => ({
      events: [...s.events, { id, name, fields: [] }],
    }))
    return id
  },

  removeEvent: (id) =>
    set((s) => ({ events: s.events.filter((e) => e.id !== id) })),

  updateEvent: (id, patch) =>
    set((s) => ({
      events: s.events.map((e) => (e.id === id ? { ...e, ...patch } : e)),
    })),

  loadPipeline: (state) => set({ ...state }),

  clear: () => set({ ...INITIAL_STATE }),
}))
