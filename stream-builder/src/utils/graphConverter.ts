import type { StreamDef, StreamOperation, ConnectorDef, EventDef } from '@/types/pipeline'
import type { GraphNode, GraphEdge } from '@/api/varpulisClient'

// =============================================================================
// Server Graph → PipelineState (streams only)
// =============================================================================

/**
 * Convert a server pipeline graph (nodes + edges) into StreamDef[].
 *
 * The server graph only contains stream declarations (not connector/event defs),
 * so connectors and events must be parsed separately (client-side).
 */
export function graphToStreams(nodes: GraphNode[], edges: GraphEdge[]): StreamDef[] {
  const successors = new Map<string, string[]>()
  const hasIncoming = new Set<string>()

  for (const edge of edges) {
    const targets = successors.get(edge.source) ?? []
    targets.push(edge.target)
    successors.set(edge.source, targets)
    hasIncoming.add(edge.target)
  }

  const nodeMap = new Map<string, GraphNode>(nodes.map((n) => [n.id, n]))
  const sourceNodes = nodes.filter((n) => !hasIncoming.has(n.id))

  let idCounter = 1
  const nextId = (prefix: string) => `${prefix}_g${idCounter++}`

  const streams: StreamDef[] = []

  for (const sourceNode of sourceNodes) {
    const c = sourceNode.config as Record<string, unknown>
    const streamName = str(c.stream_name) || 'Stream'

    // Determine source info
    let source = ''
    let sourceType: StreamDef['sourceType'] = 'stream'
    const sourceRefs: string[] = []
    const operations: StreamOperation[] = []

    const sType = str(c.source_type)
    if (sType === 'merge') {
      sourceType = 'merge'
      source = 'merge'
      for (const s of arr(c.streams)) sourceRefs.push(str(s.source) || str(s.name) || '')
    } else if (sType === 'join') {
      sourceType = 'join'
      source = 'join'
      for (const s of arr(c.clauses)) sourceRefs.push(str(s.source) || str(s.name) || '')
    } else if (sType === 'sequence') {
      sourceType = 'sequence'
      source = 'sequence'
    } else if (sType === 'timer') {
      sourceType = 'timer'
      source = 'timer'
    } else {
      source = str(c.event_type) || ''
      // If source has a connector, add a .from() operation
      const connector = str(c.connector)
      if (connector) {
        const params = arr(c.params)
          .map((p) => `${str(p.name)}: ${str(p.value)}`)
          .filter((s) => s !== ': ')
        const fromValue = params.length > 0 ? `${connector}, ${params.join(', ')}` : connector
        operations.push({ id: nextId('op'), type: 'from', value: fromValue, expanded: false })
      }
    }

    // Walk the operator chain
    let currentId = sourceNode.id
    while (true) {
      const targets = successors.get(currentId)
      if (!targets || targets.length === 0) break
      const nextNodeId = targets[0]
      const node = nodeMap.get(nextNodeId)
      if (!node) break

      const op = nodeToOperation(node, nextId)
      if (op) operations.push(op)
      currentId = nextNodeId
    }

    streams.push({
      id: nextId('stream'),
      name: streamName,
      source,
      sourceType,
      sourceRefs,
      operations,
    })
  }

  return streams
}

function nodeToOperation(
  node: GraphNode,
  nextId: (prefix: string) => string,
): StreamOperation | null {
  const c = node.config as Record<string, unknown>

  switch (node.node_type) {
    case 'filter': {
      const type = node.label.startsWith('.filter') ? 'filter' : 'where'
      return { id: nextId('op'), type, value: str(c.expr) || '', expanded: false }
    }
    case 'having':
      return { id: nextId('op'), type: 'having', value: str(c.expr) || '', expanded: false }
    case 'partition':
      return { id: nextId('op'), type: 'partition_by', value: str(c.expr) || '', expanded: false }
    case 'window': {
      const parts: string[] = []
      if (c.session_gap) {
        parts.push('session', str(c.session_gap) || '')
      } else if (c.sliding) {
        parts.push('sliding', str(c.duration) || '', str(c.sliding) || '')
      } else {
        parts.push('tumbling', str(c.duration) || '')
      }
      return { id: nextId('op'), type: 'window', value: parts.join(', '), expanded: false }
    }
    case 'aggregate': {
      const items = arr(c.items).map((i) => `${str(i.expr)} as ${str(i.alias)}`)
      return { id: nextId('op'), type: 'aggregate', value: items.join(', '), expanded: false }
    }
    case 'select': {
      const fields = (c.fields as string[]) ?? []
      return { id: nextId('op'), type: 'select', value: fields.join(', '), expanded: false }
    }
    case 'emit': {
      const fields = arr(c.fields).map((f) => `${str(f.name)} = ${str(f.expr)}`)
      return { id: nextId('op'), type: 'emit', value: fields.join(', '), expanded: false }
    }
    case 'sink':
      return { id: nextId('op'), type: 'to', value: str(c.connector) || str(c.expr) || '', expanded: false }
    case 'alert': {
      const args = arr(c.args).map((a) => `${str(a.name)} = ${str(a.expr)}`)
      return { id: nextId('op'), type: 'alert', value: args.join(', '), expanded: false }
    }
    case 'limit':
      return { id: nextId('op'), type: 'limit', value: str(c.expr) || '', expanded: false }
    case 'order': {
      const items = (c.items as string[]) ?? []
      return { id: nextId('op'), type: 'order_by', value: items.join(', '), expanded: false }
    }
    case 'distinct':
      return { id: nextId('op'), type: 'distinct', value: str(c.expr) || '', expanded: false }
    case 'map':
      return { id: nextId('op'), type: 'map', value: str(c.expr) || '', expanded: false }
    case 'sequence_step': {
      const evtType = str(c.event_type) || ''
      const alias = str(c.alias)
      return { id: nextId('op'), type: 'followed_by', value: alias ? `${evtType} as ${alias}` : evtType, expanded: false }
    }
    case 'within':
      return { id: nextId('op'), type: 'within', value: str(c.expr) || '', expanded: false }
    case 'not':
      return { id: nextId('op'), type: 'not', value: str(c.event_type) || '', expanded: false }
    case 'forecast':
      return { id: nextId('op'), type: 'forecast', value: '', expanded: false }
    case 'score':
      return { id: nextId('op'), type: 'score', value: str(c.model) || '', expanded: false }
    case 'enrich':
      return { id: nextId('op'), type: 'enrich', value: '', expanded: false }
    case 'trend_aggregate':
      return { id: nextId('op'), type: 'trend_aggregate', value: '', expanded: false }
    case 'tap':
      return { id: nextId('op'), type: 'tap', value: '', expanded: false }
    case 'log': {
      const fields = (c.fields as string[]) ?? []
      return { id: nextId('op'), type: 'log', value: fields.join(', '), expanded: false }
    }
    case 'print':
      return { id: nextId('op'), type: 'print', value: str(c.expr) || '', expanded: false }
    default:
      return null
  }
}

// =============================================================================
// PipelineState → Server Graph
// =============================================================================

/**
 * Convert pipeline streams to a graph suitable for POST /pipeline/generate.
 * Only streams are converted (connectors/events are handled separately).
 */
export function stateToGraph(streams: StreamDef[]): { nodes: GraphNode[]; edges: GraphEdge[] } {
  const nodes: GraphNode[] = []
  const edges: GraphEdge[] = []
  let nc = 0

  for (let si = 0; si < streams.length; si++) {
    const stream = streams[si]
    if (!stream.source && stream.operations.length === 0) continue

    const x = si * 300
    let y = 0

    // Source node
    const sourceId = `n${nc++}`
    const sourceConfig = buildSourceConfig(stream)
    nodes.push({
      id: sourceId,
      label: `${stream.name} = ${stream.source}`,
      node_type: 'source',
      config: sourceConfig,
    })
    y += 100

    let prevId = sourceId

    // Skip .from() ops — they're folded into the source node config
    const ops = stream.operations.filter((op) => op.type !== 'from')

    for (const op of ops) {
      const opId = `n${nc++}`
      const { nodeType, config } = buildOpNodeInfo(op)
      nodes.push({
        id: opId,
        label: `.${op.type}(${op.value})`,
        node_type: nodeType,
        config,
        position: [x, y],
      })
      edges.push({ id: `e${prevId}-${opId}`, source: prevId, target: opId })
      prevId = opId
      y += 100
    }
  }

  return { nodes, edges }
}

function buildSourceConfig(stream: StreamDef): Record<string, unknown> {
  const config: Record<string, unknown> = { stream_name: stream.name }

  switch (stream.sourceType) {
    case 'merge':
      config.source_type = 'merge'
      config.streams = stream.sourceRefs.map((r) => ({ name: r, source: r }))
      break
    case 'join':
      config.source_type = 'join'
      config.clauses = stream.sourceRefs.map((r) => ({ name: r, source: r }))
      break
    case 'sequence':
      config.source_type = 'sequence'
      config.steps = []
      break
    case 'timer':
      config.source_type = 'timer'
      break
    default:
      config.event_type = stream.source
      // Check for .from() operation to embed connector in source
      const fromOp = stream.operations.find((op) => op.type === 'from')
      if (fromOp && fromOp.value) {
        const parts = fromOp.value.split(',').map((s) => s.trim())
        config.connector = parts[0]
        if (parts.length > 1) {
          config.params = parts.slice(1).map((p) => {
            const [name, ...rest] = p.split(':')
            return { name: name.trim(), value: rest.join(':').trim().replace(/^"|"$/g, '') }
          })
        }
      }
  }

  return config
}

function buildOpNodeInfo(op: StreamOperation): { nodeType: string; config: Record<string, unknown> } {
  const val = op.value.trim()

  switch (op.type) {
    case 'where':
    case 'filter':
      return { nodeType: 'filter', config: { expr: val } }
    case 'having':
      return { nodeType: 'having', config: { expr: val } }
    case 'partition_by':
      return { nodeType: 'partition', config: { expr: val } }
    case 'window': {
      const parts = val.split(',').map((p) => p.trim())
      const config: Record<string, unknown> = {}
      if (parts[0] === 'session') {
        config.session_gap = parts[1] ?? ''
        config.duration = parts[1] ?? ''
      } else if (parts[0] === 'sliding') {
        config.duration = parts[1] ?? ''
        config.sliding = parts[2] ?? ''
      } else {
        config.duration = parts.length > 1 ? parts[1] : parts[0]
      }
      return { nodeType: 'window', config }
    }
    case 'aggregate': {
      const items = val.split(',').map((part) => {
        const m = part.trim().match(/^(.+?)\s+as\s+(\w+)$/)
        return m ? { expr: m[1].trim(), alias: m[2] } : { expr: part.trim(), alias: part.trim() }
      })
      return { nodeType: 'aggregate', config: { items } }
    }
    case 'select':
      return { nodeType: 'select', config: { fields: val.split(',').map((f) => f.trim()) } }
    case 'emit': {
      const fields = val.split(',').map((part) => {
        const m = part.trim().match(/^(\w+)\s*=\s*(.+)$/)
        return m ? { name: m[1], expr: m[2].trim() } : { name: part.trim(), expr: part.trim() }
      })
      return { nodeType: 'emit', config: { fields } }
    }
    case 'to':
      return { nodeType: 'sink', config: { connector: val } }
    case 'alert': {
      const args = val.split(',').map((part) => {
        const m = part.trim().match(/^(\w+)\s*=\s*(.+)$/)
        return m ? { name: m[1], expr: m[2].trim() } : { name: part.trim(), expr: part.trim() }
      })
      return { nodeType: 'alert', config: { args } }
    }
    case 'limit':
      return { nodeType: 'limit', config: { expr: val } }
    case 'order_by':
      return { nodeType: 'order', config: { items: val.split(',').map((f) => f.trim()) } }
    case 'distinct':
      return { nodeType: 'distinct', config: { expr: val || null } }
    case 'map':
      return { nodeType: 'map', config: { expr: val } }
    case 'followed_by':
      return { nodeType: 'sequence_step', config: { event_type: val } }
    case 'within':
      return { nodeType: 'within', config: { expr: val } }
    case 'not':
      return { nodeType: 'not', config: { event_type: val } }
    case 'forecast':
      return { nodeType: 'forecast', config: {} }
    case 'score':
      return { nodeType: 'score', config: { model: val } }
    case 'enrich':
      return { nodeType: 'enrich', config: {} }
    case 'trend_aggregate':
      return { nodeType: 'trend_aggregate', config: {} }
    case 'tap':
      return { nodeType: 'tap', config: {} }
    case 'log':
      return { nodeType: 'log', config: { fields: val ? val.split(',').map((f) => f.trim()) : [] } }
    case 'print':
      return { nodeType: 'print', config: { expr: val || null } }
    default:
      return { nodeType: op.type, config: { expr: val } }
  }
}

// =============================================================================
// Generate connector + event VPL (client-side, for combining with server streams)
// =============================================================================

/** Generate VPL for connectors and events only (streams come from server). */
export function generateConnEvtVpl(connectors: ConnectorDef[], events: EventDef[]): string {
  const lines: string[] = []

  for (const conn of connectors) {
    const params = Object.entries(conn.config)
      .filter(([, v]) => v)
      .map(([k, v]) => {
        const val = /^\d+$/.test(v) ? v : `"${v}"`
        return `    ${k}: ${val}`
      })
      .join(',\n')
    lines.push(params ? `connector ${conn.name} = ${conn.type} (\n${params}\n)` : `connector ${conn.name} = ${conn.type}()`)
    lines.push('')
  }

  for (const evt of events) {
    const evtLines = [`event ${evt.name}${evt.extends ? ` extends ${evt.extends}` : ''}:`]
    for (const field of evt.fields) {
      evtLines.push(`    ${field.name}: ${field.type}${field.optional ? '?' : ''}`)
    }
    if (evt.fields.length === 0) evtLines.push('    pass')
    lines.push(evtLines.join('\n'))
    lines.push('')
  }

  return lines.join('\n')
}

// =============================================================================
// Helpers
// =============================================================================

/* eslint-disable @typescript-eslint/no-explicit-any */
function str(v: unknown): string {
  return typeof v === 'string' ? v : ''
}

function arr(v: unknown): any[] {
  return Array.isArray(v) ? v : []
}
