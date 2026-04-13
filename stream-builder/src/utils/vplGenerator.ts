import type { PipelineState, ConnectorDef, EventDef, StreamDef, StreamOperation } from '@/types/pipeline'

/** Source map entry: which VPL line range corresponds to a pipeline entity */
export interface SourceMapEntry {
  entityType: 'connector' | 'event' | 'stream'
  entityId: string
  entityName: string
  startLine: number  // 1-based
  endLine: number    // 1-based, inclusive
}

/** Result of VPL generation: code + source map for cursor sync */
export interface VplGenerateResult {
  code: string
  sourceMap: SourceMapEntry[]
}

/** Generate VPL source code from the pipeline state, with source map */
export function generateVplWithMap(state: PipelineState): VplGenerateResult {
  const lines: string[] = []
  const sourceMap: SourceMapEntry[] = []

  for (const conn of state.connectors) {
    const startLine = lines.length + 1
    const block = generateConnector(conn)
    lines.push(block, '')
    const endLine = lines.length - 1  // exclude trailing blank
    sourceMap.push({ entityType: 'connector', entityId: conn.id, entityName: conn.name, startLine, endLine })
  }

  for (const evt of state.events) {
    const startLine = lines.length + 1
    const block = generateEvent(evt)
    lines.push(block, '')
    const endLine = lines.length - 1
    sourceMap.push({ entityType: 'event', entityId: evt.id, entityName: evt.name, startLine, endLine })
  }

  for (const stream of state.streams) {
    const startLine = lines.length + 1
    const block = generateStream(stream)
    lines.push(block, '')
    const endLine = lines.length - 1
    sourceMap.push({ entityType: 'stream', entityId: stream.id, entityName: stream.name, startLine, endLine })
  }

  const code = lines.join('\n').trimEnd() + '\n'
  return { code, sourceMap }
}

/** Generate VPL source code from the pipeline state (backwards-compatible) */
export function generateVpl(state: PipelineState): string {
  return generateVplWithMap(state).code
}

/** Find which entity a given line belongs to */
export function findEntityAtLine(sourceMap: SourceMapEntry[], line: number): SourceMapEntry | undefined {
  return sourceMap.find((e) => line >= e.startLine && line <= e.endLine)
}

/**
 * Build a source map from raw VPL text by scanning for entity declarations.
 * Matches entities to the pipeline state by name to assign correct IDs.
 * Use this when code comes from the server (no client-side source map available).
 */
export function buildSourceMapFromCode(
  code: string,
  state: PipelineState,
): SourceMapEntry[] {
  const lines = code.split('\n')
  const sourceMap: SourceMapEntry[] = []

  let i = 0
  while (i < lines.length) {
    const line = lines[i].trimEnd()
    const trimmed = line.trim()

    // Connector: "connector Name = ..."
    const connMatch = trimmed.match(/^connector\s+(\w+)\s*=/)
    if (connMatch) {
      const startLine = i + 1
      const name = connMatch[1]
      // Find end: scan until we hit a closing ) or next top-level declaration
      let endIdx = i
      if (trimmed.includes('(') && !trimmed.includes(')')) {
        endIdx++
        while (endIdx < lines.length && !lines[endIdx].trim().startsWith(')')) endIdx++
      }
      const conn = state.connectors.find((c) => c.name === name)
      if (conn) {
        sourceMap.push({ entityType: 'connector', entityId: conn.id, entityName: name, startLine, endLine: endIdx + 1 })
      }
      i = endIdx + 1
      continue
    }

    // Event: "event Name:"
    const evtMatch = trimmed.match(/^event\s+(\w+)/)
    if (evtMatch) {
      const startLine = i + 1
      const name = evtMatch[1]
      let endIdx = i + 1
      while (endIdx < lines.length && lines[endIdx].match(/^[\s\t]+\S/)) endIdx++
      const evt = state.events.find((e) => e.name === name)
      if (evt) {
        sourceMap.push({ entityType: 'event', entityId: evt.id, entityName: name, startLine, endLine: endIdx })
      }
      i = endIdx
      continue
    }

    // Stream: "stream Name = ..."
    const streamMatch = trimmed.match(/^stream\s+(\w+)\s*=/)
    if (streamMatch) {
      const startLine = i + 1
      const name = streamMatch[1]
      let endIdx = i + 1
      while (endIdx < lines.length && (lines[endIdx].match(/^[\s\t]+\S/) || lines[endIdx].trim() === '')) {
        if (lines[endIdx].trim() === '' && endIdx + 1 < lines.length && !lines[endIdx + 1].match(/^[\s\t]+\S/)) break
        endIdx++
      }
      // Trim trailing blank lines from the range
      while (endIdx > i && lines[endIdx - 1].trim() === '') endIdx--
      const stream = state.streams.find((s) => s.name === name)
      if (stream) {
        sourceMap.push({ entityType: 'stream', entityId: stream.id, entityName: name, startLine, endLine: endIdx })
      }
      i = endIdx
      continue
    }

    i++
  }

  return sourceMap
}

function generateConnector(conn: ConnectorDef): string {
  const params = Object.entries(conn.config)
    .filter(([_, v]) => v)
    .map(([k, v]) => {
      const val = /^\d+$/.test(v) ? v : `"${v}"`
      return `    ${k}: ${val}`
    })
    .join(',\n')

  if (params) {
    return `connector ${conn.name} = ${conn.type} (\n${params}\n)`
  }
  return `connector ${conn.name} = ${conn.type}()`
}

function generateEvent(evt: EventDef): string {
  const lines = [`event ${evt.name}${evt.extends ? ` extends ${evt.extends}` : ''}:`]
  for (const field of evt.fields) {
    const optSuffix = field.optional ? '?' : ''
    lines.push(`    ${field.name}: ${field.type}${optSuffix}`)
  }
  if (evt.fields.length === 0) {
    lines.push('    pass')
  }
  return lines.join('\n')
}

function generateStream(stream: StreamDef): string {
  if (!stream.source && stream.operations.length === 0) {
    return `# stream ${stream.name} (empty)`
  }

  const lines: string[] = []
  const source = stream.source || 'UnknownSource'

  let sourceLine: string
  if (stream.sourceType === 'merge' && stream.sourceRefs.length > 0) {
    sourceLine = `stream ${stream.name} = merge(${stream.sourceRefs.join(', ')})`
  } else if (stream.sourceType === 'join' && stream.sourceRefs.length > 0) {
    sourceLine = `stream ${stream.name} = join(${stream.sourceRefs.join(', ')})`
  } else {
    sourceLine = `stream ${stream.name} = ${source}`
  }

  lines.push(sourceLine)

  for (const op of stream.operations) {
    lines.push(generateOperation(op))
  }

  return lines.join('\n')
}

function generateOperation(op: StreamOperation): string {
  const indent = '    '
  const val = op.value.trim()

  switch (op.type) {
    case 'where':
    case 'having':
    case 'filter':
      return `${indent}.${op.type}(${val})`

    case 'partition_by':
      return `${indent}.partition_by(${val})`

    case 'window':
      return `${indent}.window(${val})`

    case 'aggregate':
      if (val.includes(',')) {
        const fields = val.split(',').map((f) => f.trim())
        return `${indent}.aggregate(\n${fields.map((f) => `${indent}    ${f}`).join(',\n')}\n${indent})`
      }
      return `${indent}.aggregate(${val})`

    case 'emit':
      if (val.includes(',')) {
        const fields = val.split(',').map((f) => f.trim())
        return `${indent}.emit(\n${fields.map((f) => `${indent}    ${f}`).join(',\n')}\n${indent})`
      }
      return `${indent}.emit(${val})`

    case 'select':
      return `${indent}.select(${val})`

    case 'to':
      return `${indent}.to(${val})`

    case 'from':
      return `${indent}.from(${val})`

    case 'within':
      return `${indent}.within(${val})`

    case 'distinct':
      return val ? `${indent}.distinct(${val})` : `${indent}.distinct()`

    case 'limit':
      return `${indent}.limit(${val})`

    case 'order_by':
      return `${indent}.order_by(${val})`

    case 'map':
      return `${indent}.map(${val})`

    case 'forecast':
      return `${indent}.forecast(${val})`

    case 'enrich':
      return `${indent}.enrich(${val})`

    case 'score':
      return `${indent}.score(${val})`

    case 'trend_aggregate':
      return `${indent}.trend_aggregate(${val})`

    case 'alert':
      return `${indent}.alert(${val})`

    case 'tap':
      return `${indent}.tap(${val})`

    case 'log':
      return `${indent}.log(${val})`

    case 'print':
      return val ? `${indent}.print(${val})` : `${indent}.print()`

    default:
      return `${indent}.${op.type}(${val})`
  }
}
