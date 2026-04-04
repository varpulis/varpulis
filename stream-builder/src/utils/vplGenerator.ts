import type { PipelineState, ConnectorDef, EventDef, StreamDef, StreamOperation } from '@/types/pipeline'

/** Generate VPL source code from the pipeline state */
export function generateVpl(state: PipelineState): string {
  const lines: string[] = []

  // Connectors
  for (const conn of state.connectors) {
    lines.push(generateConnector(conn))
    lines.push('')
  }

  // Event types
  for (const evt of state.events) {
    lines.push(generateEvent(evt))
    lines.push('')
  }

  // Streams
  for (const stream of state.streams) {
    lines.push(generateStream(stream))
    lines.push('')
  }

  return lines.join('\n').trimEnd() + '\n'
}

function generateConnector(conn: ConnectorDef): string {
  const params = Object.entries(conn.config)
    .filter(([_, v]) => v)
    .map(([k, v]) => {
      // Quote strings, leave numbers as-is
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

  // Handle different source types
  let sourceLine: string
  if (stream.sourceType === 'merge' && stream.sourceRefs.length > 0) {
    sourceLine = `stream ${stream.name} = merge(${stream.sourceRefs.join(', ')})`
  } else if (stream.sourceType === 'join' && stream.sourceRefs.length > 0) {
    sourceLine = `stream ${stream.name} = join(${stream.sourceRefs.join(', ')})`
  } else {
    sourceLine = `stream ${stream.name} = ${source}`
  }

  lines.push(sourceLine)

  // Operations
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
      // Multi-line aggregate
      if (val.includes(',')) {
        const fields = val.split(',').map((f) => f.trim())
        return `${indent}.aggregate(\n${fields.map((f) => `${indent}    ${f}`).join(',\n')}\n${indent})`
      }
      return `${indent}.aggregate(${val})`

    case 'emit':
      // Multi-line emit
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
