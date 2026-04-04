import type { PipelineState, ConnectorDef, EventDef, StreamDef, StreamOperation, EventField } from '@/types/pipeline'

/**
 * Parse VPL source code into a PipelineState.
 * This is a lightweight client-side parser that handles the common cases.
 * For authoritative parsing, use the server API (POST /api/v1/pipeline/graph).
 */
export function parseVpl(source: string): PipelineState {
  const lines = source.split('\n')
  const connectors: ConnectorDef[] = []
  const events: EventDef[] = []
  const streams: StreamDef[] = []

  let i = 0
  let idCounter = 1
  const nextId = (prefix: string) => `${prefix}_${idCounter++}`

  while (i < lines.length) {
    const line = lines[i].trim()

    // Skip empty lines and comments
    if (!line || line.startsWith('#')) {
      i++
      continue
    }

    // Connector declaration
    if (line.startsWith('connector ')) {
      const { connector, endLine } = parseConnector(lines, i, nextId)
      if (connector) connectors.push(connector)
      i = endLine + 1
      continue
    }

    // Event declaration
    if (line.startsWith('event ')) {
      const { event, endLine } = parseEvent(lines, i, nextId)
      if (event) events.push(event)
      i = endLine + 1
      continue
    }

    // Stream declaration
    if (line.startsWith('stream ')) {
      const { stream, endLine } = parseStream(lines, i, nextId)
      if (stream) streams.push(stream)
      i = endLine + 1
      continue
    }

    // Pattern declaration (treat like stream for now)
    if (line.startsWith('pattern ')) {
      // Skip pattern blocks for now
      i++
      while (i < lines.length && (lines[i].startsWith('    ') || lines[i].startsWith('\t') || lines[i].trim() === '')) {
        i++
      }
      continue
    }

    i++
  }

  return { connectors, events, streams }
}

function parseConnector(
  lines: string[],
  startLine: number,
  nextId: (prefix: string) => string
): { connector: ConnectorDef | null; endLine: number } {
  const firstLine = lines[startLine].trim()

  // connector Name = type (...)
  const match = firstLine.match(/^connector\s+(\w+)\s*=\s*(\w+)\s*\(?\s*$/)
  if (!match) return { connector: null, endLine: startLine }

  const name = match[1]
  const type = match[2]
  const config: Record<string, string> = {}

  let i = startLine
  // Check if params are on the same line
  const singleLineMatch = firstLine.match(/^connector\s+\w+\s*=\s*\w+\s*\((.*)\)\s*$/)
  if (singleLineMatch) {
    parseConfigParams(singleLineMatch[1], config)
    return { connector: { id: nextId('conn'), name, type, config }, endLine: i }
  }

  // Multi-line: collect until closing )
  i++
  while (i < lines.length) {
    const line = lines[i].trim()
    if (line === ')' || line === ')') {
      break
    }
    parseConfigParams(line, config)
    i++
  }

  return { connector: { id: nextId('conn'), name, type, config }, endLine: i }
}

function parseConfigParams(text: string, config: Record<string, string>) {
  // Parse "key: value" or "key: \"value\"" pairs
  const pairs = text.split(',')
  for (const pair of pairs) {
    const m = pair.trim().match(/^(\w+)\s*:\s*(.+)$/)
    if (m) {
      let val = m[2].trim()
      // Remove trailing comma
      val = val.replace(/,\s*$/, '')
      // Remove quotes
      val = val.replace(/^"(.*)"$/, '$1')
      config[m[1]] = val
    }
  }
}

function parseEvent(
  lines: string[],
  startLine: number,
  nextId: (prefix: string) => string
): { event: EventDef | null; endLine: number } {
  const firstLine = lines[startLine].trim()

  // event Name: or event Name extends Base:
  const match = firstLine.match(/^event\s+(\w+)(?:\s+extends\s+(\w+))?\s*:?\s*$/)
  if (!match) return { event: null, endLine: startLine }

  const name = match[1]
  const extendsType = match[2]
  const fields: EventField[] = []

  let i = startLine + 1
  while (i < lines.length) {
    const line = lines[i]
    // Stop at non-indented line or empty line followed by non-indented
    if (!line.match(/^[\s\t]+\S/)) break

    const trimmed = line.trim()
    if (trimmed === 'pass') { i++; continue }

    // field_name: type or field_name: type?
    const fieldMatch = trimmed.match(/^(\w+)\s*:\s*(\S+?)(\?)?\s*$/)
    if (fieldMatch) {
      fields.push({
        name: fieldMatch[1],
        type: fieldMatch[2],
        optional: !!fieldMatch[3],
      })
    }
    i++
  }

  return {
    event: { id: nextId('evt'), name, fields, extends: extendsType },
    endLine: i - 1,
  }
}

function parseStream(
  lines: string[],
  startLine: number,
  nextId: (prefix: string) => string
): { stream: StreamDef | null; endLine: number } {
  const firstLine = lines[startLine].trim()

  // stream Name = Source
  // stream Name = Source as alias
  // stream Name = merge(A, B, C)
  // stream Name = join(A, B)
  const match = firstLine.match(/^stream\s+(\w+)\s*=\s*(.+)$/)
  if (!match) return { stream: null, endLine: startLine }

  const name = match[1]
  let sourceExpr = match[2].trim()

  let sourceType: StreamDef['sourceType'] = 'stream'
  let source = ''
  const sourceRefs: string[] = []

  // Check for merge/join
  const mergeMatch = sourceExpr.match(/^merge\s*\((.+)\)\s*$/)
  const joinMatch = sourceExpr.match(/^join\s*\((.+)\)\s*$/)

  if (mergeMatch) {
    sourceType = 'merge'
    source = 'merge'
    sourceRefs.push(...mergeMatch[1].split(',').map((s) => s.trim()))
  } else if (joinMatch) {
    sourceType = 'join'
    source = 'join'
    sourceRefs.push(...joinMatch[1].split(',').map((s) => s.trim()))
  } else {
    // Simple source: could be "EventType" or "EventType as alias" or "StreamName"
    // May also have inline "where" for sequence: "EventType where cond as alias"
    const parts = sourceExpr.split(/\s+/)
    source = parts[0]
    // Check if it's a sequence (has -> on continuation lines)
    sourceType = 'stream' // will be refined below
  }

  // Collect operations from continuation lines
  const operations: StreamOperation[] = []
  let i = startLine + 1

  while (i < lines.length) {
    const line = lines[i]
    // Must be indented (part of this stream)
    if (!line.match(/^[\s\t]+\S/) && line.trim() !== '') break
    if (line.trim() === '') { i++; continue }

    const trimmed = line.trim()

    // Handle -> (followed_by) as a special case
    if (trimmed.startsWith('->')) {
      operations.push({
        id: nextId('op'),
        type: 'followed_by',
        value: trimmed.substring(2).trim(),
        expanded: false,
      })
      i++
      continue
    }

    // Parse .operator(...) — may span multiple lines
    const opMatch = trimmed.match(/^\.(\w+)\s*\((.*)$/)
    if (opMatch) {
      const opType = opMatch[1]
      let opValue = opMatch[2]

      // Check if the paren closes on this line
      if (!isBalanced(opValue)) {
        // Multi-line: collect until balanced
        i++
        while (i < lines.length) {
          const contLine = lines[i].trim()
          opValue += '\n' + contLine
          if (isBalanced(opValue)) break
          i++
        }
      }

      // Remove trailing )
      opValue = opValue.replace(/\)\s*$/, '').trim()

      operations.push({
        id: nextId('op'),
        type: opType,
        value: cleanValue(opValue),
        expanded: false,
      })
      i++
      continue
    }

    // Continuation of previous expression (e.g., multi-line "and")
    if (operations.length > 0 && (trimmed.startsWith('and ') || trimmed.startsWith('or '))) {
      const lastOp = operations[operations.length - 1]
      lastOp.value += ' ' + trimmed
      i++
      continue
    }

    i++
  }

  return {
    stream: { id: nextId('stream'), name, source, sourceType, sourceRefs, operations },
    endLine: i - 1,
  }
}

function isBalanced(text: string): boolean {
  let depth = 0
  for (const ch of text) {
    if (ch === '(') depth++
    if (ch === ')') depth--
  }
  return depth <= 0
}

function cleanValue(value: string): string {
  // Flatten multi-line values into comma-separated single line
  return value
    .split('\n')
    .map((l) => l.trim())
    .filter((l) => l)
    .join(', ')
    .replace(/,\s*,/g, ',')
    .replace(/^,\s*/, '')
    .replace(/,\s*$/, '')
}
