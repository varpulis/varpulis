import {
  FlowNode,
  FlowEdge,
  ConnectorNodeData,
  SourceNodeData,
  SinkNodeData,
  StreamNodeData,
  EventNodeData,
  PatternNodeData,
  EmitNodeData,
  ConnectorType,
  OperationType,
} from '../types/flow';
import dagre from 'dagre';

interface ParseResult {
  nodes: FlowNode[];
  edges: FlowEdge[];
}

let nodeCounter = 0;

function nextId(prefix: string): string {
  return `${prefix}-${nodeCounter++}`;
}

/**
 * Parse VPL code into flow graph nodes and edges.
 *
 * Handles real VPL syntax:
 *   connector Name = type(...)
 *   event Name:\n    field: type
 *   stream Name = EventType.from(Connector, topic: "...")
 *   stream Name = EventA as a -> EventB as b .within(5m)
 *   stream Name = merge(A, B)
 *   .where(...) .window(...) .aggregate(...) .forecast(...) .emit(...) .to(...)
 */
export function parseVpl(code: string): ParseResult {
  nodeCounter = 0;
  const nodes: FlowNode[] = [];
  const edges: FlowEdge[] = [];

  // Strip comments and join continuation lines
  const blocks = preprocess(code);

  // Track declared names → node ids for edge linking
  const connectorIds = new Map<string, string>();
  const eventIds = new Map<string, string>();
  const streamIds = new Map<string, string>();

  for (const block of blocks) {
    const trimmed = block.trim();
    if (!trimmed) continue;

    if (trimmed.startsWith('connector ')) {
      parseConnector(trimmed, nodes, connectorIds);
    } else if (trimmed.startsWith('event ')) {
      parseEvent(trimmed, nodes, eventIds);
    } else if (trimmed.startsWith('stream ')) {
      parseStream(trimmed, nodes, edges, connectorIds, eventIds, streamIds);
    }
  }

  return applyDagreLayout({ nodes, edges });
}

// ============================================
// Preprocessor: strip comments, join continuations
// ============================================

function preprocess(code: string): string[] {
  const rawLines = code.split('\n');
  const cleaned: string[] = [];

  // Strip line comments (# ...) but keep strings intact
  for (const line of rawLines) {
    let inStr = false;
    let result = '';
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"' && (i === 0 || line[i - 1] !== '\\')) {
        inStr = !inStr;
      }
      if (ch === '#' && !inStr) {
        break;
      }
      result += ch;
    }
    cleaned.push(result);
  }

  // Join continuation lines: lines starting with whitespace followed by
  // `.` or `->` or field definitions belong to the previous declaration
  const blocks: string[] = [];
  let current = '';

  for (const line of cleaned) {
    const stripped = line.trimStart();
    if (!stripped) {
      // Blank line — flush
      if (current.trim()) {
        blocks.push(current);
        current = '';
      }
      continue;
    }

    const isTopLevel =
      stripped.startsWith('connector ') ||
      stripped.startsWith('event ') ||
      stripped.startsWith('stream ');

    if (isTopLevel && current.trim()) {
      blocks.push(current);
      current = line;
    } else if (!isTopLevel && current.trim()) {
      // Continuation line
      current += '\n' + line;
    } else {
      current = line;
    }
  }
  if (current.trim()) {
    blocks.push(current);
  }

  return blocks;
}

// ============================================
// Connector: connector Name = type(params)
// ============================================

function parseConnector(
  block: string,
  nodes: FlowNode[],
  connectorIds: Map<string, string>
): void {
  const match = block.match(/^connector\s+(\w+)\s*=\s*(\w+)\s*\(/);
  if (!match) return;

  const name = match[1];
  const type = match[2].toLowerCase() as ConnectorType;
  const nodeId = nextId('connector');

  const data: ConnectorNodeData = {
    label: name,
    connectorType: type,
  };

  // Extract key-value params from the block
  const params = extractParams(block);

  switch (type) {
    case 'mqtt':
      if (params.host) data.broker = params.host;
      if (params.port) data.port = parseInt(params.port);
      if (params.client_id) data.clientId = params.client_id;
      break;
    case 'kafka':
      if (params.brokers) data.brokers = params.brokers;
      break;
    case 'nats':
      if (params.url) data.natsUrl = params.url;
      break;
    case 'http':
      if (params.base_url) data.baseUrl = params.base_url;
      break;
    case 'file':
      if (params.path) data.basePath = params.path;
      break;
  }

  nodes.push({
    id: nodeId,
    type: 'connector',
    position: { x: 0, y: 0 },
    data,
  });

  connectorIds.set(name, nodeId);
}

// ============================================
// Event: event Name:\n    field: type
// ============================================

function parseEvent(
  block: string,
  nodes: FlowNode[],
  eventIds: Map<string, string>
): void {
  const firstLine = block.split('\n')[0];
  const match = firstLine.match(/^event\s+(\w+)(?:\s+extends\s+(\w+))?/);
  if (!match) return;

  const name = match[1];
  const extendsType = match[2];
  const nodeId = nextId('event');

  const fields: { name: string; type: string; optional?: boolean }[] = [];
  const lines = block.split('\n').slice(1);

  for (const line of lines) {
    const fieldMatch = line.trim().match(/^(\w+)\s*:\s*(\w+)(\?)?/);
    if (fieldMatch) {
      fields.push({
        name: fieldMatch[1],
        type: fieldMatch[2],
        optional: fieldMatch[3] === '?',
      });
    }
  }

  const data: EventNodeData = {
    label: name,
    fields: fields.map((f) => ({
      name: f.name,
      type: f.type as EventNodeData['fields'][0]['type'],
      optional: f.optional,
    })),
  };
  if (extendsType) data.extends = extendsType;

  nodes.push({
    id: nodeId,
    type: 'event',
    position: { x: 0, y: 0 },
    data,
  });

  eventIds.set(name, nodeId);
}

// ============================================
// Stream: stream Name = ...
// ============================================

function parseStream(
  block: string,
  nodes: FlowNode[],
  edges: FlowEdge[],
  connectorIds: Map<string, string>,
  eventIds: Map<string, string>,
  streamIds: Map<string, string>
): void {
  const firstLine = block.split('\n')[0];
  const nameMatch = firstLine.match(/^stream\s+(\w+)\s*=/);
  if (!nameMatch) return;

  const streamName = nameMatch[1];

  // Flatten block into single line for analysis (keeping spaces)
  const flat = block.replace(/\n\s*/g, ' ').trim();
  const afterEquals = flat.substring(flat.indexOf('=') + 1).trim();

  // Detect merge()
  const mergeMatch = afterEquals.match(/^merge\s*\(([^)]+)\)/);
  if (mergeMatch) {
    parseMergeStream(streamName, mergeMatch[1], block, nodes, edges, streamIds);
    return;
  }

  // Detect sequence pattern (contains ` -> ` between event refs, before any dot-op)
  const dotOpIdx = findFirstDotOp(afterEquals);
  const beforeOps = dotOpIdx >= 0 ? afterEquals.substring(0, dotOpIdx) : afterEquals;

  if (beforeOps.includes(' -> ')) {
    parseSequenceStream(streamName, afterEquals, block, nodes, edges, connectorIds, eventIds, streamIds);
    return;
  }

  // Simple stream: EventType.from(...).where(...)...
  parseSimpleStream(streamName, afterEquals, block, nodes, edges, connectorIds, eventIds, streamIds);
}

/** Find index of first `.word(` that's a known operation, skipping `.from(` */
function findFirstDotOp(str: string): number {
  const ops = [
    '.where(', '.window(', '.aggregate(', '.partition_by(', '.order_by(',
    '.limit(', '.distinct(', '.select(', '.map(', '.filter(',
    '.emit(', '.to(', '.forecast(', '.trend_aggregate(', '.tap(',
  ];
  let earliest = -1;
  for (const op of ops) {
    const idx = str.indexOf(op);
    if (idx >= 0 && (earliest < 0 || idx < earliest)) {
      earliest = idx;
    }
  }
  return earliest;
}

// ============================================
// Simple stream: EventType.from(Connector, ...).where(...)
// ============================================

function parseSimpleStream(
  streamName: string,
  afterEquals: string,
  _block: string,
  nodes: FlowNode[],
  edges: FlowEdge[],
  connectorIds: Map<string, string>,
  eventIds: Map<string, string>,
  streamIds: Map<string, string>
): void {
  // Extract event type (first word)
  const eventTypeMatch = afterEquals.match(/^(\w+)/);
  const eventTypeName = eventTypeMatch ? eventTypeMatch[1] : 'Event';

  // Extract .from(ConnectorName, topic: "...")
  let connectorName: string | undefined;
  let topic: string | undefined;
  const fromMatch = afterEquals.match(/\.from\s*\(\s*(\w+)(?:\s*,\s*topic\s*:\s*"([^"]*)")?\s*\)/);
  if (fromMatch) {
    connectorName = fromMatch[1];
    topic = fromMatch[2];
  }

  // Create source node
  const sourceId = nextId('source');
  const sourceData: SourceNodeData = {
    label: streamName,
    eventType: eventTypeName,
    topic,
  };
  nodes.push({
    id: sourceId,
    type: 'source',
    position: { x: 0, y: 0 },
    data: sourceData,
  });
  streamIds.set(streamName, sourceId);

  // Edge from connector
  if (connectorName && connectorIds.has(connectorName)) {
    edges.push({
      id: nextId('edge'),
      source: connectorIds.get(connectorName)!,
      target: sourceId,
      targetHandle: 'connector-in',
    });
  }

  // Edge from event type
  if (eventIds.has(eventTypeName)) {
    edges.push({
      id: nextId('edge'),
      source: eventIds.get(eventTypeName)!,
      target: sourceId,
      targetHandle: 'event-in',
    });
  }

  // Parse pipeline operations
  const ops = parseOperations(afterEquals);
  let lastNodeId = sourceId;

  if (ops.streamOps.length > 0) {
    const streamId = nextId('stream');
    const streamData: StreamNodeData = {
      label: streamName,
      operations: ops.streamOps,
    };
    nodes.push({
      id: streamId,
      type: 'stream',
      position: { x: 0, y: 0 },
      data: streamData,
    });
    edges.push({ id: nextId('edge'), source: lastNodeId, target: streamId });
    lastNodeId = streamId;
  }

  if (ops.emitFields) {
    const emitId = nextId('emit');
    const emitData: EmitNodeData = {
      label: 'Output',
      fields: ops.emitFields,
      emitAll: ops.emitFields.length === 0,
    };
    nodes.push({
      id: emitId,
      type: 'emit',
      position: { x: 0, y: 0 },
      data: emitData,
    });
    edges.push({ id: nextId('edge'), source: lastNodeId, target: emitId });
    lastNodeId = emitId;
  }

  if (ops.sinkConnector) {
    const sinkId = nextId('sink');
    const sinkData: SinkNodeData = {
      label: ops.sinkConnector,
      sinkType: 'topic',
      topic: ops.sinkTopic,
    };
    nodes.push({
      id: sinkId,
      type: 'sink',
      position: { x: 0, y: 0 },
      data: sinkData,
    });
    edges.push({ id: nextId('edge'), source: lastNodeId, target: sinkId });

    // Edge from connector to sink
    if (connectorIds.has(ops.sinkConnector)) {
      edges.push({
        id: nextId('edge'),
        source: connectorIds.get(ops.sinkConnector)!,
        target: sinkId,
        targetHandle: 'connector-in',
      });
    }
  }

  if (ops.tapInfo) {
    const sinkId = nextId('sink');
    const sinkData: SinkNodeData = {
      label: 'tap',
      sinkType: 'tap',
      counter: ops.tapInfo.counter,
      labels: ops.tapInfo.labels,
    };
    nodes.push({
      id: sinkId,
      type: 'sink',
      position: { x: 0, y: 0 },
      data: sinkData,
    });
    edges.push({ id: nextId('edge'), source: lastNodeId, target: sinkId });
  }
}

// ============================================
// Sequence stream: EventA as a -> EventB as b .within(5m)
// ============================================

function parseSequenceStream(
  streamName: string,
  afterEquals: string,
  _block: string,
  nodes: FlowNode[],
  edges: FlowEdge[],
  connectorIds: Map<string, string>,
  eventIds: Map<string, string>,
  streamIds: Map<string, string>
): void {
  // Split at first known dot-op to separate pattern from operations
  const dotOpIdx = findFirstDotOp(afterEquals);
  const patternPart = dotOpIdx >= 0 ? afterEquals.substring(0, dotOpIdx).trim() : afterEquals.trim();
  const opsPart = dotOpIdx >= 0 ? afterEquals.substring(dotOpIdx) : '';

  // Also check for .within() which comes between pattern and ops
  let within: string | undefined;
  const withinMatch = afterEquals.match(/\.within\s*\(\s*(\d+\w+)\s*\)/);
  if (withinMatch) {
    within = withinMatch[1];
  }

  // Parse pattern events from "EventA as a -> EventB where cond as b"
  const eventSegments = patternPart.split(/\s+->\s+/);
  const patternEvents: PatternNodeData['events'] = [];

  for (const seg of eventSegments) {
    const parsed = parsePatternEvent(seg.trim());
    if (parsed) patternEvents.push(parsed);
  }

  // Create pattern node
  const patternId = nextId('pattern');
  const patternData: PatternNodeData = {
    label: streamName,
    patternType: 'SEQ',
    events: patternEvents,
    within,
  };

  // Check for forecast in operations
  const forecastMatch = opsPart.match(
    /\.forecast\s*\(([^)]*)\)/
  );
  if (forecastMatch) {
    const fParams = extractInlineParams(forecastMatch[1]);
    patternData.forecast = {
      confidence: fParams.confidence,
      horizon: fParams.horizon,
      warmup: fParams.warmup,
      maxDepth: fParams.max_depth,
      mode: fParams.mode,
    };
  }

  nodes.push({
    id: patternId,
    type: 'pattern',
    position: { x: 0, y: 0 },
    data: patternData,
  });
  streamIds.set(streamName, patternId);

  // Add edges from event types
  for (const evt of patternEvents) {
    if (eventIds.has(evt.eventType)) {
      edges.push({
        id: nextId('edge'),
        source: eventIds.get(evt.eventType)!,
        target: patternId,
        targetHandle: 'event-in',
      });
    }
  }

  // Parse remaining operations
  const ops = parseOperations(opsPart);
  let lastNodeId = patternId;

  if (ops.streamOps.length > 0) {
    const streamId = nextId('stream');
    const streamData: StreamNodeData = {
      label: streamName + '_ops',
      operations: ops.streamOps,
    };
    nodes.push({
      id: streamId,
      type: 'stream',
      position: { x: 0, y: 0 },
      data: streamData,
    });
    edges.push({ id: nextId('edge'), source: lastNodeId, target: streamId });
    lastNodeId = streamId;
  }

  if (ops.emitFields) {
    const emitId = nextId('emit');
    const emitData: EmitNodeData = {
      label: 'Output',
      fields: ops.emitFields,
      emitAll: ops.emitFields.length === 0,
    };
    nodes.push({
      id: emitId,
      type: 'emit',
      position: { x: 0, y: 0 },
      data: emitData,
    });
    edges.push({ id: nextId('edge'), source: lastNodeId, target: emitId });
    lastNodeId = emitId;
  }

  if (ops.sinkConnector) {
    const sinkId = nextId('sink');
    const sinkData: SinkNodeData = {
      label: ops.sinkConnector,
      sinkType: 'topic',
      topic: ops.sinkTopic,
    };
    nodes.push({
      id: sinkId,
      type: 'sink',
      position: { x: 0, y: 0 },
      data: sinkData,
    });
    edges.push({ id: nextId('edge'), source: lastNodeId, target: sinkId });

    if (connectorIds.has(ops.sinkConnector)) {
      edges.push({
        id: nextId('edge'),
        source: connectorIds.get(ops.sinkConnector)!,
        target: sinkId,
        targetHandle: 'connector-in',
      });
    }
  }

  if (ops.tapInfo) {
    const sinkId = nextId('sink');
    const sinkData: SinkNodeData = {
      label: 'tap',
      sinkType: 'tap',
      counter: ops.tapInfo.counter,
      labels: ops.tapInfo.labels,
    };
    nodes.push({
      id: sinkId,
      type: 'sink',
      position: { x: 0, y: 0 },
      data: sinkData,
    });
    edges.push({ id: nextId('edge'), source: lastNodeId, target: sinkId });
  }
}

/** Parse a single pattern event segment like "EventType where cond as alias" */
function parsePatternEvent(seg: string): PatternNodeData['events'][0] | null {
  if (!seg) return null;

  // Pattern: EventType [kleene] [where condition] [as alias]
  // Examples: "Login as login", "Transaction where amount > 5000 as tx"
  let remaining = seg;
  let alias: string | undefined;
  let condition: string | undefined;
  let kleene: '' | '?' | '*' | '+' = '';

  // Extract trailing "as alias"
  const asMatch = remaining.match(/\s+as\s+(\w+)\s*$/);
  if (asMatch) {
    alias = asMatch[1];
    remaining = remaining.substring(0, remaining.length - asMatch[0].length);
  }

  // Extract "where condition" (everything between 'where' and end/as)
  const whereIdx = remaining.search(/\s+where\s+/);
  if (whereIdx >= 0) {
    condition = remaining.substring(whereIdx).replace(/^\s+where\s+/, '').trim();
    remaining = remaining.substring(0, whereIdx);
  }

  // Extract event type and optional kleene
  remaining = remaining.trim();
  const kleeneMatch = remaining.match(/([?*+])$/);
  if (kleeneMatch) {
    kleene = kleeneMatch[1] as '?' | '*' | '+';
    remaining = remaining.substring(0, remaining.length - 1).trim();
  }

  const eventType = remaining;
  if (!eventType) return null;

  return { eventType, alias, condition, kleene: kleene || undefined };
}

// ============================================
// Merge stream: stream Name = merge(A, B, C)
// ============================================

function parseMergeStream(
  streamName: string,
  mergeArgs: string,
  block: string,
  nodes: FlowNode[],
  edges: FlowEdge[],
  streamIds: Map<string, string>
): void {
  const streamId = nextId('stream');
  const sources = mergeArgs.split(',').map((s) => s.trim());

  const ops: StreamNodeData['operations'] = [
    { type: 'merge' as OperationType, keys: sources.join(', ') },
  ];

  // Parse any downstream operations
  const flat = block.replace(/\n\s*/g, ' ').trim();
  const afterMerge = flat.replace(/^.*merge\s*\([^)]*\)\s*/, '');
  const parsedOps = parseOperations(afterMerge);
  ops.push(...parsedOps.streamOps);

  const streamData: StreamNodeData = {
    label: streamName,
    operations: ops,
  };

  nodes.push({
    id: streamId,
    type: 'stream',
    position: { x: 0, y: 0 },
    data: streamData,
  });
  streamIds.set(streamName, streamId);

  // Connect merge sources
  for (const src of sources) {
    if (streamIds.has(src)) {
      edges.push({
        id: nextId('edge'),
        source: streamIds.get(src)!,
        target: streamId,
      });
    }
  }

  let lastNodeId = streamId;

  if (parsedOps.emitFields) {
    const emitId = nextId('emit');
    const emitData: EmitNodeData = {
      label: 'Output',
      fields: parsedOps.emitFields,
      emitAll: parsedOps.emitFields.length === 0,
    };
    nodes.push({
      id: emitId,
      type: 'emit',
      position: { x: 0, y: 0 },
      data: emitData,
    });
    edges.push({ id: nextId('edge'), source: lastNodeId, target: emitId });
    lastNodeId = emitId;
  }

  if (parsedOps.sinkConnector) {
    const sinkId = nextId('sink');
    const sinkData: SinkNodeData = {
      label: parsedOps.sinkConnector,
      sinkType: 'topic',
    };
    nodes.push({
      id: sinkId,
      type: 'sink',
      position: { x: 0, y: 0 },
      data: sinkData,
    });
    edges.push({ id: nextId('edge'), source: lastNodeId, target: sinkId });
  }
}

// ============================================
// Operation chain parser
// ============================================

interface ParsedOps {
  streamOps: StreamNodeData['operations'];
  emitFields: EmitNodeData['fields'] | null;
  sinkConnector: string | null;
  sinkTopic: string | undefined;
  tapInfo: { counter?: string; labels?: string } | null;
}

function parseOperations(str: string): ParsedOps {
  const result: ParsedOps = {
    streamOps: [],
    emitFields: null,
    sinkConnector: null,
    sinkTopic: undefined,
    tapInfo: null,
  };

  // Match all .op(...) calls — handle nested parens
  const opCalls = extractDotOps(str);

  for (const { name, args } of opCalls) {
    switch (name) {
      case 'where':
        result.streamOps.push({ type: 'where', condition: args });
        break;
      case 'window':
        result.streamOps.push({ type: 'window', windowSize: args });
        break;
      case 'aggregate': {
        result.streamOps.push({ type: 'aggregate', aggregations: args });
        break;
      }
      case 'partition_by':
        result.streamOps.push({ type: 'partition_by', keys: args });
        break;
      case 'order_by':
        result.streamOps.push({ type: 'order_by', keys: args });
        break;
      case 'select':
        result.streamOps.push({ type: 'select', projections: args });
        break;
      case 'limit':
        result.streamOps.push({ type: 'limit', count: parseInt(args) || 0 });
        break;
      case 'distinct':
        result.streamOps.push({ type: 'distinct' });
        break;
      case 'forecast': {
        const fParams = extractInlineParams(args);
        result.streamOps.push({
          type: 'forecast',
          forecastConfidence: fParams.confidence,
          forecastHorizon: fParams.horizon,
          forecastWarmup: fParams.warmup,
          forecastMaxDepth: fParams.max_depth,
          forecastMode: fParams.mode,
        });
        break;
      }
      case 'trend_aggregate':
        result.streamOps.push({ type: 'trend_aggregate', trendAggregations: args });
        break;
      case 'emit': {
        if (!args.trim()) {
          result.emitFields = [];
        } else {
          result.emitFields = parseEmitFields(args);
        }
        break;
      }
      case 'to': {
        const toMatch = args.match(/^(\w+)(?:\s*,\s*topic\s*:\s*"([^"]*)")?/);
        if (toMatch) {
          result.sinkConnector = toMatch[1];
          result.sinkTopic = toMatch[2];
        }
        break;
      }
      case 'tap': {
        const tapParams = extractInlineParams(args);
        result.tapInfo = {
          counter: tapParams.counter,
          labels: tapParams.labels,
        };
        break;
      }
      // .from() and .within() handled separately
    }
  }

  return result;
}

/** Extract all `.name(args)` calls from a string, handling nested parens */
function extractDotOps(str: string): { name: string; args: string }[] {
  const ops: { name: string; args: string }[] = [];
  const re = /\.(\w+)\s*\(/g;
  let match;

  while ((match = re.exec(str)) !== null) {
    const name = match[1];
    // Skip .from() and .within() — handled separately
    if (name === 'from' || name === 'within') continue;

    const start = match.index + match[0].length;
    const end = findMatchingParen(str, start - 1);
    if (end > start) {
      ops.push({ name, args: str.substring(start, end).trim() });
    }
  }

  return ops;
}

/** Find matching closing paren for the opening paren at `pos` */
function findMatchingParen(str: string, openPos: number): number {
  let depth = 0;
  let inStr = false;
  for (let i = openPos; i < str.length; i++) {
    const ch = str[i];
    if (ch === '"' && (i === 0 || str[i - 1] !== '\\')) {
      inStr = !inStr;
    }
    if (inStr) continue;
    if (ch === '(') depth++;
    if (ch === ')') {
      depth--;
      if (depth === 0) return i;
    }
  }
  return str.length;
}

/** Parse emit field expressions: "name: expr, name2: expr2" */
function parseEmitFields(args: string): EmitNodeData['fields'] {
  const fields: EmitNodeData['fields'] = [];
  // Split on commas that are not inside parens or strings
  const parts = splitTopLevel(args, ',');

  for (const part of parts) {
    const colonIdx = part.indexOf(':');
    if (colonIdx > 0) {
      const name = part.substring(0, colonIdx).trim();
      const expression = part.substring(colonIdx + 1).trim();
      // Skip if it looks like a string literal value (e.g. alert_type: "HIGH_TEMP")
      fields.push({ name, expression: expression.replace(/^"(.*)"$/, '$1') });
    }
  }

  return fields;
}

/** Split a string on `sep` but only at the top-level (not inside parens/strings) */
function splitTopLevel(str: string, sep: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let inStr = false;
  let current = '';

  for (let i = 0; i < str.length; i++) {
    const ch = str[i];
    if (ch === '"' && (i === 0 || str[i - 1] !== '\\')) {
      inStr = !inStr;
    }
    if (!inStr) {
      if (ch === '(' || ch === '[') depth++;
      if (ch === ')' || ch === ']') depth--;
      if (ch === sep && depth === 0) {
        parts.push(current.trim());
        current = '';
        continue;
      }
    }
    current += ch;
  }
  if (current.trim()) parts.push(current.trim());

  return parts;
}

// ============================================
// Param extraction helpers
// ============================================

/** Extract key: value pairs from a block like "host: 'localhost', port: 1883" */
function extractParams(block: string): Record<string, string> {
  const params: Record<string, string> = {};
  const re = /(\w+)\s*:\s*(?:"([^"]*)"|(\d+(?:\.\d+)?)|(\w+))/g;
  let match;
  while ((match = re.exec(block)) !== null) {
    params[match[1]] = match[2] ?? match[3] ?? match[4] ?? '';
  }
  return params;
}

/** Extract inline params from "confidence: 0.5, horizon: 15m, warmup: 500" */
function extractInlineParams(str: string): Record<string, string> {
  const params: Record<string, string> = {};
  const re = /(\w+)\s*:\s*(?:"([^"]*)"|([^\s,)]+))/g;
  let match;
  while ((match = re.exec(str)) !== null) {
    params[match[1]] = match[2] ?? match[3] ?? '';
  }
  return params;
}

// ============================================
// Dagre layout
// ============================================

function applyDagreLayout(result: ParseResult): ParseResult {
  if (result.nodes.length === 0) return result;

  const g = new dagre.graphlib.Graph();
  g.setGraph({ rankdir: 'LR', nodesep: 50, ranksep: 100 });
  g.setDefaultEdgeLabel(() => ({}));

  result.nodes.forEach((node) => {
    g.setNode(node.id, { width: 180, height: 100 });
  });

  result.edges.forEach((edge) => {
    g.setEdge(edge.source, edge.target);
  });

  dagre.layout(g);

  const layoutedNodes = result.nodes.map((node) => {
    const nodeWithPosition = g.node(node.id);
    return {
      ...node,
      position: {
        x: nodeWithPosition.x - 90,
        y: nodeWithPosition.y - 50,
      },
    };
  });

  return { nodes: layoutedNodes, edges: result.edges };
}
