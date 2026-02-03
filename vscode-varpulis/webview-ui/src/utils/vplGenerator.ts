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
  isConnectorNodeData,
  isEventNodeData,
  isSourceNodeData,
  isStreamNodeData,
  isPatternNodeData,
  isEmitNodeData,
  isSinkNodeData,
} from '../types/flow';

export function generateVpl(nodes: FlowNode[], edges: FlowEdge[]): string {
  if (nodes.length === 0) {
    return '';
  }

  const lines: string[] = [];
  const nodeMap = new Map(nodes.map(n => [n.id, n]));

  // Build edge lookup for traversal
  const outgoingEdges = new Map<string, FlowEdge[]>();
  const incomingEdges = new Map<string, FlowEdge[]>();

  for (const edge of edges) {
    if (!outgoingEdges.has(edge.source)) {
      outgoingEdges.set(edge.source, []);
    }
    outgoingEdges.get(edge.source)!.push(edge);

    if (!incomingEdges.has(edge.target)) {
      incomingEdges.set(edge.target, []);
    }
    incomingEdges.get(edge.target)!.push(edge);
  }

  // 1. Generate connector declarations
  const connectorNodes = nodes.filter(n => n.type === 'connector');
  for (const node of connectorNodes) {
    lines.push(generateConnectorDecl(node));
    lines.push('');
  }

  // 2. Generate event type declarations
  const eventNodes = nodes.filter(n => n.type === 'event');
  for (const node of eventNodes) {
    lines.push(generateEventDecl(node));
    lines.push('');
  }

  // 3. Generate pattern declarations (standalone, not part of streams)
  const patternNodes = nodes.filter(n => n.type === 'pattern');
  for (const node of patternNodes) {
    if (!hasIncomingStreamEdge(node.id, incomingEdges, nodeMap)) {
      lines.push(generatePatternDecl(node));
      lines.push('');
    }
  }

  // 4. Generate streams by following from source nodes
  const sourceNodes = nodes.filter(n => n.type === 'source');

  for (const sourceNode of sourceNodes) {
    const streamCode = generateStreamFromSource(
      sourceNode,
      nodeMap,
      outgoingEdges,
      incomingEdges
    );
    if (streamCode) {
      lines.push(streamCode);
      lines.push('');
    }
  }

  // 5. Generate sink statements for sinks not inline with streams
  const sinkNodes = nodes.filter(n => n.type === 'sink');
  for (const sinkNode of sinkNodes) {
    // Check if sink is already handled inline
    const incoming = incomingEdges.get(sinkNode.id) || [];
    const hasEmitSource = incoming.some(e => {
      const sourceNode = nodeMap.get(e.source);
      return sourceNode?.type === 'emit' || sourceNode?.type === 'stream';
    });

    if (!hasEmitSource) {
      lines.push(generateSinkStmt(sinkNode, nodeMap, incomingEdges));
    }
  }

  return lines.join('\n').trim();
}

function hasIncomingStreamEdge(
  nodeId: string,
  incomingEdges: Map<string, FlowEdge[]>,
  nodeMap: Map<string, FlowNode>
): boolean {
  const incoming = incomingEdges.get(nodeId) || [];
  return incoming.some(e => {
    const sourceNode = nodeMap.get(e.source);
    return sourceNode?.type === 'stream' || sourceNode?.type === 'source';
  });
}

// ============================================
// Connector Declaration
// ============================================

function generateConnectorDecl(node: FlowNode): string {
  const data = node.data as ConnectorNodeData;
  if (!isConnectorNodeData(data)) return `# Invalid connector: ${node.id}`;

  const params: string[] = [];

  switch (data.connectorType) {
    case 'mqtt':
      if (data.broker) params.push(`host: "${data.broker}"`);
      if (data.port) params.push(`port: ${data.port}`);
      if (data.clientId) params.push(`client_id: "${data.clientId}"`);
      if (data.username) params.push(`username: "${data.username}"`);
      if (data.useTls) params.push(`tls: true`);
      break;
    case 'kafka':
      if (data.brokers) params.push(`brokers: [${data.brokers.split(',').map(b => `"${b.trim()}"`).join(', ')}]`);
      if (data.groupId) params.push(`group_id: "${data.groupId}"`);
      if (data.securityProtocol) params.push(`security_protocol: "${data.securityProtocol}"`);
      break;
    case 'http':
      if (data.baseUrl) params.push(`base_url: "${data.baseUrl}"`);
      if (data.authType && data.authType !== 'none') params.push(`auth: ${data.authType}("...")`);
      break;
    case 'file':
      if (data.basePath) params.push(`path: "${data.basePath}"`);
      break;
    case 'websocket':
      if (data.wsUrl) params.push(`url: "${data.wsUrl}"`);
      break;
  }

  const paramsStr = params.length > 0 ? `\n    ${params.join(',\n    ')}\n` : '';
  return `connector ${data.label} = ${data.connectorType} (${paramsStr})`;
}

// ============================================
// Event Declaration
// ============================================

function generateEventDecl(node: FlowNode): string {
  const data = node.data as EventNodeData;
  if (!isEventNodeData(data)) return `# Invalid event: ${node.id}`;

  const fields = data.fields
    .map(f => `    ${f.name}: ${f.type}${f.optional ? '?' : ''}`)
    .join(',\n');

  const extendsClause = data.extends ? ` extends ${data.extends}` : '';
  return `event ${data.label}${extendsClause} {\n${fields}\n}`;
}

// ============================================
// Pattern Declaration
// ============================================

function generatePatternDecl(node: FlowNode): string {
  const data = node.data as PatternNodeData;
  if (!isPatternNodeData(data)) return `# Invalid pattern: ${node.id}`;

  const eventStrs = data.events.map(e => {
    let str = e.eventType;
    if (e.kleene) str += e.kleene;
    if (e.condition) str += ` where ${e.condition}`;
    if (e.alias) str += ` as ${e.alias}`;
    return str;
  });

  let pattern = `pattern ${data.label} = ${data.patternType}(${eventStrs.join(', ')})`;
  if (data.within) pattern += `\n    within ${data.within}`;
  if (data.partitionBy) pattern += `\n    partition by ${data.partitionBy}`;

  return pattern;
}

// ============================================
// Stream Generation
// ============================================

function generateStreamFromSource(
  sourceNode: FlowNode,
  nodeMap: Map<string, FlowNode>,
  outgoingEdges: Map<string, FlowEdge[]>,
  incomingEdges: Map<string, FlowEdge[]>
): string {
  const sourceData = sourceNode.data as SourceNodeData;
  if (!isSourceNodeData(sourceData)) return '';

  // Find connected connector
  const connectorName = findConnectedConnector(sourceNode.id, incomingEdges, nodeMap);

  // Find connected event type
  const eventTypeName = findConnectedEventType(sourceNode.id, incomingEdges, nodeMap)
    || sourceData.eventType
    || 'Event';

  // Build stream name
  const streamName = sourceData.label.replace(/\s+/g, '');

  // Start building the stream expression
  let streamExpr = `${eventTypeName}`;

  // Add .from() with connector and params
  const fromParams: string[] = [];
  if (sourceData.topic) fromParams.push(`topic: "${sourceData.topic}"`);
  if (sourceData.endpoint) fromParams.push(`endpoint: "${sourceData.endpoint}"`);
  if (sourceData.path) fromParams.push(`path: "${sourceData.path}"`);

  if (connectorName) {
    const paramsStr = fromParams.length > 0 ? `, ${fromParams.join(', ')}` : '';
    streamExpr += `.from(${connectorName}${paramsStr})`;
  } else if (sourceData.inlineUri) {
    streamExpr += `.from(file("${sourceData.inlineUri}"))`;
  }

  // Follow the pipeline and add operations
  let currentNodeId = sourceNode.id;
  let hasEmit = false;
  let sinkInfo: { connectorName: string; params: string[] } | null = null;

  while (true) {
    const outgoing = outgoingEdges.get(currentNodeId) || [];
    if (outgoing.length === 0) break;

    const nextNodeId = outgoing[0].target;
    const nextNode = nodeMap.get(nextNodeId);
    if (!nextNode) break;

    if (nextNode.type === 'stream') {
      const streamData = nextNode.data as StreamNodeData;
      if (isStreamNodeData(streamData)) {
        streamExpr += generateStreamOperations(streamData);
      }
    } else if (nextNode.type === 'emit') {
      const emitData = nextNode.data as EmitNodeData;
      if (isEmitNodeData(emitData)) {
        streamExpr += generateEmitOperation(emitData);
        hasEmit = true;
      }
    } else if (nextNode.type === 'sink') {
      const sinkData = nextNode.data as SinkNodeData;
      if (isSinkNodeData(sinkData)) {
        sinkInfo = getSinkInfo(nextNode, nodeMap, incomingEdges);
      }
      break; // Sink is the end
    } else if (nextNode.type === 'pattern') {
      // Pattern in stream - skip for now, patterns are declared separately
      break;
    }

    currentNodeId = nextNodeId;
  }

  // Add default emit if none present
  if (!hasEmit) {
    streamExpr += '\n    .emit()';
  }

  // Add inline .to() if sink is present
  if (sinkInfo) {
    const paramsStr = sinkInfo.params.length > 0 ? `, ${sinkInfo.params.join(', ')}` : '';
    streamExpr += `\n    .to(${sinkInfo.connectorName}${paramsStr})`;
  }

  return `stream ${streamName} = ${streamExpr}`;
}

function generateStreamOperations(data: StreamNodeData): string {
  let ops = '';

  for (const op of data.operations) {
    switch (op.type) {
      case 'where':
        if (op.condition) ops += `\n    .where(${op.condition})`;
        break;
      case 'select':
        if (op.projections) ops += `\n    .select(${op.projections})`;
        break;
      case 'window':
        if (op.windowSize) {
          const windowType = op.windowType || 'tumbling';
          if (windowType === 'sliding' && op.slideSize) {
            ops += `\n    .window(${op.windowSize}, sliding: ${op.slideSize})`;
          } else {
            ops += `\n    .window(${op.windowSize})`;
          }
        }
        break;
      case 'aggregate':
        if (op.aggregations) ops += `\n    .aggregate(${op.aggregations})`;
        break;
      case 'partition_by':
        if (op.keys) ops += `\n    .partition_by(${op.keys})`;
        break;
      case 'order_by':
        if (op.keys) ops += `\n    .order_by(${op.keys})`;
        break;
      case 'limit':
        if (op.count) ops += `\n    .limit(${op.count})`;
        break;
      case 'distinct':
        ops += `\n    .distinct()`;
        break;
      case 'map':
        if (op.lambda) ops += `\n    .map(${op.lambda})`;
        break;
      case 'filter':
        if (op.condition) ops += `\n    .filter(${op.condition})`;
        break;
    }
  }

  return ops;
}

function generateEmitOperation(data: EmitNodeData): string {
  if (data.emitAll || data.fields.length === 0) {
    return '\n    .emit()';
  }

  const fields = data.fields
    .map(f => `${f.name}: ${f.expression}`)
    .join(',\n        ');

  return `\n    .emit(\n        ${fields}\n    )`;
}

// ============================================
// Sink Statement Generation
// ============================================

function generateSinkStmt(
  sinkNode: FlowNode,
  nodeMap: Map<string, FlowNode>,
  incomingEdges: Map<string, FlowEdge[]>
): string {
  const data = sinkNode.data as SinkNodeData;
  if (!isSinkNodeData(data)) return `# Invalid sink: ${sinkNode.id}`;

  // Find the stream this sink connects to
  const incoming = incomingEdges.get(sinkNode.id) || [];
  let streamName = data.label;

  for (const edge of incoming) {
    const sourceNode = nodeMap.get(edge.source);
    if (sourceNode && (sourceNode.type === 'stream' || sourceNode.type === 'emit' || sourceNode.type === 'source')) {
      const sourceData = sourceNode.data as { label?: string };
      if (sourceData.label) {
        streamName = sourceData.label.replace(/\s+/g, '');
      }
      break;
    }
  }

  // Handle built-in sinks
  if (data.sinkType === 'console') {
    return `sink ${streamName} to console()`;
  }
  if (data.sinkType === 'log') {
    const level = data.logLevel || 'info';
    return `sink ${streamName} to log(level: "${level}")`;
  }
  if (data.sinkType === 'tap') {
    const params: string[] = [];
    if (data.counter) params.push(`counter: "${data.counter}"`);
    if (data.histogram) params.push(`histogram: "${data.histogram}"`);
    if (data.labels) params.push(`labels: [${data.labels}]`);
    return `sink ${streamName} to tap(${params.join(', ')})`;
  }

  // Connector-based sink
  const connectorName = findConnectedConnector(sinkNode.id, incomingEdges, nodeMap);
  if (connectorName) {
    const params: string[] = [];
    if (data.topic) params.push(`topic: "${data.topic}"`);
    if (data.endpoint) params.push(`endpoint: "${data.endpoint}"`);
    if (data.method) params.push(`method: "${data.method}"`);
    if (data.path) params.push(`path: "${data.path}"`);

    const paramsStr = params.length > 0 ? params.join(', ') : '';
    return `sink ${streamName} to ${connectorName} (${paramsStr})`;
  }

  return `sink ${streamName} to console()`;
}

function getSinkInfo(
  sinkNode: FlowNode,
  nodeMap: Map<string, FlowNode>,
  incomingEdges: Map<string, FlowEdge[]>
): { connectorName: string; params: string[] } | null {
  const data = sinkNode.data as SinkNodeData;
  if (!isSinkNodeData(data)) return null;

  // Built-in sinks
  if (data.sinkType === 'console') {
    return { connectorName: 'console()', params: [] };
  }
  if (data.sinkType === 'log') {
    return { connectorName: `log(level: "${data.logLevel || 'info'}")`, params: [] };
  }

  // Find connected connector
  const connectorName = findConnectedConnector(sinkNode.id, incomingEdges, nodeMap);
  if (!connectorName) return null;

  const params: string[] = [];
  if (data.topic) params.push(`topic: "${data.topic}"`);
  if (data.endpoint) params.push(`endpoint: "${data.endpoint}"`);
  if (data.method) params.push(`method: "${data.method}"`);

  return { connectorName, params };
}

// ============================================
// Helper Functions
// ============================================

function findConnectedConnector(
  nodeId: string,
  incomingEdges: Map<string, FlowEdge[]>,
  nodeMap: Map<string, FlowNode>
): string | null {
  const incoming = incomingEdges.get(nodeId) || [];

  for (const edge of incoming) {
    // Only check connector-in handle edges
    if (edge.targetHandle !== 'connector-in') continue;

    const sourceNode = nodeMap.get(edge.source);
    if (sourceNode?.type === 'connector') {
      const data = sourceNode.data as ConnectorNodeData;
      return data.label;
    }
  }

  return null;
}

function findConnectedEventType(
  nodeId: string,
  incomingEdges: Map<string, FlowEdge[]>,
  nodeMap: Map<string, FlowNode>
): string | null {
  const incoming = incomingEdges.get(nodeId) || [];

  for (const edge of incoming) {
    // Only check event-in handle edges
    if (edge.targetHandle !== 'event-in') continue;

    const sourceNode = nodeMap.get(edge.source);
    if (sourceNode?.type === 'event') {
      const data = sourceNode.data as EventNodeData;
      return data.label;
    }
  }

  return null;
}

