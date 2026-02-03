import { Node, Edge } from '@xyflow/react';

// ============================================
// Node Types
// ============================================

export type NodeType =
  | 'connector'   // External system connection (MQTT, Kafka, HTTP, etc.)
  | 'event'       // Event type definition
  | 'source'      // Data input (listens on a connector)
  | 'stream'      // Stream processing pipeline
  | 'pattern'     // SASE+ pattern matching
  | 'emit'        // Output shaping
  | 'sink';       // Data output (sends to a connector)

// ============================================
// Connector Node - External System Connection
// ============================================

export type ConnectorType = 'mqtt' | 'kafka' | 'amqp' | 'http' | 'file' | 'websocket';

export interface ConnectorNodeData {
  label: string;
  connectorType: ConnectorType;
  // MQTT settings
  broker?: string;
  port?: number;
  clientId?: string;
  username?: string;
  useTls?: boolean;
  // Kafka settings
  brokers?: string;       // comma-separated
  groupId?: string;
  securityProtocol?: 'PLAINTEXT' | 'SSL' | 'SASL_PLAINTEXT' | 'SASL_SSL';
  // HTTP settings
  baseUrl?: string;
  headers?: string;       // JSON object as string
  authType?: 'none' | 'basic' | 'bearer' | 'api_key';
  // File settings
  basePath?: string;
  // WebSocket settings
  wsUrl?: string;
  [key: string]: unknown;
}

// ============================================
// Event Node - Type Definition
// ============================================

export type FieldType = 'int' | 'float' | 'bool' | 'str' | 'timestamp' | 'duration' | 'list' | 'map' | 'any';

export interface EventField {
  name: string;
  type: FieldType;
  optional?: boolean;
  defaultValue?: string;
}

export interface EventNodeData {
  label: string;
  extends?: string;
  fields: EventField[];
  [key: string]: unknown;
}

// ============================================
// Source Node - Data Input
// ============================================

export interface SourceNodeData {
  label: string;
  // Topic/path/endpoint to listen on (relative to connector)
  topic?: string;
  path?: string;
  endpoint?: string;
  // Query parameters for HTTP
  queryParams?: string;
  // Event type this source produces
  eventType?: string;
  // Inline connection (if not using connector node)
  inlineUri?: string;
  [key: string]: unknown;
}

// ============================================
// Stream Node - Processing Pipeline
// ============================================

export type OperationType =
  | 'where' | 'select' | 'window' | 'aggregate'
  | 'partition_by' | 'order_by' | 'limit' | 'distinct'
  | 'map' | 'filter' | 'join' | 'merge' | 'flatten';

export interface StreamOperation {
  type: OperationType;
  // Where/Filter condition
  condition?: string;
  // Select projections (field expressions)
  projections?: string;
  // Window settings
  windowSize?: string;
  windowType?: 'tumbling' | 'sliding' | 'session';
  slideSize?: string;
  // Aggregate functions
  aggregations?: string;
  // Partition/Order keys
  keys?: string;
  // Limit count
  count?: number;
  // Join settings
  joinType?: 'inner' | 'left' | 'right' | 'full';
  joinOn?: string;
  joinWindow?: string;
  // Map/Filter lambda
  lambda?: string;
}

export interface StreamNodeData {
  label: string;
  operations: StreamOperation[];
  [key: string]: unknown;
}

// ============================================
// Pattern Node - SASE+ Pattern Matching
// ============================================

export type KleeneOperator = '' | '?' | '*' | '+';
export type PatternType = 'SEQ' | 'AND' | 'OR' | 'NOT';

export interface PatternEvent {
  eventType: string;
  alias?: string;
  condition?: string;
  kleene?: KleeneOperator;
  negated?: boolean;
}

export interface PatternNodeData {
  label: string;
  patternType: PatternType;
  events: PatternEvent[];
  within?: string;
  partitionBy?: string;
  skipTillMatch?: 'strict' | 'skip_till_next' | 'skip_till_any';
  [key: string]: unknown;
}

// ============================================
// Emit Node - Output Shaping
// ============================================

export interface EmitField {
  name: string;
  expression: string;
}

export interface EmitNodeData {
  label: string;
  fields: EmitField[];
  emitAll?: boolean;
  [key: string]: unknown;
}

// ============================================
// Sink Node - Data Output
// ============================================

export type SinkType = 'topic' | 'endpoint' | 'file' | 'console' | 'log' | 'tap';

export interface SinkNodeData {
  label: string;
  sinkType: SinkType;
  // Topic/path/endpoint to send to (relative to connector)
  topic?: string;
  path?: string;
  endpoint?: string;
  method?: 'POST' | 'PUT' | 'PATCH';
  // Inline connection (if not using connector node)
  inlineUri?: string;
  // Tap/metrics options
  counter?: string;
  histogram?: string;
  labels?: string;
  sampleRate?: number;
  // Log options
  logLevel?: 'debug' | 'info' | 'warn' | 'error';
  [key: string]: unknown;
}

// ============================================
// Union Types
// ============================================

export type FlowNodeData =
  | ConnectorNodeData
  | EventNodeData
  | SourceNodeData
  | StreamNodeData
  | PatternNodeData
  | EmitNodeData
  | SinkNodeData;

export type FlowNode = Node<FlowNodeData>;
export type FlowEdge = Edge;

export interface FlowState {
  nodes: FlowNode[];
  edges: FlowEdge[];
}

// ============================================
// Type Guards
// ============================================

export function isConnectorNodeData(data: unknown): data is ConnectorNodeData {
  return (
    typeof data === 'object' &&
    data !== null &&
    'label' in data &&
    'connectorType' in data
  );
}

export function isEventNodeData(data: unknown): data is EventNodeData {
  return (
    typeof data === 'object' &&
    data !== null &&
    'label' in data &&
    'fields' in data &&
    Array.isArray((data as EventNodeData).fields)
  );
}

export function isSourceNodeData(data: unknown): data is SourceNodeData {
  return (
    typeof data === 'object' &&
    data !== null &&
    'label' in data &&
    !('connectorType' in data) &&
    !('operations' in data) &&
    !('patternType' in data) &&
    !('sinkType' in data) &&
    !('fields' in data && Array.isArray((data as EmitNodeData).fields) && (data as EmitNodeData).fields[0]?.expression !== undefined)
  );
}

export function isStreamNodeData(data: unknown): data is StreamNodeData {
  return (
    typeof data === 'object' &&
    data !== null &&
    'label' in data &&
    'operations' in data &&
    Array.isArray((data as StreamNodeData).operations)
  );
}

export function isPatternNodeData(data: unknown): data is PatternNodeData {
  return (
    typeof data === 'object' &&
    data !== null &&
    'label' in data &&
    'patternType' in data &&
    'events' in data
  );
}

export function isEmitNodeData(data: unknown): data is EmitNodeData {
  return (
    typeof data === 'object' &&
    data !== null &&
    'label' in data &&
    'fields' in data &&
    Array.isArray((data as EmitNodeData).fields)
  );
}

export function isSinkNodeData(data: unknown): data is SinkNodeData {
  return (
    typeof data === 'object' &&
    data !== null &&
    'label' in data &&
    'sinkType' in data
  );
}

// ============================================
// Default Data Factories
// ============================================

export function createDefaultConnectorData(type: ConnectorType): ConnectorNodeData {
  const base = { label: `${type.toUpperCase()} Connection`, connectorType: type };
  switch (type) {
    case 'mqtt':
      return { ...base, broker: 'localhost', port: 1883 };
    case 'kafka':
      return { ...base, brokers: 'localhost:9092', groupId: 'my-group' };
    case 'http':
      return { ...base, baseUrl: 'https://api.example.com' };
    case 'amqp':
      return { ...base, broker: 'localhost', port: 5672 };
    case 'file':
      return { ...base, basePath: './data' };
    case 'websocket':
      return { ...base, wsUrl: 'ws://localhost:8080' };
    default:
      return base;
  }
}

export function createDefaultEventData(): EventNodeData {
  return {
    label: 'NewEvent',
    fields: [
      { name: 'id', type: 'str' },
      { name: 'timestamp', type: 'timestamp' },
      { name: 'value', type: 'float' },
    ],
  };
}

export function createDefaultSourceData(): SourceNodeData {
  return {
    label: 'DataSource',
    topic: 'events/#',
  };
}

export function createDefaultStreamData(): StreamNodeData {
  return {
    label: 'ProcessingStream',
    operations: [],
  };
}

export function createDefaultPatternData(): PatternNodeData {
  return {
    label: 'EventPattern',
    patternType: 'SEQ',
    events: [],
    within: '5m',
  };
}

export function createDefaultEmitData(): EmitNodeData {
  return {
    label: 'Output',
    fields: [],
    emitAll: true,
  };
}

export function createDefaultSinkData(type: SinkType): SinkNodeData {
  const base = { label: `${type} Output`, sinkType: type };
  switch (type) {
    case 'topic':
      return { ...base, topic: 'output-events' };
    case 'endpoint':
      return { ...base, endpoint: '/api/events', method: 'POST' };
    case 'file':
      return { ...base, path: './output.json' };
    case 'console':
      return { ...base };
    case 'log':
      return { ...base, logLevel: 'info' };
    case 'tap':
      return { ...base, counter: 'events_total' };
    default:
      return base;
  }
}

// ============================================
// VSCode API
// ============================================

declare global {
  interface Window {
    acquireVsCodeApi?: () => {
      postMessage: (message: unknown) => void;
      getState: () => unknown;
      setState: (state: unknown) => void;
    };
  }
}

export interface VsCodeMessage {
  type: string;
  payload?: unknown;
}
