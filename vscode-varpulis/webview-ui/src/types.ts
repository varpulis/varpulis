export type ConnectorType = 'mqtt' | 'kafka' | 'amqp' | 'file' | 'http' | 'console';

export interface ConnectorConfig {
  type: ConnectorType;
  name: string;
  // MQTT
  host?: string;
  port?: number;
  topic?: string;
  clientId?: string;
  username?: string;
  password?: string;
  // Kafka
  brokers?: string[];
  groupId?: string;
  // AMQP
  queue?: string;
  exchange?: string;
  routingKey?: string;
  // File
  path?: string;
  format?: 'evt' | 'json' | 'csv';
  // HTTP
  url?: string;
  method?: 'GET' | 'POST';
  headers?: Record<string, string>;
}

export interface EventDefinition {
  name: string;
  fields: EventField[];
}

export interface EventField {
  name: string;
  type: 'string' | 'int' | 'float' | 'bool' | 'timestamp';
  required?: boolean;
}

export interface StreamDefinition {
  name: string;
  pattern: string;
  window?: string;
  filters?: string[];
}

export interface FlowNode {
  id: string;
  type: 'source' | 'sink' | 'event' | 'stream' | 'pattern';
  data: {
    label: string;
    connector?: ConnectorConfig;
    event?: EventDefinition;
    stream?: StreamDefinition;
  };
  position: { x: number; y: number };
}

export interface FlowEdge {
  id: string;
  source: string;
  target: string;
  sourceHandle?: string;
  targetHandle?: string;
}

export interface VarpulisFlow {
  nodes: FlowNode[];
  edges: FlowEdge[];
  metadata: {
    name: string;
    version: string;
    description?: string;
  };
}

// VSCode API interface for webview communication
export interface VSCodeAPI {
  postMessage(message: unknown): void;
  getState(): unknown;
  setState(state: unknown): void;
}

declare global {
  function acquireVsCodeApi(): VSCodeAPI;
}
