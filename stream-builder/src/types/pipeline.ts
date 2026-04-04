/** Pipeline types mirroring the Rust AST (crates/varpulis-core/src/ast.rs) */

export type OperationCategory =
  | 'filter'
  | 'window'
  | 'aggregate'
  | 'transform'
  | 'io'
  | 'temporal'
  | 'advanced'
  | 'observability'

export interface OperationMeta {
  name: string
  label: string
  category: OperationCategory
  color: string
  description: string
}

/** All 38 stream operations grouped by category */
export const OPERATION_CATALOG: OperationMeta[] = [
  // Filter
  { name: 'where', label: '.where()', category: 'filter', color: 'var(--color-op-filter)', description: 'Filter events by condition' },
  { name: 'filter', label: '.filter()', category: 'filter', color: 'var(--color-op-filter)', description: 'Lambda-based filter' },
  { name: 'having', label: '.having()', category: 'filter', color: 'var(--color-op-filter)', description: 'Filter after aggregation' },
  { name: 'distinct', label: '.distinct()', category: 'filter', color: 'var(--color-op-filter)', description: 'Remove duplicates' },
  // Window
  { name: 'window', label: '.window()', category: 'window', color: 'var(--color-op-window)', description: 'Time window (tumbling/sliding/session)' },
  { name: 'partition_by', label: '.partition_by()', category: 'window', color: 'var(--color-op-window)', description: 'Partition by key' },
  // Aggregate
  { name: 'aggregate', label: '.aggregate()', category: 'aggregate', color: 'var(--color-op-aggregate)', description: 'Compute aggregates (avg, sum, count, etc.)' },
  { name: 'order_by', label: '.order_by()', category: 'aggregate', color: 'var(--color-op-aggregate)', description: 'Sort results' },
  { name: 'limit', label: '.limit()', category: 'aggregate', color: 'var(--color-op-aggregate)', description: 'Take first N results' },
  // Transform
  { name: 'select', label: '.select()', category: 'transform', color: 'var(--color-op-transform)', description: 'Project fields' },
  { name: 'map', label: '.map()', category: 'transform', color: 'var(--color-op-transform)', description: 'Transform each event' },
  { name: 'emit', label: '.emit()', category: 'transform', color: 'var(--color-op-transform)', description: 'Output event with field mapping' },
  // I/O
  { name: 'from', label: '.from()', category: 'io', color: 'var(--color-op-io)', description: 'Read from connector' },
  { name: 'to', label: '.to()', category: 'io', color: 'var(--color-op-io)', description: 'Send to connector' },
  { name: 'alert', label: '.alert()', category: 'io', color: 'var(--color-op-io)', description: 'Send alert notification' },
  // Temporal
  { name: 'followed_by', label: '->', category: 'temporal', color: 'var(--color-op-temporal)', description: 'Temporal sequence step' },
  { name: 'within', label: '.within()', category: 'temporal', color: 'var(--color-op-temporal)', description: 'Sequence time constraint' },
  { name: 'not', label: '.not()', category: 'temporal', color: 'var(--color-op-temporal)', description: 'Negation in sequence' },
  // Advanced
  { name: 'forecast', label: '.forecast()', category: 'advanced', color: 'var(--color-op-advanced)', description: 'PST-based pattern prediction' },
  { name: 'score', label: '.score()', category: 'advanced', color: 'var(--color-op-advanced)', description: 'ONNX ML model inference' },
  { name: 'enrich', label: '.enrich()', category: 'advanced', color: 'var(--color-op-advanced)', description: 'External data lookup' },
  { name: 'trend_aggregate', label: '.trend_aggregate()', category: 'advanced', color: 'var(--color-op-advanced)', description: 'Hamlet trend aggregation' },
  // Observability
  { name: 'tap', label: '.tap()', category: 'observability', color: 'var(--color-op-observability)', description: 'Metrics tap' },
  { name: 'log', label: '.log()', category: 'observability', color: 'var(--color-op-observability)', description: 'Structured logging' },
  { name: 'print', label: '.print()', category: 'observability', color: 'var(--color-op-observability)', description: 'Debug output' },
]

export const CATEGORY_COLORS: Record<OperationCategory, string> = {
  filter: '#3b82f6',
  window: '#22c55e',
  aggregate: '#f97316',
  transform: '#14b8a6',
  io: '#6b7280',
  temporal: '#ef4444',
  advanced: '#8b5cf6',
  observability: '#64748b',
}

export const CATEGORY_LABELS: Record<OperationCategory, string> = {
  filter: 'Filter',
  window: 'Window',
  aggregate: 'Aggregate',
  transform: 'Transform',
  io: 'I/O',
  temporal: 'Temporal',
  advanced: 'Advanced',
  observability: 'Observability',
}

/** A single operation step within a stream card */
export interface StreamOperation {
  id: string
  type: string       // operation name from OPERATION_CATALOG
  value: string      // expression/config string
  expanded: boolean  // whether the editor is open
}

/** A stream definition */
export interface StreamDef {
  id: string
  name: string
  source: string        // source stream/event name
  sourceType: 'stream' | 'event' | 'merge' | 'join' | 'sequence' | 'timer'
  sourceRefs: string[]  // for merge/join: list of source names
  operations: StreamOperation[]
}

/** A connector definition */
export interface ConnectorDef {
  id: string
  name: string
  type: string     // mqtt, kafka, http, etc.
  config: Record<string, string>
}

/** An event field */
export interface EventField {
  name: string
  type: string     // str, float, int, bool, timestamp, etc.
  optional: boolean
}

/** An event type definition */
export interface EventDef {
  id: string
  name: string
  fields: EventField[]
  extends?: string
}

/** The complete pipeline state */
export interface PipelineState {
  connectors: ConnectorDef[]
  events: EventDef[]
  streams: StreamDef[]
}
