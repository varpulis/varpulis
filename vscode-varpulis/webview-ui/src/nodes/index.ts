import { ConnectorNode } from './ConnectorNode';
import { EventNode } from './EventNode';
import { SourceNode } from './SourceNode';
import { StreamNode } from './StreamNode';
import { PatternNode } from './PatternNode';
import { EmitNode } from './EmitNode';
import { SinkNode } from './SinkNode';

export const nodeTypes = {
  connector: ConnectorNode,
  event: EventNode,
  source: SourceNode,
  stream: StreamNode,
  pattern: PatternNode,
  emit: EmitNode,
  sink: SinkNode,
};

export { ConnectorNode, EventNode, SourceNode, StreamNode, PatternNode, EmitNode, SinkNode };
