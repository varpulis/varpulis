import EventNode from './EventNode';
import PatternNode from './PatternNode';
import SinkNode from './SinkNode';
import SourceNode from './SourceNode';
import StreamNode from './StreamNode';

export const nodeTypes = {
  source: SourceNode,
  sink: SinkNode,
  event: EventNode,
  stream: StreamNode,
  pattern: PatternNode,
};

export { EventNode, PatternNode, SinkNode, SourceNode, StreamNode };

