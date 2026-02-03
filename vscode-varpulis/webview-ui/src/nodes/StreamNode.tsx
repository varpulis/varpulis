import React from 'react';
import { Handle, Position, NodeProps } from '@xyflow/react';
import { isStreamNodeData, StreamOperation } from '../types/flow';

const operationLabels: Record<string, string> = {
  where: '.where',
  select: '.select',
  window: '.window',
  aggregate: '.aggregate',
  partition_by: '.partition_by',
  order_by: '.order_by',
  limit: '.limit',
  distinct: '.distinct',
  map: '.map',
  filter: '.filter',
  join: '.join',
  merge: 'merge',
};

const renderOperation = (op: StreamOperation, index: number) => {
  const label = operationLabels[op.type] || op.type;
  let value = '';

  switch (op.type) {
    case 'where':
    case 'filter':
      value = op.condition || op.lambda || '';
      break;
    case 'select':
      value = op.projections || '';
      break;
    case 'window':
      value = op.windowSize || '';
      if (op.windowType === 'sliding' && op.slideSize) {
        value += `, sliding: ${op.slideSize}`;
      }
      break;
    case 'aggregate':
      value = op.aggregations || '';
      break;
    case 'partition_by':
    case 'order_by':
      value = op.keys || '';
      break;
    case 'limit':
      value = op.count?.toString() || '';
      break;
    case 'map':
      value = op.lambda || '';
      break;
    case 'join':
      value = `${op.joinType || 'inner'} on ${op.joinOn || '...'}`;
      break;
    default:
      value = '';
  }

  return (
    <div key={index} className="stream-operation">
      <span className="op-name">{label}</span>
      {value && <span className="op-value">({value.length > 25 ? value.slice(0, 25) + '...' : value})</span>}
    </div>
  );
};

export const StreamNode: React.FC<NodeProps> = ({ data, selected }) => {
  if (!isStreamNodeData(data)) {
    return <div className="custom-node stream-node error">Invalid Stream</div>;
  }

  return (
    <div className={`custom-node stream-node ${selected ? 'selected' : ''}`}>
      {/* Input from source or another stream */}
      <Handle
        type="target"
        position={Position.Left}
        id="data-in"
        title="Input from Source, Stream, or Pattern"
      />
      {/* Secondary input for joins */}
      <Handle
        type="target"
        position={Position.Top}
        id="join-in"
        style={{ left: '70%' }}
        title="Join input (optional)"
      />
      <div className="node-header">
        <span className="node-icon stream">🌊</span>
        <span className="node-title">{data.label}</span>
      </div>
      <div className="node-body">
        {data.operations.length === 0 ? (
          <div className="node-prop empty">
            <span className="prop-value">No operations - click to add</span>
          </div>
        ) : (
          <div className="stream-operations">
            {data.operations.map((op, i) => renderOperation(op, i))}
          </div>
        )}
      </div>
      {/* Output to next stream, pattern, or emit */}
      <Handle
        type="source"
        position={Position.Right}
        id="data-out"
        title="Output to Stream, Pattern, or Emit"
      />
    </div>
  );
};
