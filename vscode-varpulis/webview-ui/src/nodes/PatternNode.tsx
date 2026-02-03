import React from 'react';
import { Handle, Position, NodeProps } from '@xyflow/react';
import { isPatternNodeData, PatternEvent } from '../types/flow';

const renderPatternEvent = (event: PatternEvent, index: number, isLast: boolean) => {
  const kleeneSymbol = event.kleene || '';

  return (
    <React.Fragment key={index}>
      <div className="pattern-event">
        <span className="event-type">{event.eventType}</span>
        {kleeneSymbol && <span className="kleene-op">{kleeneSymbol}</span>}
        {event.alias && <span className="event-alias"> as {event.alias}</span>}
        {event.condition && (
          <div className="event-condition">
            <span className="cond-where">where</span> {event.condition.length > 20 ? event.condition.slice(0, 20) + '...' : event.condition}
          </div>
        )}
      </div>
      {!isLast && <span className="pattern-arrow">→</span>}
    </React.Fragment>
  );
};

export const PatternNode: React.FC<NodeProps> = ({ data, selected }) => {
  if (!isPatternNodeData(data)) {
    return <div className="custom-node pattern-node error">Invalid Pattern</div>;
  }

  const patternTypeColors: Record<string, string> = {
    SEQ: '#ec4899',
    AND: '#8b5cf6',
    OR: '#f59e0b',
    NOT: '#ef4444',
  };

  return (
    <div className={`custom-node pattern-node ${selected ? 'selected' : ''}`}>
      {/* Input from stream */}
      <Handle
        type="target"
        position={Position.Left}
        id="data-in"
        title="Input from Stream"
      />
      <div className="node-header">
        <span
          className="node-icon pattern"
          style={{ backgroundColor: patternTypeColors[data.patternType] + '33', color: patternTypeColors[data.patternType] }}
        >
          {data.patternType}
        </span>
        <span className="node-title">{data.label}</span>
      </div>
      <div className="node-body">
        {data.events.length === 0 ? (
          <div className="node-prop empty">
            <span className="prop-value">No events - click to configure</span>
          </div>
        ) : (
          <div className="pattern-events">
            {data.events.map((ev, i) => renderPatternEvent(ev, i, i === data.events.length - 1))}
          </div>
        )}
        {data.within && (
          <div className="pattern-within">
            <span className="within-label">within</span>
            <span className="within-value">{data.within}</span>
          </div>
        )}
        {data.partitionBy && (
          <div className="pattern-partition">
            <span className="partition-label">partition_by</span>
            <span className="partition-value">{data.partitionBy}</span>
          </div>
        )}
      </div>
      {/* Output to emit */}
      <Handle
        type="source"
        position={Position.Right}
        id="match-out"
        title="Pattern match output to Emit"
      />
    </div>
  );
};
