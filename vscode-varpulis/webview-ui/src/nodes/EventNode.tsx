import React from 'react';
import { Handle, Position, NodeProps } from '@xyflow/react';
import { isEventNodeData } from '../types/flow';

const typeColors: Record<string, string> = {
  int: '#4ec9b0',
  float: '#4ec9b0',
  bool: '#569cd6',
  str: '#ce9178',
  timestamp: '#dcdcaa',
  duration: '#dcdcaa',
  list: '#c586c0',
  map: '#c586c0',
};

export const EventNode: React.FC<NodeProps> = ({ data, selected }) => {
  if (!isEventNodeData(data)) {
    return <div className="custom-node event-node error">Invalid Event</div>;
  }

  return (
    <div className={`custom-node event-node ${selected ? 'selected' : ''}`}>
      <div className="node-header">
        <span className="node-icon event">📋</span>
        <span className="node-title">{data.label}</span>
        {data.extends && (
          <span className="event-extends" title={`extends ${data.extends}`}>
            ← {data.extends}
          </span>
        )}
      </div>
      <div className="node-body">
        {data.fields.length === 0 ? (
          <div className="node-prop empty">
            <span className="prop-value">No fields - click to add</span>
          </div>
        ) : (
          <div className="event-fields">
            {data.fields.map((field, i) => (
              <div key={i} className="event-field">
                <span className="field-name">{field.name}</span>
                <span className="field-colon">:</span>
                <span className="field-type" style={{ color: typeColors[field.type] || '#d4d4d4' }}>
                  {field.type}
                  {field.optional && '?'}
                </span>
              </div>
            ))}
          </div>
        )}
      </div>
      {/* Event types can be referenced by sources */}
      <Handle
        type="source"
        position={Position.Right}
        id="type-out"
        title="Event type reference"
      />
    </div>
  );
};
