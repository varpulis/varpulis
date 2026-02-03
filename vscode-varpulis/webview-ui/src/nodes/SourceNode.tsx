import React from 'react';
import { Handle, Position, NodeProps } from '@xyflow/react';
import { isSourceNodeData } from '../types/flow';

export const SourceNode: React.FC<NodeProps> = ({ data, selected }) => {
  if (!isSourceNodeData(data)) {
    return <div className="custom-node source-node error">Invalid Source</div>;
  }

  return (
    <div className={`custom-node source-node ${selected ? 'selected' : ''}`}>
      {/* Connector input handle (left side, top) */}
      <Handle
        type="target"
        position={Position.Left}
        id="connector-in"
        style={{ top: '25%' }}
        title="Connect from Connector"
      />
      {/* Event type input handle (left side, bottom) */}
      <Handle
        type="target"
        position={Position.Left}
        id="event-type-in"
        style={{ top: '75%' }}
        title="Event type reference (optional)"
      />
      <div className="node-header">
        <span className="node-icon source">📥</span>
        <span className="node-title">{data.label}</span>
      </div>
      <div className="node-body">
        {data.topic && (
          <div className="node-prop">
            <span className="prop-label">Topic:</span>
            <span className="prop-value">{data.topic}</span>
          </div>
        )}
        {data.path && (
          <div className="node-prop">
            <span className="prop-label">Path:</span>
            <span className="prop-value">{data.path}</span>
          </div>
        )}
        {data.endpoint && (
          <div className="node-prop">
            <span className="prop-label">Endpoint:</span>
            <span className="prop-value">{data.endpoint}</span>
          </div>
        )}
        {data.eventType && (
          <div className="node-prop">
            <span className="prop-label">Event:</span>
            <span className="prop-value event-ref">{data.eventType}</span>
          </div>
        )}
        {data.inlineUri && (
          <div className="node-prop uri">
            <span className="prop-value">{data.inlineUri}</span>
          </div>
        )}
        {!data.topic && !data.path && !data.endpoint && !data.inlineUri && (
          <div className="node-prop empty">
            <span className="prop-value">Click to configure</span>
          </div>
        )}
      </div>
      {/* Data output handle */}
      <Handle
        type="source"
        position={Position.Right}
        id="data-out"
        title="Data output to Stream"
      />
    </div>
  );
};
