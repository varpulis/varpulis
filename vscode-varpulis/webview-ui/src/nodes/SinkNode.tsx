import React from 'react';
import { Handle, Position, NodeProps } from '@xyflow/react';
import { isSinkNodeData } from '../types/flow';

export const SinkNode: React.FC<NodeProps> = ({ data, selected }) => {
  if (!isSinkNodeData(data)) {
    return <div className="custom-node sink-node error">Invalid Sink</div>;
  }

  const sinkIcons: Record<string, string> = {
    topic: '📤',
    endpoint: '🌐',
    file: '📄',
    console: '💻',
    log: '📝',
    tap: '📊',
  };

  return (
    <div className={`custom-node sink-node ${selected ? 'selected' : ''}`}>
      {/* Data input handle */}
      <Handle
        type="target"
        position={Position.Left}
        id="data-in"
        title="Data input from Emit or Stream"
      />
      {/* Connector input handle (top) */}
      <Handle
        type="target"
        position={Position.Top}
        id="connector-in"
        style={{ left: '50%' }}
        title="Connect from Connector (for topic/endpoint sinks)"
      />
      <div className="node-header">
        <span className="node-icon sink">{sinkIcons[data.sinkType] || '📤'}</span>
        <span className="node-title">{data.label}</span>
      </div>
      <div className="node-body">
        <div className="node-prop">
          <span className="prop-label">Type:</span>
          <span className="prop-value sink-type">{data.sinkType.toUpperCase()}</span>
        </div>
        {data.topic && (
          <div className="node-prop">
            <span className="prop-label">Topic:</span>
            <span className="prop-value">{data.topic}</span>
          </div>
        )}
        {data.endpoint && (
          <div className="node-prop">
            <span className="prop-label">Endpoint:</span>
            <span className="prop-value">{data.method || 'POST'} {data.endpoint}</span>
          </div>
        )}
        {data.path && (
          <div className="node-prop">
            <span className="prop-label">Path:</span>
            <span className="prop-value">{data.path}</span>
          </div>
        )}
        {data.counter && (
          <div className="node-prop">
            <span className="prop-label">Counter:</span>
            <span className="prop-value">{data.counter}</span>
          </div>
        )}
        {data.logLevel && (
          <div className="node-prop">
            <span className="prop-label">Level:</span>
            <span className="prop-value">{data.logLevel}</span>
          </div>
        )}
        {data.inlineUri && (
          <div className="node-prop uri">
            <span className="prop-value">{data.inlineUri}</span>
          </div>
        )}
      </div>
    </div>
  );
};
