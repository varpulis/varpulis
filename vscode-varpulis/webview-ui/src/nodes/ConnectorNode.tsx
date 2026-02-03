import React from 'react';
import { Handle, Position, NodeProps } from '@xyflow/react';
import { isConnectorNodeData } from '../types/flow';

export const ConnectorNode: React.FC<NodeProps> = ({ data, selected }) => {
  if (!isConnectorNodeData(data)) {
    return <div className="custom-node connector-node error">Invalid Connector</div>;
  }

  const connectorIcons: Record<string, string> = {
    mqtt: '📡',
    kafka: '📨',
    amqp: '🐰',
    http: '🌐',
    file: '📁',
    websocket: '🔌',
  };

  const connectorColors: Record<string, string> = {
    mqtt: '#00bcd4',
    kafka: '#ff5722',
    amqp: '#ff9800',
    http: '#4caf50',
    file: '#9c27b0',
    websocket: '#2196f3',
  };

  const color = connectorColors[data.connectorType] || '#888';

  return (
    <div
      className={`custom-node connector-node ${selected ? 'selected' : ''}`}
      style={{ borderColor: color }}
    >
      <div className="node-header" style={{ borderBottomColor: color + '44' }}>
        <span className="node-icon connector" style={{ backgroundColor: color + '33', color }}>
          {connectorIcons[data.connectorType] || '🔗'}
        </span>
        <span className="node-title">{data.label}</span>
      </div>
      <div className="node-body">
        <div className="node-prop">
          <span className="prop-label">Type:</span>
          <span className="prop-value connector-type" style={{ color }}>{data.connectorType.toUpperCase()}</span>
        </div>
        {data.connectorType === 'mqtt' && (
          <div className="node-prop">
            <span className="prop-label">Broker:</span>
            <span className="prop-value">{data.broker || 'localhost'}:{data.port || 1883}</span>
          </div>
        )}
        {data.connectorType === 'kafka' && (
          <div className="node-prop">
            <span className="prop-label">Brokers:</span>
            <span className="prop-value">{data.brokers || 'localhost:9092'}</span>
          </div>
        )}
        {data.connectorType === 'http' && (
          <div className="node-prop">
            <span className="prop-label">URL:</span>
            <span className="prop-value">{data.baseUrl || 'https://...'}</span>
          </div>
        )}
        {data.connectorType === 'amqp' && (
          <div className="node-prop">
            <span className="prop-label">Broker:</span>
            <span className="prop-value">{data.broker || 'localhost'}:{data.port || 5672}</span>
          </div>
        )}
        {data.connectorType === 'file' && (
          <div className="node-prop">
            <span className="prop-label">Path:</span>
            <span className="prop-value">{data.basePath || './'}</span>
          </div>
        )}
        {data.connectorType === 'websocket' && (
          <div className="node-prop">
            <span className="prop-label">URL:</span>
            <span className="prop-value">{data.wsUrl || 'ws://...'}</span>
          </div>
        )}
      </div>
      {/* Sources and Sinks connect to this connector */}
      <Handle
        type="source"
        position={Position.Right}
        id="connector-out"
        title="Connect to Source or Sink"
        style={{ background: color }}
      />
    </div>
  );
};
