import React from 'react';
import {
  FlowNode,
  ConnectorNodeData,
  SourceNodeData,
  StreamNodeData,
  PatternNodeData,
  EmitNodeData,
  SinkNodeData,
  OperationType,
} from '../types/flow';

interface VplPreviewProps {
  code: string;
  selectedNode: FlowNode | null;
  onUpdateNode: (nodeId: string, data: Partial<FlowNode['data']>) => void;
}

const VplPreview: React.FC<VplPreviewProps> = ({ code, selectedNode, onUpdateNode }) => {
  return (
    <div className="preview-panel">
      <div className="preview-header">
        <h3>VPL Code</h3>
      </div>
      <div className="preview-content">
        {selectedNode ? (
          <NodeEditor node={selectedNode} onUpdate={onUpdateNode} />
        ) : (
          <pre>{code || '# Drag components to the canvas to build your pipeline'}</pre>
        )}
      </div>
    </div>
  );
};

interface NodeEditorProps {
  node: FlowNode;
  onUpdate: (nodeId: string, data: Partial<FlowNode['data']>) => void;
}

const NodeEditor: React.FC<NodeEditorProps> = ({ node, onUpdate }) => {
  const data = node.data as Record<string, unknown>;

  const handleChange = (field: string, value: unknown) => {
    onUpdate(node.id, { [field]: value });
  };

  return (
    <div className="node-editor">
      <h4>Edit {node.type}</h4>
      <div className="editor-field">
        <label>Label:</label>
        <input
          type="text"
          value={(data.label as string) || ''}
          onChange={(e) => handleChange('label', e.target.value)}
        />
      </div>

      {node.type === 'connector' && (
        <ConnectorEditor data={data as ConnectorNodeData} onChange={handleChange} />
      )}

      {node.type === 'source' && (
        <SourceEditor data={data as SourceNodeData} onChange={handleChange} />
      )}

      {node.type === 'stream' && (
        <StreamEditor
          data={data as StreamNodeData}
          onUpdate={(updated) => onUpdate(node.id, updated)}
        />
      )}

      {node.type === 'pattern' && (
        <PatternEditor
          data={data as PatternNodeData}
          onUpdate={(updated) => onUpdate(node.id, updated)}
        />
      )}

      {node.type === 'emit' && (
        <EmitEditor
          data={data as EmitNodeData}
          onUpdate={(updated) => onUpdate(node.id, updated)}
        />
      )}

      {node.type === 'sink' && (
        <SinkEditor data={data as SinkNodeData} onChange={handleChange} />
      )}

      <style>{editorStyles}</style>
    </div>
  );
};

// ============================================
// Connector Editor
// ============================================

const ConnectorEditor: React.FC<{
  data: ConnectorNodeData;
  onChange: (field: string, value: unknown) => void;
}> = ({ data, onChange }) => (
  <>
    <div className="editor-field">
      <label>Type:</label>
      <select
        value={data.connectorType || 'mqtt'}
        onChange={(e) => onChange('connectorType', e.target.value)}
      >
        <option value="mqtt">MQTT</option>
        <option value="kafka">Kafka</option>
        <option value="nats">NATS</option>
        <option value="http">HTTP</option>
        <option value="amqp">AMQP</option>
        <option value="file">File</option>
        <option value="websocket">WebSocket</option>
      </select>
    </div>
    {data.connectorType === 'mqtt' && (
      <>
        <div className="editor-field">
          <label>Broker:</label>
          <input type="text" value={data.broker || ''} onChange={(e) => onChange('broker', e.target.value)} placeholder="localhost" />
        </div>
        <div className="editor-field">
          <label>Port:</label>
          <input type="number" value={data.port || 1883} onChange={(e) => onChange('port', parseInt(e.target.value))} />
        </div>
        <div className="editor-field">
          <label>Client ID:</label>
          <input type="text" value={data.clientId || ''} onChange={(e) => onChange('clientId', e.target.value)} />
        </div>
      </>
    )}
    {data.connectorType === 'kafka' && (
      <>
        <div className="editor-field">
          <label>Brokers:</label>
          <input type="text" value={data.brokers || ''} onChange={(e) => onChange('brokers', e.target.value)} placeholder="localhost:9092" />
        </div>
        <div className="editor-field">
          <label>Group ID:</label>
          <input type="text" value={data.groupId || ''} onChange={(e) => onChange('groupId', e.target.value)} />
        </div>
      </>
    )}
    {data.connectorType === 'nats' && (
      <div className="editor-field">
        <label>URL:</label>
        <input type="text" value={data.natsUrl || ''} onChange={(e) => onChange('natsUrl', e.target.value)} placeholder="nats://localhost:4222" />
      </div>
    )}
    {data.connectorType === 'http' && (
      <div className="editor-field">
        <label>Base URL:</label>
        <input type="text" value={data.baseUrl || ''} onChange={(e) => onChange('baseUrl', e.target.value)} placeholder="https://api.example.com" />
      </div>
    )}
    {data.connectorType === 'file' && (
      <div className="editor-field">
        <label>Path:</label>
        <input type="text" value={data.basePath || ''} onChange={(e) => onChange('basePath', e.target.value)} placeholder="./data" />
      </div>
    )}
  </>
);

// ============================================
// Source Editor
// ============================================

const SourceEditor: React.FC<{
  data: SourceNodeData;
  onChange: (field: string, value: unknown) => void;
}> = ({ data, onChange }) => (
  <>
    <div className="editor-field">
      <label>Event Type:</label>
      <input type="text" value={data.eventType || ''} onChange={(e) => onChange('eventType', e.target.value)} placeholder="EventTypeName" />
    </div>
    <div className="editor-field">
      <label>Topic:</label>
      <input type="text" value={data.topic || ''} onChange={(e) => onChange('topic', e.target.value)} placeholder="sensors/temp/#" />
    </div>
  </>
);

// ============================================
// Stream Editor
// ============================================

const opTypes: OperationType[] = [
  'where', 'window', 'aggregate', 'partition_by', 'select',
  'order_by', 'limit', 'distinct', 'forecast', 'trend_aggregate',
];

const StreamEditor: React.FC<{
  data: StreamNodeData;
  onUpdate: (data: Partial<StreamNodeData>) => void;
}> = ({ data, onUpdate }) => {
  const ops = data.operations || [];

  const updateOp = (idx: number, updates: Partial<StreamNodeData['operations'][0]>) => {
    const newOps = [...ops];
    newOps[idx] = { ...newOps[idx], ...updates };
    onUpdate({ operations: newOps });
  };

  const addOp = () => {
    onUpdate({ operations: [...ops, { type: 'where' }] });
  };

  const removeOp = (idx: number) => {
    onUpdate({ operations: ops.filter((_, i) => i !== idx) });
  };

  return (
    <>
      <div className="editor-section-header">
        <label>Operations:</label>
        <button className="btn-small" onClick={addOp}>+ Add</button>
      </div>
      {ops.map((op, idx) => (
        <div key={idx} className="editor-op-item">
          <div className="editor-op-header">
            <select value={op.type} onChange={(e) => updateOp(idx, { type: e.target.value as OperationType })}>
              {opTypes.map((t) => <option key={t} value={t}>{t}</option>)}
            </select>
            <button className="btn-remove" onClick={() => removeOp(idx)}>x</button>
          </div>
          {(op.type === 'where' || op.type === 'filter') && (
            <input type="text" value={op.condition || ''} onChange={(e) => updateOp(idx, { condition: e.target.value })} placeholder="condition" />
          )}
          {op.type === 'window' && (
            <input type="text" value={op.windowSize || ''} onChange={(e) => updateOp(idx, { windowSize: e.target.value })} placeholder="5m" />
          )}
          {op.type === 'aggregate' && (
            <input type="text" value={op.aggregations || ''} onChange={(e) => updateOp(idx, { aggregations: e.target.value })} placeholder="name: avg(field)" />
          )}
          {(op.type === 'partition_by' || op.type === 'order_by') && (
            <input type="text" value={op.keys || ''} onChange={(e) => updateOp(idx, { keys: e.target.value })} placeholder="field1, field2" />
          )}
          {op.type === 'select' && (
            <input type="text" value={op.projections || ''} onChange={(e) => updateOp(idx, { projections: e.target.value })} placeholder="field1, field2" />
          )}
          {op.type === 'forecast' && (
            <div className="editor-forecast">
              <input type="text" value={op.forecastConfidence || ''} onChange={(e) => updateOp(idx, { forecastConfidence: e.target.value })} placeholder="confidence (0.0-1.0)" />
              <input type="text" value={op.forecastHorizon || ''} onChange={(e) => updateOp(idx, { forecastHorizon: e.target.value })} placeholder="horizon (e.g. 15m)" />
              <input type="text" value={op.forecastWarmup || ''} onChange={(e) => updateOp(idx, { forecastWarmup: e.target.value })} placeholder="warmup (e.g. 500)" />
            </div>
          )}
          {op.type === 'trend_aggregate' && (
            <input type="text" value={op.trendAggregations || ''} onChange={(e) => updateOp(idx, { trendAggregations: e.target.value })} placeholder="name: trend_sum(field)" />
          )}
        </div>
      ))}
    </>
  );
};

// ============================================
// Pattern Editor
// ============================================

const PatternEditor: React.FC<{
  data: PatternNodeData;
  onUpdate: (data: Partial<PatternNodeData>) => void;
}> = ({ data, onUpdate }) => {
  const events = data.events || [];

  const updateEvent = (idx: number, updates: Partial<PatternNodeData['events'][0]>) => {
    const newEvents = [...events];
    newEvents[idx] = { ...newEvents[idx], ...updates };
    onUpdate({ events: newEvents });
  };

  const addEvent = () => {
    onUpdate({ events: [...events, { eventType: 'EventType' }] });
  };

  const removeEvent = (idx: number) => {
    onUpdate({ events: events.filter((_, i) => i !== idx) });
  };

  return (
    <>
      <div className="editor-field">
        <label>Within:</label>
        <input type="text" value={data.within || ''} onChange={(e) => onUpdate({ within: e.target.value })} placeholder="5m" />
      </div>
      <div className="editor-section-header">
        <label>Sequence Events:</label>
        <button className="btn-small" onClick={addEvent}>+ Add</button>
      </div>
      {events.map((evt, idx) => (
        <div key={idx} className="editor-op-item">
          <div className="editor-op-header">
            <span className="editor-op-label">{idx > 0 ? '-> ' : ''}{evt.eventType}</span>
            <button className="btn-remove" onClick={() => removeEvent(idx)}>x</button>
          </div>
          <input type="text" value={evt.eventType} onChange={(e) => updateEvent(idx, { eventType: e.target.value })} placeholder="EventType" />
          <input type="text" value={evt.alias || ''} onChange={(e) => updateEvent(idx, { alias: e.target.value })} placeholder="alias" />
          <input type="text" value={evt.condition || ''} onChange={(e) => updateEvent(idx, { condition: e.target.value })} placeholder="where condition" />
        </div>
      ))}
      {data.forecast !== undefined && (
        <>
          <div className="editor-section-header"><label>Forecast:</label></div>
          <div className="editor-forecast">
            <input type="text" value={data.forecast?.confidence || ''} onChange={(e) => onUpdate({ forecast: { ...data.forecast, confidence: e.target.value } })} placeholder="confidence" />
            <input type="text" value={data.forecast?.horizon || ''} onChange={(e) => onUpdate({ forecast: { ...data.forecast, horizon: e.target.value } })} placeholder="horizon" />
            <input type="text" value={data.forecast?.warmup || ''} onChange={(e) => onUpdate({ forecast: { ...data.forecast, warmup: e.target.value } })} placeholder="warmup" />
          </div>
        </>
      )}
    </>
  );
};

// ============================================
// Emit Editor
// ============================================

const EmitEditor: React.FC<{
  data: EmitNodeData;
  onUpdate: (data: Partial<EmitNodeData>) => void;
}> = ({ data, onUpdate }) => {
  const fields = data.fields || [];

  const updateField = (idx: number, updates: Partial<EmitNodeData['fields'][0]>) => {
    const newFields = [...fields];
    newFields[idx] = { ...newFields[idx], ...updates };
    onUpdate({ fields: newFields });
  };

  const addField = () => {
    onUpdate({ fields: [...fields, { name: '', expression: '' }] });
  };

  const removeField = (idx: number) => {
    onUpdate({ fields: fields.filter((_, i) => i !== idx) });
  };

  return (
    <>
      <div className="editor-field">
        <label>
          <input type="checkbox" checked={data.emitAll || false} onChange={(e) => onUpdate({ emitAll: e.target.checked })} />
          {' '}Emit all fields
        </label>
      </div>
      {!data.emitAll && (
        <>
          <div className="editor-section-header">
            <label>Fields:</label>
            <button className="btn-small" onClick={addField}>+ Add</button>
          </div>
          {fields.map((f, idx) => (
            <div key={idx} className="editor-op-item">
              <div className="editor-op-header">
                <span className="editor-op-label">{f.name || '(unnamed)'}</span>
                <button className="btn-remove" onClick={() => removeField(idx)}>x</button>
              </div>
              <input type="text" value={f.name} onChange={(e) => updateField(idx, { name: e.target.value })} placeholder="field name" />
              <input type="text" value={f.expression} onChange={(e) => updateField(idx, { expression: e.target.value })} placeholder="expression" />
            </div>
          ))}
        </>
      )}
    </>
  );
};

// ============================================
// Sink Editor
// ============================================

const SinkEditor: React.FC<{
  data: SinkNodeData;
  onChange: (field: string, value: unknown) => void;
}> = ({ data, onChange }) => (
  <>
    <div className="editor-field">
      <label>Sink Type:</label>
      <select value={data.sinkType || 'console'} onChange={(e) => onChange('sinkType', e.target.value)}>
        <option value="console">Console</option>
        <option value="topic">Topic</option>
        <option value="file">File</option>
        <option value="log">Log</option>
        <option value="tap">Tap (Metrics)</option>
        <option value="endpoint">HTTP Endpoint</option>
      </select>
    </div>
    {data.sinkType === 'topic' && (
      <div className="editor-field">
        <label>Topic:</label>
        <input type="text" value={data.topic || ''} onChange={(e) => onChange('topic', e.target.value)} placeholder="output-events" />
      </div>
    )}
    {data.sinkType === 'file' && (
      <div className="editor-field">
        <label>Path:</label>
        <input type="text" value={data.path || ''} onChange={(e) => onChange('path', e.target.value)} placeholder="./output.json" />
      </div>
    )}
    {data.sinkType === 'endpoint' && (
      <>
        <div className="editor-field">
          <label>Endpoint:</label>
          <input type="text" value={data.endpoint || ''} onChange={(e) => onChange('endpoint', e.target.value)} placeholder="/api/events" />
        </div>
        <div className="editor-field">
          <label>Method:</label>
          <select value={data.method || 'POST'} onChange={(e) => onChange('method', e.target.value)}>
            <option value="POST">POST</option>
            <option value="PUT">PUT</option>
            <option value="PATCH">PATCH</option>
          </select>
        </div>
      </>
    )}
    {data.sinkType === 'tap' && (
      <>
        <div className="editor-field">
          <label>Counter:</label>
          <input type="text" value={data.counter || ''} onChange={(e) => onChange('counter', e.target.value)} placeholder="events_total" />
        </div>
        <div className="editor-field">
          <label>Labels:</label>
          <input type="text" value={data.labels || ''} onChange={(e) => onChange('labels', e.target.value)} placeholder="field1, field2" />
        </div>
      </>
    )}
    {data.sinkType === 'log' && (
      <div className="editor-field">
        <label>Level:</label>
        <select value={data.logLevel || 'info'} onChange={(e) => onChange('logLevel', e.target.value)}>
          <option value="debug">Debug</option>
          <option value="info">Info</option>
          <option value="warn">Warn</option>
          <option value="error">Error</option>
        </select>
      </div>
    )}
  </>
);

// ============================================
// Styles
// ============================================

const editorStyles = `
  .node-editor {
    font-size: 12px;
  }
  .node-editor h4 {
    margin: 0 0 15px 0;
    text-transform: capitalize;
  }
  .editor-field {
    margin-bottom: 10px;
  }
  .editor-field label {
    display: block;
    margin-bottom: 4px;
    color: var(--vscode-descriptionForeground, #888);
  }
  .editor-field input[type="text"],
  .editor-field input[type="number"],
  .editor-field select {
    width: 100%;
    padding: 6px 8px;
    background: var(--vscode-input-background, #3c3c3c);
    border: 1px solid var(--vscode-input-border, #3c3c3c);
    color: var(--vscode-input-foreground, #cccccc);
    border-radius: 2px;
    box-sizing: border-box;
  }
  .editor-field input:focus,
  .editor-field select:focus {
    outline: none;
    border-color: var(--vscode-focusBorder, #007fd4);
  }
  .editor-section-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin: 12px 0 6px;
  }
  .editor-section-header label {
    color: var(--vscode-descriptionForeground, #888);
    font-weight: bold;
  }
  .btn-small {
    padding: 2px 8px;
    font-size: 11px;
    background: var(--vscode-button-background, #0e639c);
    color: var(--vscode-button-foreground, #fff);
    border: none;
    border-radius: 2px;
    cursor: pointer;
  }
  .btn-small:hover {
    background: var(--vscode-button-hoverBackground, #1177bb);
  }
  .btn-remove {
    padding: 1px 6px;
    font-size: 10px;
    background: transparent;
    color: var(--vscode-errorForeground, #f48771);
    border: 1px solid var(--vscode-errorForeground, #f48771);
    border-radius: 2px;
    cursor: pointer;
  }
  .editor-op-item {
    margin-bottom: 8px;
    padding: 6px;
    background: var(--vscode-editor-background, #1e1e1e);
    border: 1px solid var(--vscode-panel-border, #444);
    border-radius: 3px;
  }
  .editor-op-item input,
  .editor-op-item select {
    width: 100%;
    padding: 4px 6px;
    margin-top: 4px;
    background: var(--vscode-input-background, #3c3c3c);
    border: 1px solid var(--vscode-input-border, #3c3c3c);
    color: var(--vscode-input-foreground, #cccccc);
    border-radius: 2px;
    box-sizing: border-box;
  }
  .editor-op-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 4px;
  }
  .editor-op-label {
    font-family: monospace;
    font-size: 11px;
    color: var(--vscode-textLink-foreground, #3794ff);
  }
  .editor-forecast input {
    margin-bottom: 4px;
  }
`;

export default VplPreview;
