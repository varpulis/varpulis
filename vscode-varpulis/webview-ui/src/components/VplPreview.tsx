import React from 'react';
import { FlowNode } from '../types/flow';

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
  const handleChange = (field: string, value: string) => {
    onUpdate(node.id, { [field]: value });
  };

  const data = node.data as Record<string, unknown>;

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
      
      {node.type === 'source' && (
        <>
          <div className="editor-field">
            <label>Source Type:</label>
            <select
              value={(data.sourceType as string) || 'mqtt'}
              onChange={(e) => handleChange('sourceType', e.target.value)}
            >
              <option value="mqtt">MQTT</option>
              <option value="kafka">Kafka</option>
              <option value="file">File</option>
              <option value="http">HTTP</option>
            </select>
          </div>
          <div className="editor-field">
            <label>URI:</label>
            <input
              type="text"
              value={(data.uri as string) || ''}
              onChange={(e) => handleChange('uri', e.target.value)}
            />
          </div>
        </>
      )}

      {node.type === 'stream' && (
        <>
          <div className="editor-field">
            <label>Where:</label>
            <input
              type="text"
              value={(data.where as string) || ''}
              onChange={(e) => handleChange('where', e.target.value)}
              placeholder="e.g., value > 10"
            />
          </div>
          <div className="editor-field">
            <label>Window:</label>
            <input
              type="text"
              value={(data.window as string) || ''}
              onChange={(e) => handleChange('window', e.target.value)}
              placeholder="e.g., tumbling(5m)"
            />
          </div>
        </>
      )}

      {node.type === 'pattern' && (
        <>
          <div className="editor-field">
            <label>Pattern Type:</label>
            <select
              value={(data.patternType as string) || 'SEQ'}
              onChange={(e) => handleChange('patternType', e.target.value)}
            >
              <option value="SEQ">SEQ (Sequence)</option>
              <option value="AND">AND (All)</option>
              <option value="OR">OR (Any)</option>
            </select>
          </div>
          <div className="editor-field">
            <label>Within:</label>
            <input
              type="text"
              value={(data.within as string) || '5m'}
              onChange={(e) => handleChange('within', e.target.value)}
              placeholder="e.g., 5m, 1h"
            />
          </div>
        </>
      )}

      {node.type === 'sink' && (
        <>
          <div className="editor-field">
            <label>Sink Type:</label>
            <select
              value={(data.sinkType as string) || 'console'}
              onChange={(e) => handleChange('sinkType', e.target.value)}
            >
              <option value="console">Console</option>
              <option value="file">File</option>
              <option value="mqtt">MQTT</option>
              <option value="kafka">Kafka</option>
              <option value="http">HTTP</option>
            </select>
          </div>
          {(data.sinkType !== 'console') && (
            <div className="editor-field">
              <label>URI:</label>
              <input
                type="text"
                value={(data.uri as string) || ''}
                onChange={(e) => handleChange('uri', e.target.value)}
              />
            </div>
          )}
        </>
      )}

      <style>{`
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
        .editor-field input,
        .editor-field select {
          width: 100%;
          padding: 6px 8px;
          background: var(--vscode-input-background, #3c3c3c);
          border: 1px solid var(--vscode-input-border, #3c3c3c);
          color: var(--vscode-input-foreground, #cccccc);
          border-radius: 2px;
        }
        .editor-field input:focus,
        .editor-field select:focus {
          outline: none;
          border-color: var(--vscode-focusBorder, #007fd4);
        }
      `}</style>
    </div>
  );
};

export default VplPreview;
