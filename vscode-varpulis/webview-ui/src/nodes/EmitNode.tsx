import React from 'react';
import { Handle, Position, NodeProps } from '@xyflow/react';
import { isEmitNodeData } from '../types/flow';

export const EmitNode: React.FC<NodeProps> = ({ data, selected }) => {
  if (!isEmitNodeData(data)) {
    return <div className="custom-node emit-node error">Invalid Emit</div>;
  }

  return (
    <div className={`custom-node emit-node ${selected ? 'selected' : ''}`}>
      {/* Input from stream or pattern */}
      <Handle
        type="target"
        position={Position.Left}
        id="data-in"
        title="Input from Stream or Pattern"
      />
      <div className="node-header">
        <span className="node-icon emit">📤</span>
        <span className="node-title">{data.label || '.emit'}</span>
      </div>
      <div className="node-body">
        {data.fields.length === 0 ? (
          <div className="node-prop empty">
            <span className="prop-value">Emit all fields</span>
          </div>
        ) : (
          <div className="emit-fields">
            {data.fields.slice(0, 5).map((field, i) => (
              <div key={i} className="emit-field">
                <span className="field-name">{field.name}:</span>
                <span className="field-expr">{field.expression.length > 15 ? field.expression.slice(0, 15) + '...' : field.expression}</span>
              </div>
            ))}
            {data.fields.length > 5 && (
              <div className="emit-field more">
                +{data.fields.length - 5} more...
              </div>
            )}
          </div>
        )}
      </div>
      {/* Output to sink */}
      <Handle
        type="source"
        position={Position.Right}
        id="emit-out"
        title="Output to Sink"
      />
    </div>
  );
};
