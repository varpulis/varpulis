import { Handle, NodeProps, Position } from '@xyflow/react';
import { Zap } from 'lucide-react';
import { memo } from 'react';
import type { EventDefinition } from '../types';

interface EventNodeData {
    label: string;
    event?: EventDefinition;
}

function EventNode({ data, selected }: NodeProps<EventNodeData>) {
    return (
        <div
            className={`px-4 py-3 rounded-lg border-2 border-amber-500 bg-amber-900/30 min-w-[140px] ${selected ? 'ring-2 ring-vscode-accent' : ''
                }`}
        >
            <Handle
                type="target"
                position={Position.Left}
                className="!bg-amber-500 !w-3 !h-3"
            />

            <div className="flex items-center gap-2 mb-2">
                <Zap className="w-4 h-4 text-amber-400" />
                <div className="font-semibold text-sm">{data.label}</div>
            </div>

            {data.event && data.event.fields.length > 0 && (
                <div className="text-xs text-gray-400 space-y-0.5 border-t border-amber-800/50 pt-2 mt-2">
                    {data.event.fields.slice(0, 4).map((field) => (
                        <div key={field.name} className="flex justify-between">
                            <span className="text-amber-300">{field.name}</span>
                            <span className="text-gray-500">{field.type}</span>
                        </div>
                    ))}
                    {data.event.fields.length > 4 && (
                        <div className="text-gray-500 text-center">
                            +{data.event.fields.length - 4} more
                        </div>
                    )}
                </div>
            )}

            <Handle
                type="source"
                position={Position.Right}
                className="!bg-amber-500 !w-3 !h-3"
            />
        </div>
    );
}

export default memo(EventNode);
