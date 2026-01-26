import { Handle, NodeProps, Position } from '@xyflow/react';
import { GitBranch } from 'lucide-react';
import { memo } from 'react';
import type { StreamDefinition } from '../types';

interface StreamNodeData {
    label: string;
    stream?: StreamDefinition;
}

function StreamNode({ data, selected }: NodeProps<StreamNodeData>) {
    return (
        <div
            className={`px-4 py-3 rounded-lg border-2 border-indigo-500 bg-indigo-900/30 min-w-[180px] ${selected ? 'ring-2 ring-vscode-accent' : ''
                }`}
        >
            <Handle
                type="target"
                position={Position.Left}
                className="!bg-indigo-500 !w-3 !h-3"
            />

            <div className="flex items-center gap-2 mb-2">
                <GitBranch className="w-4 h-4 text-indigo-400" />
                <div className="font-semibold text-sm">{data.label}</div>
            </div>

            {data.stream && (
                <div className="text-xs space-y-1 border-t border-indigo-800/50 pt-2 mt-2">
                    {data.stream.pattern && (
                        <div className="bg-black/30 px-2 py-1 rounded font-mono text-indigo-300 text-[10px]">
                            {data.stream.pattern}
                        </div>
                    )}
                    {data.stream.window && (
                        <div className="text-gray-400">
                            <span className="text-indigo-400">window:</span> {data.stream.window}
                        </div>
                    )}
                    {data.stream.filters && data.stream.filters.length > 0 && (
                        <div className="text-gray-400">
                            <span className="text-indigo-400">filters:</span> {data.stream.filters.length}
                        </div>
                    )}
                </div>
            )}

            <Handle
                type="source"
                position={Position.Right}
                className="!bg-indigo-500 !w-3 !h-3"
            />
        </div>
    );
}

export default memo(StreamNode);
