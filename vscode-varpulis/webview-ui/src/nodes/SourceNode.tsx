import { Handle, NodeProps, Position } from '@xyflow/react';
import { Database, FileText, Globe, MessageSquare, Radio } from 'lucide-react';
import { memo } from 'react';
import type { ConnectorConfig } from '../types';

interface SourceNodeData {
    label: string;
    connector?: ConnectorConfig;
    onConfigure?: () => void;
}

const connectorIcons: Record<string, React.ReactNode> = {
    mqtt: <Radio className="w-4 h-4" />,
    kafka: <Database className="w-4 h-4" />,
    amqp: <MessageSquare className="w-4 h-4" />,
    file: <FileText className="w-4 h-4" />,
    http: <Globe className="w-4 h-4" />,
};

const connectorColors: Record<string, string> = {
    mqtt: 'border-green-500 bg-green-900/30',
    kafka: 'border-orange-500 bg-orange-900/30',
    amqp: 'border-purple-500 bg-purple-900/30',
    file: 'border-blue-500 bg-blue-900/30',
    http: 'border-cyan-500 bg-cyan-900/30',
    default: 'border-gray-500 bg-gray-900/30',
};

function SourceNode({ data, selected }: NodeProps<SourceNodeData>) {
    const connectorType = data.connector?.type || 'default';
    const colorClass = connectorColors[connectorType] || connectorColors.default;
    const icon = connectorIcons[connectorType] || <Database className="w-4 h-4" />;

    return (
        <div
            className={`px-4 py-3 rounded-lg border-2 min-w-[160px] ${colorClass} ${selected ? 'ring-2 ring-vscode-accent' : ''
                }`}
        >
            <div className="flex items-center gap-2 mb-2">
                <div className="p-1.5 bg-black/30 rounded">{icon}</div>
                <div className="font-semibold text-sm">{data.label}</div>
            </div>

            {data.connector && (
                <div className="text-xs text-gray-400 space-y-0.5">
                    <div className="uppercase tracking-wide text-[10px] text-gray-500">
                        {data.connector.type}
                    </div>
                    {data.connector.host && (
                        <div>{data.connector.host}:{data.connector.port || 1883}</div>
                    )}
                    {data.connector.topic && (
                        <div className="truncate max-w-[140px]">{data.connector.topic}</div>
                    )}
                    {data.connector.path && (
                        <div className="truncate max-w-[140px]">{data.connector.path}</div>
                    )}
                </div>
            )}

            {!data.connector && (
                <div className="text-xs text-gray-500 italic">Click to configure</div>
            )}

            <Handle
                type="source"
                position={Position.Right}
                className="!bg-green-500 !w-3 !h-3"
            />
        </div>
    );
}

export default memo(SourceNode);
