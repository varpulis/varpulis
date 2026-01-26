import { Handle, NodeProps, Position } from '@xyflow/react';
import { Workflow } from 'lucide-react';
import { memo } from 'react';

interface PatternNodeData {
    label: string;
    patternType?: 'sequence' | 'and' | 'or' | 'not' | 'kleene';
    pattern?: string;
}

const patternColors: Record<string, string> = {
    sequence: 'border-teal-500 bg-teal-900/30',
    and: 'border-emerald-500 bg-emerald-900/30',
    or: 'border-sky-500 bg-sky-900/30',
    not: 'border-rose-500 bg-rose-900/30',
    kleene: 'border-violet-500 bg-violet-900/30',
    default: 'border-gray-500 bg-gray-900/30',
};

const patternLabels: Record<string, string> = {
    sequence: 'SEQ',
    and: 'AND',
    or: 'OR',
    not: 'NOT',
    kleene: 'K+',
};

function PatternNode({ data, selected }: NodeProps<PatternNodeData>) {
    const patternType = data.patternType || 'sequence';
    const colorClass = patternColors[patternType] || patternColors.default;

    return (
        <div
            className={`px-4 py-3 rounded-lg border-2 min-w-[140px] ${colorClass} ${selected ? 'ring-2 ring-vscode-accent' : ''
                }`}
        >
            <Handle
                type="target"
                position={Position.Left}
                id="input"
                className="!bg-white !w-3 !h-3"
            />

            <div className="flex items-center gap-2 mb-1">
                <Workflow className="w-4 h-4" />
                <div className="font-semibold text-sm">{data.label}</div>
            </div>

            <div className="flex items-center gap-2">
                <span className="text-[10px] uppercase tracking-wide px-1.5 py-0.5 bg-black/30 rounded">
                    {patternLabels[patternType] || patternType}
                </span>
            </div>

            {data.pattern && (
                <div className="mt-2 text-[10px] font-mono bg-black/30 px-2 py-1 rounded text-gray-300 max-w-[160px] truncate">
                    {data.pattern}
                </div>
            )}

            <Handle
                type="source"
                position={Position.Right}
                id="output"
                className="!bg-white !w-3 !h-3"
            />
        </div>
    );
}

export default memo(PatternNode);
