import {
    FileDown,
    GitBranch,
    Play,
    Radio,
    Save,
    Terminal,
    Trash2,
    Workflow,
    Zap
} from 'lucide-react';

interface ToolbarProps {
    onAddNode: (type: string) => void;
    onSave: () => void;
    onExport: () => void;
    onRun: () => void;
    onClear: () => void;
}

const nodeOptions = [
    { type: 'source', label: 'Source', icon: <Radio className="w-4 h-4" />, color: 'text-green-400' },
    { type: 'sink', label: 'Sink', icon: <Terminal className="w-4 h-4" />, color: 'text-yellow-400' },
    { type: 'event', label: 'Event', icon: <Zap className="w-4 h-4" />, color: 'text-amber-400' },
    { type: 'stream', label: 'Stream', icon: <GitBranch className="w-4 h-4" />, color: 'text-indigo-400' },
    { type: 'pattern', label: 'Pattern', icon: <Workflow className="w-4 h-4" />, color: 'text-teal-400' },
];

export default function Toolbar({ onAddNode, onSave, onExport, onRun, onClear }: ToolbarProps) {
    return (
        <div className="flex items-center justify-between px-4 py-2 bg-black/30 border-b border-vscode-border">
            <div className="flex items-center gap-1">
                <span className="text-sm text-gray-400 mr-2">Add:</span>
                {nodeOptions.map(opt => (
                    <button
                        key={opt.type}
                        onClick={() => onAddNode(opt.type)}
                        className={`flex items-center gap-1.5 px-3 py-1.5 text-sm rounded hover:bg-white/10 transition-colors ${opt.color}`}
                        title={`Add ${opt.label}`}
                    >
                        {opt.icon}
                        <span className="hidden sm:inline">{opt.label}</span>
                    </button>
                ))}
            </div>

            <div className="flex items-center gap-1">
                <button
                    onClick={onRun}
                    className="flex items-center gap-1.5 px-3 py-1.5 text-sm bg-green-600 hover:bg-green-700 rounded transition-colors"
                    title="Run Flow"
                >
                    <Play className="w-4 h-4" />
                    <span className="hidden sm:inline">Run</span>
                </button>
                <button
                    onClick={onExport}
                    className="flex items-center gap-1.5 px-3 py-1.5 text-sm hover:bg-white/10 rounded transition-colors"
                    title="Export to VPL"
                >
                    <FileDown className="w-4 h-4" />
                    <span className="hidden sm:inline">Export</span>
                </button>
                <button
                    onClick={onSave}
                    className="flex items-center gap-1.5 px-3 py-1.5 text-sm hover:bg-white/10 rounded transition-colors"
                    title="Save Flow"
                >
                    <Save className="w-4 h-4" />
                    <span className="hidden sm:inline">Save</span>
                </button>
                <button
                    onClick={onClear}
                    className="flex items-center gap-1.5 px-3 py-1.5 text-sm text-red-400 hover:bg-red-900/30 rounded transition-colors"
                    title="Clear All"
                >
                    <Trash2 className="w-4 h-4" />
                </button>
            </div>
        </div>
    );
}
