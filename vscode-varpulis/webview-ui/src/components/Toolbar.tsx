interface ToolbarProps {
    onAddNode: (type: string) => void;
    onSave: () => void;
    onExport: () => void;
    onRun: () => void;
    onClear: () => void;
    onImport?: () => void;
}

const nodeOptions = [
    { type: 'source', label: 'Source', icon: '📥', color: '#22c55e' },
    { type: 'sink', label: 'Sink', icon: '📤', color: '#eab308' },
    { type: 'event', label: 'Event', icon: '📋', color: '#f59e0b' },
    { type: 'stream', label: 'Stream', icon: '🌊', color: '#6366f1' },
    { type: 'pattern', label: 'Pattern', icon: '🔍', color: '#ec4899' },
];

export default function Toolbar({ onAddNode, onSave, onExport, onRun, onClear, onImport }: ToolbarProps) {
    return (
        <div className="toolbar">
            <div className="toolbar-group">
                <span className="toolbar-label">Add:</span>
                {nodeOptions.map(opt => (
                    <button
                        key={opt.type}
                        onClick={() => onAddNode(opt.type)}
                        className="toolbar-btn"
                        style={{ color: opt.color }}
                        title={`Add ${opt.label}`}
                    >
                        <span>{opt.icon}</span>
                        <span>{opt.label}</span>
                    </button>
                ))}
            </div>

            <div className="toolbar-group">
                <button
                    onClick={onRun}
                    className="toolbar-btn primary"
                    title="Run Flow"
                >
                    <span>▶</span>
                    <span>Run</span>
                </button>
                <button
                    onClick={onExport}
                    className="toolbar-btn"
                    title="Export to VPL"
                >
                    <span>⬇</span>
                    <span>Export</span>
                </button>
                {onImport && (
                    <button
                        onClick={onImport}
                        className="toolbar-btn"
                        title="Import VPL"
                    >
                        <span>⬆</span>
                        <span>Import</span>
                    </button>
                )}
                <button
                    onClick={onSave}
                    className="toolbar-btn"
                    title="Save Flow"
                >
                    <span>💾</span>
                    <span>Save</span>
                </button>
                <button
                    onClick={onClear}
                    className="toolbar-btn danger"
                    title="Clear All"
                >
                    <span>🗑</span>
                </button>
            </div>
        </div>
    );
}
