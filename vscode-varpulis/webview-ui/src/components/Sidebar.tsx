import { ChevronDown, ChevronRight, Database, FileText, GitBranch, Globe, MessageSquare, Radio, Terminal, Workflow, Zap } from 'lucide-react';
import { useState } from 'react';

interface SidebarProps {
    onDragStart: (event: React.DragEvent, nodeType: string, data?: Record<string, unknown>) => void;
}

const categories = [
    {
        name: 'Sources',
        icon: <Radio className="w-4 h-4" />,
        items: [
            { type: 'source', label: 'MQTT Source', icon: <Radio className="w-4 h-4 text-green-400" />, data: { connector: { type: 'mqtt' } } },
            { type: 'source', label: 'Kafka Source', icon: <Database className="w-4 h-4 text-orange-400" />, data: { connector: { type: 'kafka' } } },
            { type: 'source', label: 'AMQP Source', icon: <MessageSquare className="w-4 h-4 text-purple-400" />, data: { connector: { type: 'amqp' } } },
            { type: 'source', label: 'File Source', icon: <FileText className="w-4 h-4 text-blue-400" />, data: { connector: { type: 'file' } } },
            { type: 'source', label: 'HTTP Source', icon: <Globe className="w-4 h-4 text-cyan-400" />, data: { connector: { type: 'http' } } },
        ],
    },
    {
        name: 'Sinks',
        icon: <Terminal className="w-4 h-4" />,
        items: [
            { type: 'sink', label: 'Console Sink', icon: <Terminal className="w-4 h-4 text-yellow-400" />, data: { connector: { type: 'console' } } },
            { type: 'sink', label: 'MQTT Sink', icon: <Radio className="w-4 h-4 text-green-400" />, data: { connector: { type: 'mqtt' } } },
            { type: 'sink', label: 'Kafka Sink', icon: <Database className="w-4 h-4 text-orange-400" />, data: { connector: { type: 'kafka' } } },
            { type: 'sink', label: 'File Sink', icon: <FileText className="w-4 h-4 text-blue-400" />, data: { connector: { type: 'file' } } },
            { type: 'sink', label: 'HTTP Sink', icon: <Globe className="w-4 h-4 text-cyan-400" />, data: { connector: { type: 'http' } } },
        ],
    },
    {
        name: 'Events',
        icon: <Zap className="w-4 h-4" />,
        items: [
            { type: 'event', label: 'Event Type', icon: <Zap className="w-4 h-4 text-amber-400" />, data: {} },
        ],
    },
    {
        name: 'Streams',
        icon: <GitBranch className="w-4 h-4" />,
        items: [
            { type: 'stream', label: 'Stream', icon: <GitBranch className="w-4 h-4 text-indigo-400" />, data: {} },
        ],
    },
    {
        name: 'Patterns',
        icon: <Workflow className="w-4 h-4" />,
        items: [
            { type: 'pattern', label: 'Sequence (A→B)', icon: <Workflow className="w-4 h-4 text-teal-400" />, data: { patternType: 'sequence' } },
            { type: 'pattern', label: 'AND', icon: <Workflow className="w-4 h-4 text-emerald-400" />, data: { patternType: 'and' } },
            { type: 'pattern', label: 'OR', icon: <Workflow className="w-4 h-4 text-sky-400" />, data: { patternType: 'or' } },
            { type: 'pattern', label: 'NOT', icon: <Workflow className="w-4 h-4 text-rose-400" />, data: { patternType: 'not' } },
            { type: 'pattern', label: 'Kleene+', icon: <Workflow className="w-4 h-4 text-violet-400" />, data: { patternType: 'kleene' } },
        ],
    },
];

export default function Sidebar({ onDragStart }: SidebarProps) {
    const [expanded, setExpanded] = useState<Record<string, boolean>>({
        Sources: true,
        Sinks: true,
        Events: true,
        Streams: true,
        Patterns: true,
    });

    const toggleCategory = (name: string) => {
        setExpanded(prev => ({ ...prev, [name]: !prev[name] }));
    };

    return (
        <div className="w-56 bg-black/20 border-r border-vscode-border overflow-y-auto">
            <div className="p-3 border-b border-vscode-border">
                <h2 className="text-sm font-semibold text-gray-300">Components</h2>
                <p className="text-xs text-gray-500 mt-1">Drag to canvas</p>
            </div>

            {categories.map(category => (
                <div key={category.name} className="border-b border-vscode-border/50">
                    <button
                        onClick={() => toggleCategory(category.name)}
                        className="w-full flex items-center gap-2 px-3 py-2 hover:bg-white/5 transition-colors"
                    >
                        {expanded[category.name] ? (
                            <ChevronDown className="w-3 h-3 text-gray-500" />
                        ) : (
                            <ChevronRight className="w-3 h-3 text-gray-500" />
                        )}
                        {category.icon}
                        <span className="text-sm">{category.name}</span>
                    </button>

                    {expanded[category.name] && (
                        <div className="pb-2">
                            {category.items.map((item, idx) => (
                                <div
                                    key={`${item.type}-${idx}`}
                                    draggable
                                    onDragStart={e => onDragStart(e, item.type, item.data)}
                                    className="flex items-center gap-2 px-4 py-1.5 mx-2 text-sm rounded cursor-grab hover:bg-white/10 transition-colors"
                                >
                                    {item.icon}
                                    <span className="text-gray-300">{item.label}</span>
                                </div>
                            ))}
                        </div>
                    )}
                </div>
            ))}
        </div>
    );
}
