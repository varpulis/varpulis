import { Database, FileText, Globe, MessageSquare, Radio, Terminal, X } from 'lucide-react';
import { useState } from 'react';
import type { ConnectorConfig, ConnectorType } from '../types';

interface ConnectorPanelProps {
    connector?: ConnectorConfig;
    mode: 'source' | 'sink';
    onSave: (config: ConnectorConfig) => void;
    onCancel: () => void;
}

const connectorOptions: { type: ConnectorType; label: string; icon: React.ReactNode; modes: ('source' | 'sink')[] }[] = [
    { type: 'mqtt', label: 'MQTT', icon: <Radio className="w-5 h-5" />, modes: ['source', 'sink'] },
    { type: 'kafka', label: 'Kafka', icon: <Database className="w-5 h-5" />, modes: ['source', 'sink'] },
    { type: 'amqp', label: 'AMQP/RabbitMQ', icon: <MessageSquare className="w-5 h-5" />, modes: ['source', 'sink'] },
    { type: 'file', label: 'File (.evt/.json)', icon: <FileText className="w-5 h-5" />, modes: ['source', 'sink'] },
    { type: 'http', label: 'HTTP/Webhook', icon: <Globe className="w-5 h-5" />, modes: ['source', 'sink'] },
    { type: 'console', label: 'Console', icon: <Terminal className="w-5 h-5" />, modes: ['sink'] },
];

export default function ConnectorPanel({ connector, mode, onSave, onCancel }: ConnectorPanelProps) {
    const [config, setConfig] = useState<ConnectorConfig>(
        connector || { type: 'mqtt', name: '' }
    );

    const availableConnectors = connectorOptions.filter(c => c.modes.includes(mode));

    const updateConfig = (updates: Partial<ConnectorConfig>) => {
        setConfig(prev => ({ ...prev, ...updates }));
    };

    return (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
            <div className="bg-vscode-bg border border-vscode-border rounded-lg w-[480px] max-h-[80vh] overflow-hidden">
                <div className="flex items-center justify-between px-4 py-3 border-b border-vscode-border">
                    <h2 className="font-semibold">Configure {mode === 'source' ? 'Source' : 'Sink'}</h2>
                    <button onClick={onCancel} className="p-1 hover:bg-white/10 rounded">
                        <X className="w-4 h-4" />
                    </button>
                </div>

                <div className="p-4 space-y-4 overflow-y-auto max-h-[60vh]">
                    {/* Connector Type Selection */}
                    <div>
                        <label className="block text-sm text-gray-400 mb-2">Connector Type</label>
                        <div className="grid grid-cols-3 gap-2">
                            {availableConnectors.map(opt => (
                                <button
                                    key={opt.type}
                                    onClick={() => updateConfig({ type: opt.type })}
                                    className={`flex flex-col items-center gap-1 p-3 rounded border transition-colors ${config.type === opt.type
                                            ? 'border-vscode-accent bg-vscode-accent/20'
                                            : 'border-vscode-border hover:border-gray-500'
                                        }`}
                                >
                                    {opt.icon}
                                    <span className="text-xs">{opt.label}</span>
                                </button>
                            ))}
                        </div>
                    </div>

                    {/* Name */}
                    <div>
                        <label className="block text-sm text-gray-400 mb-1">Name</label>
                        <input
                            type="text"
                            value={config.name}
                            onChange={e => updateConfig({ name: e.target.value })}
                            placeholder="my-connector"
                            className="w-full px-3 py-2 bg-vscode-input border border-vscode-border rounded text-sm focus:border-vscode-accent outline-none"
                        />
                    </div>

                    {/* MQTT Configuration */}
                    {config.type === 'mqtt' && (
                        <>
                            <div className="grid grid-cols-2 gap-3">
                                <div>
                                    <label className="block text-sm text-gray-400 mb-1">Host</label>
                                    <input
                                        type="text"
                                        value={config.host || ''}
                                        onChange={e => updateConfig({ host: e.target.value })}
                                        placeholder="localhost"
                                        className="w-full px-3 py-2 bg-vscode-input border border-vscode-border rounded text-sm focus:border-vscode-accent outline-none"
                                    />
                                </div>
                                <div>
                                    <label className="block text-sm text-gray-400 mb-1">Port</label>
                                    <input
                                        type="number"
                                        value={config.port || 1883}
                                        onChange={e => updateConfig({ port: parseInt(e.target.value) })}
                                        className="w-full px-3 py-2 bg-vscode-input border border-vscode-border rounded text-sm focus:border-vscode-accent outline-none"
                                    />
                                </div>
                            </div>
                            <div>
                                <label className="block text-sm text-gray-400 mb-1">Topic</label>
                                <input
                                    type="text"
                                    value={config.topic || ''}
                                    onChange={e => updateConfig({ topic: e.target.value })}
                                    placeholder="sensors/temperature"
                                    className="w-full px-3 py-2 bg-vscode-input border border-vscode-border rounded text-sm focus:border-vscode-accent outline-none"
                                />
                            </div>
                            <div className="grid grid-cols-2 gap-3">
                                <div>
                                    <label className="block text-sm text-gray-400 mb-1">Username (optional)</label>
                                    <input
                                        type="text"
                                        value={config.username || ''}
                                        onChange={e => updateConfig({ username: e.target.value })}
                                        className="w-full px-3 py-2 bg-vscode-input border border-vscode-border rounded text-sm focus:border-vscode-accent outline-none"
                                    />
                                </div>
                                <div>
                                    <label className="block text-sm text-gray-400 mb-1">Password (optional)</label>
                                    <input
                                        type="password"
                                        value={config.password || ''}
                                        onChange={e => updateConfig({ password: e.target.value })}
                                        className="w-full px-3 py-2 bg-vscode-input border border-vscode-border rounded text-sm focus:border-vscode-accent outline-none"
                                    />
                                </div>
                            </div>
                        </>
                    )}

                    {/* Kafka Configuration */}
                    {config.type === 'kafka' && (
                        <>
                            <div>
                                <label className="block text-sm text-gray-400 mb-1">Brokers (comma-separated)</label>
                                <input
                                    type="text"
                                    value={config.brokers?.join(', ') || ''}
                                    onChange={e => updateConfig({ brokers: e.target.value.split(',').map(s => s.trim()) })}
                                    placeholder="localhost:9092"
                                    className="w-full px-3 py-2 bg-vscode-input border border-vscode-border rounded text-sm focus:border-vscode-accent outline-none"
                                />
                            </div>
                            <div>
                                <label className="block text-sm text-gray-400 mb-1">Topic</label>
                                <input
                                    type="text"
                                    value={config.topic || ''}
                                    onChange={e => updateConfig({ topic: e.target.value })}
                                    placeholder="events"
                                    className="w-full px-3 py-2 bg-vscode-input border border-vscode-border rounded text-sm focus:border-vscode-accent outline-none"
                                />
                            </div>
                            {mode === 'source' && (
                                <div>
                                    <label className="block text-sm text-gray-400 mb-1">Consumer Group ID</label>
                                    <input
                                        type="text"
                                        value={config.groupId || ''}
                                        onChange={e => updateConfig({ groupId: e.target.value })}
                                        placeholder="varpulis-group"
                                        className="w-full px-3 py-2 bg-vscode-input border border-vscode-border rounded text-sm focus:border-vscode-accent outline-none"
                                    />
                                </div>
                            )}
                        </>
                    )}

                    {/* AMQP Configuration */}
                    {config.type === 'amqp' && (
                        <>
                            <div className="grid grid-cols-2 gap-3">
                                <div>
                                    <label className="block text-sm text-gray-400 mb-1">Host</label>
                                    <input
                                        type="text"
                                        value={config.host || ''}
                                        onChange={e => updateConfig({ host: e.target.value })}
                                        placeholder="localhost"
                                        className="w-full px-3 py-2 bg-vscode-input border border-vscode-border rounded text-sm focus:border-vscode-accent outline-none"
                                    />
                                </div>
                                <div>
                                    <label className="block text-sm text-gray-400 mb-1">Port</label>
                                    <input
                                        type="number"
                                        value={config.port || 5672}
                                        onChange={e => updateConfig({ port: parseInt(e.target.value) })}
                                        className="w-full px-3 py-2 bg-vscode-input border border-vscode-border rounded text-sm focus:border-vscode-accent outline-none"
                                    />
                                </div>
                            </div>
                            <div>
                                <label className="block text-sm text-gray-400 mb-1">Queue</label>
                                <input
                                    type="text"
                                    value={config.queue || ''}
                                    onChange={e => updateConfig({ queue: e.target.value })}
                                    placeholder="events-queue"
                                    className="w-full px-3 py-2 bg-vscode-input border border-vscode-border rounded text-sm focus:border-vscode-accent outline-none"
                                />
                            </div>
                            <div>
                                <label className="block text-sm text-gray-400 mb-1">Exchange (optional)</label>
                                <input
                                    type="text"
                                    value={config.exchange || ''}
                                    onChange={e => updateConfig({ exchange: e.target.value })}
                                    placeholder="amq.topic"
                                    className="w-full px-3 py-2 bg-vscode-input border border-vscode-border rounded text-sm focus:border-vscode-accent outline-none"
                                />
                            </div>
                        </>
                    )}

                    {/* File Configuration */}
                    {config.type === 'file' && (
                        <>
                            <div>
                                <label className="block text-sm text-gray-400 mb-1">File Path</label>
                                <input
                                    type="text"
                                    value={config.path || ''}
                                    onChange={e => updateConfig({ path: e.target.value })}
                                    placeholder="./events/input.evt"
                                    className="w-full px-3 py-2 bg-vscode-input border border-vscode-border rounded text-sm focus:border-vscode-accent outline-none"
                                />
                            </div>
                            <div>
                                <label className="block text-sm text-gray-400 mb-1">Format</label>
                                <select
                                    value={config.format || 'evt'}
                                    onChange={e => updateConfig({ format: e.target.value as 'evt' | 'json' | 'csv' })}
                                    className="w-full px-3 py-2 bg-vscode-input border border-vscode-border rounded text-sm focus:border-vscode-accent outline-none"
                                >
                                    <option value="evt">.evt (Varpulis Events)</option>
                                    <option value="json">JSON</option>
                                    <option value="csv">CSV</option>
                                </select>
                            </div>
                        </>
                    )}

                    {/* HTTP Configuration */}
                    {config.type === 'http' && (
                        <>
                            <div>
                                <label className="block text-sm text-gray-400 mb-1">URL</label>
                                <input
                                    type="text"
                                    value={config.url || ''}
                                    onChange={e => updateConfig({ url: e.target.value })}
                                    placeholder="https://api.example.com/events"
                                    className="w-full px-3 py-2 bg-vscode-input border border-vscode-border rounded text-sm focus:border-vscode-accent outline-none"
                                />
                            </div>
                            <div>
                                <label className="block text-sm text-gray-400 mb-1">Method</label>
                                <select
                                    value={config.method || 'POST'}
                                    onChange={e => updateConfig({ method: e.target.value as 'GET' | 'POST' })}
                                    className="w-full px-3 py-2 bg-vscode-input border border-vscode-border rounded text-sm focus:border-vscode-accent outline-none"
                                >
                                    <option value="GET">GET</option>
                                    <option value="POST">POST</option>
                                </select>
                            </div>
                        </>
                    )}
                </div>

                <div className="flex justify-end gap-2 px-4 py-3 border-t border-vscode-border">
                    <button
                        onClick={onCancel}
                        className="px-4 py-2 text-sm border border-vscode-border rounded hover:bg-white/10"
                    >
                        Cancel
                    </button>
                    <button
                        onClick={() => onSave(config)}
                        disabled={!config.name}
                        className="px-4 py-2 text-sm bg-vscode-button text-white rounded hover:opacity-90 disabled:opacity-50"
                    >
                        Save
                    </button>
                </div>
            </div>
        </div>
    );
}
