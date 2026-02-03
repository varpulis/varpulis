import { useState } from 'react';
import type { ConnectorConfig, ConnectorType } from '../types';

interface ConnectorPanelProps {
    connector?: ConnectorConfig;
    mode: 'source' | 'sink';
    onSave: (config: ConnectorConfig) => void;
    onCancel: () => void;
}

const connectorOptions: { type: ConnectorType; label: string; icon: string; modes: ('source' | 'sink')[] }[] = [
    { type: 'mqtt', label: 'MQTT', icon: '📡', modes: ['source', 'sink'] },
    { type: 'kafka', label: 'Kafka', icon: '🗄️', modes: ['source', 'sink'] },
    { type: 'amqp', label: 'AMQP/RabbitMQ', icon: '💬', modes: ['source', 'sink'] },
    { type: 'file', label: 'File (.evt/.json)', icon: '📄', modes: ['source', 'sink'] },
    { type: 'http', label: 'HTTP/Webhook', icon: '🌐', modes: ['source', 'sink'] },
    { type: 'console', label: 'Console', icon: '💻', modes: ['sink'] },
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
                        <span>✕</span>
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
                                    <span className="text-xl">{opt.icon}</span>
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
