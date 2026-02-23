import { useState } from 'react';
import type { ConnectorNodeData, ConnectorType } from '../types/flow';

interface ConnectorPanelProps {
    connector?: ConnectorNodeData;
    mode: 'source' | 'sink';
    onSave: (config: ConnectorNodeData) => void;
    onCancel: () => void;
}

const connectorOptions: { type: ConnectorType; label: string; icon: string; modes: ('source' | 'sink')[] }[] = [
    { type: 'mqtt', label: 'MQTT', icon: '📡', modes: ['source', 'sink'] },
    { type: 'kafka', label: 'Kafka', icon: '🗄️', modes: ['source', 'sink'] },
    { type: 'nats', label: 'NATS', icon: '⚡', modes: ['source', 'sink'] },
    { type: 'amqp', label: 'AMQP/RabbitMQ', icon: '💬', modes: ['source', 'sink'] },
    { type: 'file', label: 'File (.evt/.json)', icon: '📄', modes: ['source', 'sink'] },
    { type: 'http', label: 'HTTP/Webhook', icon: '🌐', modes: ['source', 'sink'] },
];

export default function ConnectorPanel({ connector, mode, onSave, onCancel }: ConnectorPanelProps) {
    const [config, setConfig] = useState<ConnectorNodeData>(
        connector || { label: '', connectorType: 'mqtt' }
    );

    const availableConnectors = connectorOptions.filter(c => c.modes.includes(mode));

    const updateConfig = (updates: Partial<ConnectorNodeData>) => {
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
                                    onClick={() => updateConfig({ connectorType: opt.type })}
                                    className={`flex flex-col items-center gap-1 p-3 rounded border transition-colors ${config.connectorType === opt.type
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
                            value={config.label}
                            onChange={e => updateConfig({ label: e.target.value })}
                            placeholder="my-connector"
                            className="w-full px-3 py-2 bg-vscode-input border border-vscode-border rounded text-sm focus:border-vscode-accent outline-none"
                        />
                    </div>

                    {/* MQTT Configuration */}
                    {config.connectorType === 'mqtt' && (
                        <>
                            <div className="grid grid-cols-2 gap-3">
                                <div>
                                    <label className="block text-sm text-gray-400 mb-1">Host</label>
                                    <input
                                        type="text"
                                        value={config.broker || ''}
                                        onChange={e => updateConfig({ broker: e.target.value })}
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
                        </>
                    )}

                    {/* Kafka Configuration */}
                    {config.connectorType === 'kafka' && (
                        <div>
                            <label className="block text-sm text-gray-400 mb-1">Brokers (comma-separated)</label>
                            <input
                                type="text"
                                value={config.brokers || ''}
                                onChange={e => updateConfig({ brokers: e.target.value })}
                                placeholder="localhost:9092"
                                className="w-full px-3 py-2 bg-vscode-input border border-vscode-border rounded text-sm focus:border-vscode-accent outline-none"
                            />
                        </div>
                    )}

                    {/* NATS Configuration */}
                    {config.connectorType === 'nats' && (
                        <div>
                            <label className="block text-sm text-gray-400 mb-1">URL</label>
                            <input
                                type="text"
                                value={config.natsUrl || ''}
                                onChange={e => updateConfig({ natsUrl: e.target.value })}
                                placeholder="nats://localhost:4222"
                                className="w-full px-3 py-2 bg-vscode-input border border-vscode-border rounded text-sm focus:border-vscode-accent outline-none"
                            />
                        </div>
                    )}

                    {/* File Configuration */}
                    {config.connectorType === 'file' && (
                        <div>
                            <label className="block text-sm text-gray-400 mb-1">File Path</label>
                            <input
                                type="text"
                                value={config.basePath || ''}
                                onChange={e => updateConfig({ basePath: e.target.value })}
                                placeholder="./events/input.evt"
                                className="w-full px-3 py-2 bg-vscode-input border border-vscode-border rounded text-sm focus:border-vscode-accent outline-none"
                            />
                        </div>
                    )}

                    {/* HTTP Configuration */}
                    {config.connectorType === 'http' && (
                        <div>
                            <label className="block text-sm text-gray-400 mb-1">URL</label>
                            <input
                                type="text"
                                value={config.baseUrl || ''}
                                onChange={e => updateConfig({ baseUrl: e.target.value })}
                                placeholder="https://api.example.com/events"
                                className="w-full px-3 py-2 bg-vscode-input border border-vscode-border rounded text-sm focus:border-vscode-accent outline-none"
                            />
                        </div>
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
                        disabled={!config.label}
                        className="px-4 py-2 text-sm bg-vscode-button text-white rounded hover:opacity-90 disabled:opacity-50"
                    >
                        Save
                    </button>
                </div>
            </div>
        </div>
    );
}
