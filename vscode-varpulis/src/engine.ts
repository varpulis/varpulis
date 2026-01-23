import { ChildProcess, spawn } from 'child_process';
import * as vscode from 'vscode';
import * as WebSocket from 'ws';

export interface StreamInfo {
    name: string;
    source: string;
    operations: string[];
    eventsPerSecond: number;
    status: 'active' | 'paused' | 'error';
}

export interface VarpulisEvent {
    id: string;
    type: string;
    timestamp: string;
    data: any;
}

export interface VarpulisAlert {
    id: string;
    type: string;
    severity: 'info' | 'warning' | 'critical';
    message: string;
    timestamp: string;
    data: any;
}

export interface EngineMetrics {
    eventsProcessed: number;
    alertsGenerated: number;
    activeStreams: number;
    uptime: number;
    memoryUsage: number;
    cpuUsage: number;
}

export interface LoadResult {
    success: boolean;
    streamsLoaded: number;
    error?: string;
}

export interface EngineStatus {
    running: boolean;
    activeStreams: number;
    eventsProcessed: number;
    alertsGenerated: number;
}

type StreamUpdateHandler = (streams: StreamInfo[]) => void;
type EventHandler = (event: VarpulisEvent) => void;
type AlertHandler = (alert: VarpulisAlert) => void;
type MetricsHandler = (metrics: EngineMetrics) => void;

export class VarpulisEngine {
    private process: ChildProcess | null = null;
    private ws: WebSocket | null = null;
    private running: boolean = false;
    private outputChannel: vscode.OutputChannel;
    private enginePath: string;
    private port: number;

    private streamUpdateHandlers: StreamUpdateHandler[] = [];
    private eventHandlers: EventHandler[] = [];
    private alertHandlers: AlertHandler[] = [];
    private metricsHandlers: MetricsHandler[] = [];

    private streams: StreamInfo[] = [];
    private metrics: EngineMetrics = {
        eventsProcessed: 0,
        alertsGenerated: 0,
        activeStreams: 0,
        uptime: 0,
        memoryUsage: 0,
        cpuUsage: 0
    };

    constructor(enginePath: string, port: number, outputChannel: vscode.OutputChannel) {
        this.enginePath = enginePath;
        this.port = port;
        this.outputChannel = outputChannel;
    }

    async start(): Promise<void> {
        return new Promise((resolve, reject) => {
            try {
                // Start the engine process in server mode
                this.process = spawn(this.enginePath, ['server', '--port', this.port.toString()], {
                    stdio: ['pipe', 'pipe', 'pipe']
                });

                this.process.stdout?.on('data', (data: Buffer) => {
                    this.outputChannel.appendLine(`[engine] ${data.toString().trim()}`);
                });

                this.process.stderr?.on('data', (data: Buffer) => {
                    this.outputChannel.appendLine(`[engine:err] ${data.toString().trim()}`);
                });

                this.process.on('error', (error) => {
                    this.outputChannel.appendLine(`[engine] Process error: ${error.message}`);
                    this.running = false;
                    reject(error);
                });

                this.process.on('exit', (code) => {
                    this.outputChannel.appendLine(`[engine] Process exited with code ${code}`);
                    this.running = false;
                    this.disconnectWebSocket();
                });

                // Wait for the engine to start and then connect WebSocket
                setTimeout(async () => {
                    try {
                        await this.connectWebSocket();
                        this.running = true;
                        resolve();
                    } catch (wsError) {
                        reject(wsError);
                    }
                }, 1000);

            } catch (error) {
                reject(error);
            }
        });
    }

    async stop(): Promise<void> {
        this.disconnectWebSocket();
        
        if (this.process) {
            this.process.kill();
            this.process = null;
        }
        
        this.running = false;
    }

    isRunning(): boolean {
        return this.running;
    }

    getStatus(): EngineStatus {
        return {
            running: this.running,
            activeStreams: this.streams.length,
            eventsProcessed: this.metrics.eventsProcessed,
            alertsGenerated: this.metrics.alertsGenerated
        };
    }

    async loadFile(filePath: string): Promise<LoadResult> {
        if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
            return { success: false, streamsLoaded: 0, error: 'Not connected to engine' };
        }

        return new Promise((resolve) => {
            const messageHandler = (data: WebSocket.Data) => {
                try {
                    const message = JSON.parse(data.toString());
                    if (message.type === 'load_result') {
                        this.ws?.off('message', messageHandler);
                        resolve({
                            success: message.success,
                            streamsLoaded: message.streams_loaded || 0,
                            error: message.error
                        });
                    }
                } catch (e) {
                    // Ignore parse errors for other messages
                }
            };

            this.ws!.on('message', messageHandler);
            this.ws!.send(JSON.stringify({
                type: 'load_file',
                path: filePath
            }));

            // Timeout after 10 seconds
            setTimeout(() => {
                this.ws?.off('message', messageHandler);
                resolve({ success: false, streamsLoaded: 0, error: 'Timeout waiting for load result' });
            }, 10000);
        });
    }

    async injectEvent(eventType: string, data: any): Promise<void> {
        if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
            throw new Error('Not connected to engine');
        }

        this.ws.send(JSON.stringify({
            type: 'inject_event',
            event_type: eventType,
            data: data
        }));
    }

    onStreamUpdate(handler: StreamUpdateHandler): void {
        this.streamUpdateHandlers.push(handler);
    }

    onEvent(handler: EventHandler): void {
        this.eventHandlers.push(handler);
    }

    onAlert(handler: AlertHandler): void {
        this.alertHandlers.push(handler);
    }

    onMetrics(handler: MetricsHandler): void {
        this.metricsHandlers.push(handler);
    }

    private async connectWebSocket(): Promise<void> {
        return new Promise((resolve, reject) => {
            const wsUrl = `ws://localhost:${this.port}/ws`;
            this.ws = new WebSocket(wsUrl);

            this.ws.on('open', () => {
                this.outputChannel.appendLine(`[ws] Connected to ${wsUrl}`);
                resolve();
            });

            this.ws.on('message', (data: WebSocket.Data) => {
                this.handleMessage(data);
            });

            this.ws.on('error', (error: Error) => {
                this.outputChannel.appendLine(`[ws] Error: ${error.message}`);
                reject(error);
            });

            this.ws.on('close', () => {
                this.outputChannel.appendLine('[ws] Connection closed');
            });
        });
    }

    private disconnectWebSocket(): void {
        if (this.ws) {
            this.ws.close();
            this.ws = null;
        }
    }

    private handleMessage(data: WebSocket.Data): void {
        try {
            const message = JSON.parse(data.toString());

            switch (message.type) {
                case 'streams':
                    this.streams = message.data;
                    this.streamUpdateHandlers.forEach(h => h(this.streams));
                    break;

                case 'event':
                    const event: VarpulisEvent = {
                        id: message.id,
                        type: message.event_type,
                        timestamp: message.timestamp,
                        data: message.data
                    };
                    this.eventHandlers.forEach(h => h(event));
                    this.metrics.eventsProcessed++;
                    break;

                case 'alert':
                    const alert: VarpulisAlert = {
                        id: message.id,
                        type: message.alert_type,
                        severity: message.severity,
                        message: message.message,
                        timestamp: message.timestamp,
                        data: message.data
                    };
                    this.alertHandlers.forEach(h => h(alert));
                    this.metrics.alertsGenerated++;
                    break;

                case 'metrics':
                    this.metrics = {
                        eventsProcessed: message.events_processed,
                        alertsGenerated: message.alerts_generated,
                        activeStreams: message.active_streams,
                        uptime: message.uptime,
                        memoryUsage: message.memory_usage,
                        cpuUsage: message.cpu_usage
                    };
                    this.metricsHandlers.forEach(h => h(this.metrics));
                    break;

                default:
                    this.outputChannel.appendLine(`[ws] Unknown message type: ${message.type}`);
            }
        } catch (error) {
            this.outputChannel.appendLine(`[ws] Failed to parse message: ${error}`);
        }
    }
}
