import { ChildProcess, spawn } from 'child_process';
import * as vscode from 'vscode';
import WebSocket from 'ws';

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

    /**
     * Parse and inject events from an .evt file
     * @param filePath Path to the event file
     * @param immediate If true, ignore timing and send all events immediately
     * @param onProgress Callback for progress updates
     */
    async injectEventFile(
        filePath: string, 
        immediate: boolean = false,
        onProgress?: (current: number, total: number, eventType: string) => void
    ): Promise<{ sent: number; errors: string[] }> {
        const fs = await import('fs');
        const content = fs.readFileSync(filePath, 'utf-8');
        
        const events = this.parseEventFile(content);
        const errors: string[] = [];
        let sent = 0;

        if (immediate) {
            // Send all events immediately
            for (const event of events) {
                try {
                    await this.injectEvent(event.eventType, event.data);
                    sent++;
                    onProgress?.(sent, events.length, event.eventType);
                } catch (e: any) {
                    errors.push(`Event ${event.eventType}: ${e.message}`);
                }
            }
        } else {
            // Send events respecting BATCH timing
            const startTime = Date.now();
            
            // Group events by time offset
            const batches = new Map<number, typeof events>();
            for (const event of events) {
                const batch = batches.get(event.timeOffset) || [];
                batch.push(event);
                batches.set(event.timeOffset, batch);
            }

            // Sort batch times
            const times = Array.from(batches.keys()).sort((a, b) => a - b);

            for (const batchTime of times) {
                // Wait until batch time
                const elapsed = Date.now() - startTime;
                if (batchTime > elapsed) {
                    await new Promise(resolve => setTimeout(resolve, batchTime - elapsed));
                }

                // Send all events in this batch
                const batchEvents = batches.get(batchTime) || [];
                for (const event of batchEvents) {
                    try {
                        await this.injectEvent(event.eventType, event.data);
                        sent++;
                        onProgress?.(sent, events.length, event.eventType);
                    } catch (e: any) {
                        errors.push(`Event ${event.eventType}: ${e.message}`);
                    }
                }
            }
        }

        return { sent, errors };
    }

    /**
     * Parse .evt file content into events
     */
    private parseEventFile(content: string): Array<{ eventType: string; data: any; timeOffset: number }> {
        const events: Array<{ eventType: string; data: any; timeOffset: number }> = [];
        let currentBatchTime = 0;

        for (const line of content.split('\n')) {
            const trimmed = line.trim();

            // Skip empty lines and comments
            if (!trimmed || trimmed.startsWith('#') || trimmed.startsWith('//')) {
                continue;
            }

            // Check for BATCH directive
            if (trimmed.startsWith('BATCH')) {
                const parts = trimmed.split(/\s+/);
                if (parts.length >= 2) {
                    currentBatchTime = parseInt(parts[1], 10) || 0;
                }
                continue;
            }

            // Parse event: EventType { field: value, ... }
            const event = this.parseEventLine(trimmed);
            if (event) {
                events.push({ ...event, timeOffset: currentBatchTime });
            }
        }

        return events;
    }

    /**
     * Parse a single event line
     */
    private parseEventLine(line: string): { eventType: string; data: any } | null {
        // Remove trailing semicolon
        line = line.replace(/;$/, '').trim();

        // Find event type name and content
        const braceMatch = line.match(/^(\w+)\s*\{(.*)}\s*$/);
        const parenMatch = line.match(/^(\w+)\s*\((.*)?\)\s*$/);

        if (braceMatch) {
            // JSON-style: EventType { field: value, ... }
            const eventType = braceMatch[1];
            const fieldsStr = braceMatch[2].trim();
            const data = this.parseFields(fieldsStr);
            return { eventType, data };
        } else if (parenMatch) {
            // Positional: EventType(value1, value2)
            const eventType = parenMatch[1];
            const valuesStr = parenMatch[2]?.trim() || '';
            const values = this.splitValues(valuesStr);
            const data: any = {};
            values.forEach((v, i) => {
                data[`field_${i}`] = this.parseValue(v.trim());
            });
            return { eventType, data };
        }

        return null;
    }

    /**
     * Parse field assignments: field1: value1, field2: value2
     */
    private parseFields(fieldsStr: string): any {
        const data: any = {};
        const fields = this.splitValues(fieldsStr);

        for (const field of fields) {
            const colonIdx = field.indexOf(':');
            if (colonIdx > 0) {
                const key = field.substring(0, colonIdx).trim();
                const value = field.substring(colonIdx + 1).trim();
                data[key] = this.parseValue(value);
            }
        }

        return data;
    }

    /**
     * Split values by comma, respecting nested structures
     */
    private splitValues(str: string): string[] {
        const values: string[] = [];
        let current = '';
        let depth = 0;
        let inString = false;
        let escape = false;

        for (const ch of str) {
            if (escape) {
                current += ch;
                escape = false;
                continue;
            }

            if (ch === '\\') {
                current += ch;
                escape = true;
                continue;
            }

            if (ch === '"') {
                current += ch;
                inString = !inString;
                continue;
            }

            if (!inString) {
                if (ch === '{' || ch === '[' || ch === '(') {
                    depth++;
                } else if (ch === '}' || ch === ']' || ch === ')') {
                    depth--;
                } else if (ch === ',' && depth === 0) {
                    values.push(current.trim());
                    current = '';
                    continue;
                }
            }

            current += ch;
        }

        if (current.trim()) {
            values.push(current.trim());
        }

        return values;
    }

    /**
     * Parse a value string
     */
    private parseValue(s: string): any {
        s = s.trim();

        // Boolean
        if (s === 'true') return true;
        if (s === 'false') return false;

        // Null
        if (s === 'null' || s === 'nil') return null;

        // String (quoted)
        if ((s.startsWith('"') && s.endsWith('"')) || (s.startsWith("'") && s.endsWith("'"))) {
            return s.slice(1, -1)
                .replace(/\\n/g, '\n')
                .replace(/\\t/g, '\t')
                .replace(/\\"/g, '"')
                .replace(/\\'/g, "'")
                .replace(/\\\\/g, '\\');
        }

        // Number
        const num = parseFloat(s);
        if (!isNaN(num)) {
            return Number.isInteger(num) ? parseInt(s, 10) : num;
        }

        // Array
        if (s.startsWith('[') && s.endsWith(']')) {
            const inner = s.slice(1, -1);
            return this.splitValues(inner).filter(v => v).map(v => this.parseValue(v));
        }

        // Unquoted string
        return s;
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
            const ws = new WebSocket(wsUrl);
            this.ws = ws;

            ws.on('open', () => {
                this.outputChannel.appendLine(`[ws] Connected to ${wsUrl}`);
                resolve();
            });

            ws.on('message', (data: WebSocket.RawData) => {
                this.handleMessage(data);
            });

            ws.on('error', (error: Error) => {
                this.outputChannel.appendLine(`[ws] Error: ${error.message}`);
                reject(error);
            });

            ws.on('close', () => {
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

    private handleMessage(data: WebSocket.RawData): void {
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
