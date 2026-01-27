/**
 * React hook for connecting to Varpulis via WebSocket
 */

import { useCallback, useEffect, useRef, useState } from 'react';

export interface VarpulisEvent {
    type: string;
    topic: string;
    timestamp: string;
    data: Record<string, unknown>;
}

export interface VarpulisConnection {
    connected: boolean;
    mqttConnected: boolean;
    events: VarpulisEvent[];
    alerts: VarpulisEvent[];
    dashboardData: Record<string, unknown>;
    send: (message: unknown) => void;
    clearEvents: () => void;
}

// WebSocket URL - use same origin /ws path (proxied by nginx)
const getWsUrl = () => {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    return import.meta.env.VITE_WS_URL || `${protocol}//${window.location.host}/ws`;
};
const WS_URL = getWsUrl();
const MAX_EVENTS = 100;

export function useVarpulis(): VarpulisConnection {
    const [connected, setConnected] = useState(false);
    const [mqttConnected, setMqttConnected] = useState(false);
    const [events, setEvents] = useState<VarpulisEvent[]>([]);
    const [alerts, setAlerts] = useState<VarpulisEvent[]>([]);
    const [dashboardData, setDashboardData] = useState<Record<string, unknown>>({});
    
    const wsRef = useRef<WebSocket | null>(null);
    const reconnectTimeoutRef = useRef<number | null>(null);

    const connect = useCallback(() => {
        if (wsRef.current?.readyState === WebSocket.OPEN) return;

        console.log(`Connecting to Varpulis at ${WS_URL}...`);
        const ws = new WebSocket(WS_URL);

        ws.onopen = () => {
            console.log('WebSocket connected');
            setConnected(true);
        };

        ws.onclose = () => {
            console.log('WebSocket disconnected');
            setConnected(false);
            setMqttConnected(false);
            
            // Reconnect after 3 seconds
            reconnectTimeoutRef.current = window.setTimeout(() => {
                connect();
            }, 3000);
        };

        ws.onerror = (error) => {
            console.error('WebSocket error:', error);
        };

        ws.onmessage = (event) => {
            try {
                const message = JSON.parse(event.data);
                handleMessage(message);
            } catch (e) {
                console.error('Failed to parse message:', e);
            }
        };

        wsRef.current = ws;
    }, []);

    const handleMessage = useCallback((message: { type: string; topic?: string; data?: unknown; connected?: boolean }) => {
        switch (message.type) {
            case 'status':
                setMqttConnected(message.connected || false);
                break;

            case 'event':
                const eventData = message.data as Record<string, unknown>;
                const varpulisEvent: VarpulisEvent = {
                    type: String(eventData?.event_type || 'unknown'),
                    topic: message.topic || '',
                    timestamp: new Date().toISOString(),
                    data: eventData
                };

                // Route to appropriate state based on topic
                if (message.topic?.includes('/alerts') || message.topic?.includes('/security_alerts')) {
                    setAlerts(prev => [varpulisEvent, ...prev].slice(0, MAX_EVENTS));
                } else if (message.topic?.includes('/dashboard/')) {
                    // Update dashboard-specific data
                    const key = message.topic.split('/dashboard/')[1];
                    if (key) {
                        setDashboardData(prev => ({
                            ...prev,
                            [key]: message.data
                        }));
                    }
                }
                
                // Always add to events stream
                setEvents(prev => [varpulisEvent, ...prev].slice(0, MAX_EVENTS));
                break;

            case 'pong':
                // Heartbeat response
                break;
        }
    }, []);

    const send = useCallback((message: unknown) => {
        if (wsRef.current?.readyState === WebSocket.OPEN) {
            wsRef.current.send(JSON.stringify(message));
        }
    }, []);

    const clearEvents = useCallback(() => {
        setEvents([]);
        setAlerts([]);
    }, []);

    useEffect(() => {
        connect();

        // Heartbeat every 30 seconds
        const heartbeat = setInterval(() => {
            send({ type: 'ping' });
        }, 30000);

        return () => {
            clearInterval(heartbeat);
            if (reconnectTimeoutRef.current) {
                clearTimeout(reconnectTimeoutRef.current);
            }
            wsRef.current?.close();
        };
    }, [connect, send]);

    return {
        connected,
        mqttConnected,
        events,
        alerts,
        dashboardData,
        send,
        clearEvents
    };
}

// Type-specific hooks for each demo

export function useHVACData() {
    const { events, alerts, dashboardData, connected, mqttConnected } = useVarpulis();

    const zones = (dashboardData.zones as unknown[]) || [];
    const energy = (dashboardData.energy as Record<string, number>) || {};
    
    const temperatureEvents = events.filter(e => 
        e.data?.event_type === 'TemperatureReading' || 
        e.topic?.includes('TemperatureReading')
    );

    return {
        connected,
        mqttConnected,
        zones,
        energy,
        temperatureEvents,
        alerts: alerts.filter(a => 
            a.topic?.includes('hvac') || 
            a.data?.event_type?.toString().includes('Temperature') ||
            a.data?.event_type?.toString().includes('Humidity') ||
            a.data?.event_type?.toString().includes('Energy')
        ),
        rawEvents: events
    };
}

export function useFinancialData() {
    const { events, alerts, dashboardData, connected, mqttConnected } = useVarpulis();

    const prices = (dashboardData.prices as unknown[]) || [];
    const indicators = (dashboardData.indicators as unknown[]) || [];
    const signals = (dashboardData.signals as unknown[]) || [];

    const marketTicks = events.filter(e => 
        e.data?.event_type === 'MarketTick' || 
        e.topic?.includes('MarketTick')
    );

    return {
        connected,
        mqttConnected,
        prices,
        indicators,
        signals,
        marketTicks,
        alerts: alerts.filter(a => 
            a.topic?.includes('financial') || 
            a.data?.event_type?.toString().includes('Cross') ||
            a.data?.event_type?.toString().includes('Volume') ||
            a.data?.event_type?.toString().includes('Breakout')
        ),
        rawEvents: events
    };
}

export function useSASEData() {
    const { events, alerts, dashboardData, connected, mqttConnected } = useVarpulis();

    const sessions = (dashboardData.sessions as unknown[]) || [];
    const transactions = (dashboardData.transactions as unknown[]) || [];

    const securityAlerts = alerts.filter(a => 
        a.topic?.includes('security') || 
        a.data?.event_type?.toString().includes('Takeover') ||
        a.data?.event_type?.toString().includes('Travel') ||
        a.data?.event_type?.toString().includes('Velocity')
    );

    const loginEvents = events.filter(e => 
        e.data?.event_type === 'Login' || 
        e.topic?.includes('Login')
    );

    return {
        connected,
        mqttConnected,
        sessions,
        transactions,
        securityAlerts,
        loginEvents,
        rawEvents: events
    };
}
