/**
 * Varpulis Dashboard Backend
 * 
 * Bridges MQTT events from Varpulis to WebSocket for React dashboard.
 * Also provides REST API for configuration.
 */

const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const mqtt = require('mqtt');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// Configuration
const config = {
    mqtt: {
        broker: process.env.MQTT_BROKER || 'mqtt://localhost:1883',
        topics: [
            'varpulis/dashboard/#',
            'varpulis/alerts/#',
            'varpulis/events/#'
        ]
    },
    port: process.env.PORT || 3002
};

// MQTT Client
let mqttClient = null;
let connected = false;

function connectMqtt() {
    console.log(`Connecting to MQTT broker: ${config.mqtt.broker}`);

    mqttClient = mqtt.connect(config.mqtt.broker, {
        clientId: `varpulis-dashboard-${Date.now()}`,
        clean: true,
        reconnectPeriod: 5000
    });

    mqttClient.on('connect', () => {
        console.log('Connected to MQTT broker');
        connected = true;

        config.mqtt.topics.forEach(topic => {
            mqttClient.subscribe(topic, (err) => {
                if (err) {
                    console.error(`Failed to subscribe to ${topic}:`, err);
                } else {
                    console.log(`Subscribed to: ${topic}`);
                }
            });
        });

        broadcastToClients({ type: 'status', connected: true });
    });

    mqttClient.on('message', (topic, message) => {
        console.log(`MQTT message on ${topic}: ${message.toString().substring(0, 100)}`);
        try {
            const payload = JSON.parse(message.toString());
            const wsMessage = {
                type: 'event',
                topic,
                timestamp: new Date().toISOString(),
                data: payload
            };
            console.log(`Broadcasting to ${clients.size} WebSocket clients`);
            broadcastToClients(wsMessage);
        } catch (e) {
            console.error('Failed to parse MQTT message:', e);
        }
    });

    mqttClient.on('error', (err) => {
        console.error('MQTT error:', err);
        connected = false;
    });

    mqttClient.on('close', () => {
        console.log('MQTT connection closed');
        connected = false;
        broadcastToClients({ type: 'status', connected: false });
    });
}

// WebSocket handling
const clients = new Set();

wss.on('connection', (ws) => {
    console.log('New WebSocket client connected');
    clients.add(ws);

    // Send current status
    ws.send(JSON.stringify({
        type: 'status',
        connected,
        topics: config.mqtt.topics
    }));

    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data);
            handleClientMessage(ws, msg);
        } catch (e) {
            console.error('Invalid WebSocket message:', e);
        }
    });

    ws.on('close', () => {
        console.log('WebSocket client disconnected');
        clients.delete(ws);
    });
});

function broadcastToClients(message) {
    const data = JSON.stringify(message);
    clients.forEach(client => {
        if (client.readyState === 1) { // OPEN
            client.send(data);
        }
    });
}

function handleClientMessage(ws, msg) {
    switch (msg.type) {
        case 'subscribe':
            // Subscribe to additional topics
            if (msg.topic && mqttClient) {
                mqttClient.subscribe(msg.topic);
                console.log(`Client requested subscription to: ${msg.topic}`);
            }
            break;
        case 'ping':
            ws.send(JSON.stringify({ type: 'pong' }));
            break;
    }
}

// REST API
app.get('/api/status', (req, res) => {
    res.json({
        mqtt: {
            connected,
            broker: config.mqtt.broker,
            topics: config.mqtt.topics
        },
        clients: clients.size
    });
});

app.get('/api/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Start server
server.listen(config.port, () => {
    console.log(`Dashboard server running on port ${config.port}`);
    console.log(`WebSocket endpoint: ws://localhost:${config.port}`);
    connectMqtt();
});

// Graceful shutdown
process.on('SIGINT', () => {
    console.log('Shutting down...');
    if (mqttClient) mqttClient.end();
    server.close();
    process.exit(0);
});
