/**
 * Stream Pipeline Graph Visualization
 * 
 * SVG-based DAG showing how events flow through streams
 */

import { useRef } from 'react'

interface StreamNode {
    id: string
    label: string
    type: 'event' | 'stream' | 'pattern' | 'alert'
    x: number
    y: number
    count?: number
}

interface StreamEdge {
    from: string
    to: string
    label?: string
}

interface StreamGraphProps {
    nodes: StreamNode[]
    edges: StreamEdge[]
    eventCounts: Record<string, number>
    streamCounts: Record<string, number>
    patternCounts: Record<string, number>
}

const NODE_COLORS = {
    event: { fill: '#1e40af', stroke: '#3b82f6', text: '#93c5fd' },
    stream: { fill: '#6b21a8', stroke: '#a855f7', text: '#d8b4fe' },
    pattern: { fill: '#b45309', stroke: '#f59e0b', text: '#fcd34d' },
    alert: { fill: '#991b1b', stroke: '#ef4444', text: '#fca5a5' },
}

export default function StreamGraph({ nodes, edges, eventCounts, streamCounts, patternCounts }: StreamGraphProps) {
    const svgRef = useRef<SVGSVGElement>(null)

    // Get count for a node
    const getCount = (node: StreamNode): number => {
        if (node.type === 'event') return eventCounts[node.id] || 0
        if (node.type === 'stream') return streamCounts[node.id] || 0
        if (node.type === 'pattern') return patternCounts[node.id] || 0
        return 0
    }

    // Calculate edge path with curve
    const getEdgePath = (from: StreamNode, to: StreamNode): string => {
        const startX = from.x + 60
        const startY = from.y + 15
        const endX = to.x
        const endY = to.y + 15
        const midX = (startX + endX) / 2

        return `M ${startX} ${startY} C ${midX} ${startY}, ${midX} ${endY}, ${endX} ${endY}`
    }

    // Find node by id
    const findNode = (id: string): StreamNode | undefined => nodes.find(n => n.id === id)

    return (
        <svg
            ref={svgRef}
            viewBox="0 0 800 400"
            className="w-full h-full"
            style={{ minHeight: '350px' }}
        >
            <defs>
                {/* Arrow marker */}
                <marker
                    id="arrowhead"
                    markerWidth="10"
                    markerHeight="7"
                    refX="9"
                    refY="3.5"
                    orient="auto"
                >
                    <polygon points="0 0, 10 3.5, 0 7" fill="#64748b" />
                </marker>

                {/* Glow filter for active nodes */}
                <filter id="glow" x="-50%" y="-50%" width="200%" height="200%">
                    <feGaussianBlur stdDeviation="3" result="coloredBlur" />
                    <feMerge>
                        <feMergeNode in="coloredBlur" />
                        <feMergeNode in="SourceGraphic" />
                    </feMerge>
                </filter>
            </defs>

            {/* Background grid */}
            <pattern id="grid" width="20" height="20" patternUnits="userSpaceOnUse">
                <path d="M 20 0 L 0 0 0 20" fill="none" stroke="#1e293b" strokeWidth="0.5" />
            </pattern>
            <rect width="100%" height="100%" fill="url(#grid)" />

            {/* Column headers */}
            <text x="60" y="25" fill="#64748b" fontSize="12" fontWeight="bold">EVENTS</text>
            <text x="250" y="25" fill="#64748b" fontSize="12" fontWeight="bold">STREAMS</text>
            <text x="500" y="25" fill="#64748b" fontSize="12" fontWeight="bold">DERIVED</text>
            <text x="680" y="25" fill="#64748b" fontSize="12" fontWeight="bold">PATTERNS</text>

            {/* Edges */}
            {edges.map((edge, i) => {
                const from = findNode(edge.from)
                const to = findNode(edge.to)
                if (!from || !to) return null

                const fromCount = getCount(from)
                const isActive = fromCount > 0

                return (
                    <path
                        key={i}
                        d={getEdgePath(from, to)}
                        fill="none"
                        stroke={isActive ? '#64748b' : '#334155'}
                        strokeWidth={isActive ? 2 : 1}
                        markerEnd="url(#arrowhead)"
                        opacity={isActive ? 1 : 0.5}
                    />
                )
            })}

            {/* Nodes */}
            {nodes.map(node => {
                const colors = NODE_COLORS[node.type]
                const count = getCount(node)
                const isActive = count > 0

                return (
                    <g key={node.id} filter={isActive ? 'url(#glow)' : undefined}>
                        {/* Node rectangle */}
                        <rect
                            x={node.x}
                            y={node.y}
                            width={120}
                            height={30}
                            rx={4}
                            fill={colors.fill}
                            stroke={colors.stroke}
                            strokeWidth={isActive ? 2 : 1}
                            opacity={isActive ? 1 : 0.6}
                        />

                        {/* Node label */}
                        <text
                            x={node.x + 8}
                            y={node.y + 19}
                            fill={colors.text}
                            fontSize="11"
                            fontWeight="500"
                        >
                            {node.label.length > 14 ? node.label.slice(0, 12) + '…' : node.label}
                        </text>

                        {/* Count badge */}
                        {count > 0 && (
                            <>
                                <rect
                                    x={node.x + 90}
                                    y={node.y + 5}
                                    width={25}
                                    height={20}
                                    rx={3}
                                    fill={colors.stroke}
                                />
                                <text
                                    x={node.x + 102}
                                    y={node.y + 19}
                                    fill="#fff"
                                    fontSize="10"
                                    fontWeight="bold"
                                    textAnchor="middle"
                                >
                                    {count > 999 ? '999+' : count}
                                </text>
                            </>
                        )}
                    </g>
                )
            })}

            {/* Edge labels (rendered after nodes so they appear on top) */}
            {edges.map((edge, i) => {
                const from = findNode(edge.from)
                const to = findNode(edge.to)
                if (!from || !to || !edge.label) return null

                const labelX = (from.x + 60 + to.x) / 2
                const labelY = (from.y + to.y) / 2 + 15
                const labelWidth = edge.label.length * 5.5 + 8

                return (
                    <g key={`label-${i}`}>
                        {/* Background rect for label */}
                        <rect
                            x={labelX - labelWidth / 2}
                            y={labelY - 9}
                            width={labelWidth}
                            height={14}
                            rx={3}
                            fill="#1e293b"
                            stroke="#334155"
                            strokeWidth={0.5}
                        />
                        <text
                            x={labelX}
                            y={labelY + 2}
                            fill="#94a3b8"
                            fontSize="9"
                            textAnchor="middle"
                            fontFamily="monospace"
                        >
                            {edge.label}
                        </text>
                    </g>
                )
            })}

            {/* Legend */}
            <g transform="translate(10, 360)">
                <rect x="0" y="0" width="12" height="12" rx="2" fill="#1e40af" />
                <text x="16" y="10" fill="#64748b" fontSize="9">Event</text>

                <rect x="60" y="0" width="12" height="12" rx="2" fill="#6b21a8" />
                <text x="76" y="10" fill="#64748b" fontSize="9">Stream</text>

                <rect x="130" y="0" width="12" height="12" rx="2" fill="#b45309" />
                <text x="146" y="10" fill="#64748b" fontSize="9">Pattern</text>

                <rect x="200" y="0" width="12" height="12" rx="2" fill="#991b1b" />
                <text x="216" y="10" fill="#64748b" fontSize="9">Alert</text>
            </g>
        </svg>
    )
}

// Graph definitions for each demo

export const HVAC_GRAPH = {
    nodes: [
        // Events (Column 1)
        { id: 'TemperatureReading', label: '🌡️ TempReading', type: 'event' as const, x: 20, y: 50 },
        { id: 'HumidityReading', label: '💧 Humidity', type: 'event' as const, x: 20, y: 120 },
        { id: 'HVACStatus', label: '❄️ HVACStatus', type: 'event' as const, x: 20, y: 190 },
        { id: 'EnergyMeter', label: '⚡ EnergyMeter', type: 'event' as const, x: 20, y: 260 },

        // Streams (Column 2)
        { id: 'Temperatures', label: 'Temperatures', type: 'stream' as const, x: 200, y: 50 },
        { id: 'Humidity', label: 'Humidity', type: 'stream' as const, x: 200, y: 120 },
        { id: 'HVACMetrics', label: 'HVACMetrics', type: 'stream' as const, x: 200, y: 190 },
        { id: 'Energy', label: 'Energy', type: 'stream' as const, x: 200, y: 260 },

        // Derived Streams (Column 3)
        { id: 'ZoneTemperatures', label: 'ZoneTemps', type: 'stream' as const, x: 380, y: 50 },
        { id: 'ZoneHumidity', label: 'ZoneHumidity', type: 'stream' as const, x: 380, y: 120 },
        { id: 'ComfortIndex', label: 'ComfortIndex', type: 'stream' as const, x: 380, y: 190 },
        { id: 'ServerRoom', label: 'ServerRoom', type: 'stream' as const, x: 380, y: 260 },

        // Patterns (Column 4)
        { id: 'TempAnomaly', label: '🔴 TempAnomaly', type: 'pattern' as const, x: 560, y: 50 },
        { id: 'HumidityAlert', label: '🟡 HumidityAlert', type: 'pattern' as const, x: 560, y: 120 },
        { id: 'PowerSpike', label: '🔴 PowerSpike', type: 'pattern' as const, x: 560, y: 190 },
        { id: 'ServerAlert', label: '🔴 ServerAlert', type: 'pattern' as const, x: 560, y: 260 },
    ],
    edges: [
        // Event to Stream
        { from: 'TemperatureReading', to: 'Temperatures' },
        { from: 'HumidityReading', to: 'Humidity' },
        { from: 'HVACStatus', to: 'HVACMetrics' },
        { from: 'EnergyMeter', to: 'Energy' },

        // Stream to Derived
        { from: 'Temperatures', to: 'ZoneTemperatures', label: 'group by zone' },
        { from: 'Temperatures', to: 'ServerRoom', label: 'filter zone' },
        { from: 'Humidity', to: 'ZoneHumidity', label: 'group by zone' },
        { from: 'ZoneTemperatures', to: 'ComfortIndex', label: 'join' },
        { from: 'ZoneHumidity', to: 'ComfortIndex', label: 'join' },

        // Derived to Pattern
        { from: 'ZoneTemperatures', to: 'TempAnomaly', label: 'temp > 28' },
        { from: 'ZoneHumidity', to: 'HumidityAlert', label: 'humidity > 70' },
        { from: 'HVACMetrics', to: 'PowerSpike', label: 'power > 5kW' },
        { from: 'ServerRoom', to: 'ServerAlert', label: 'temp > 25' },
    ]
}

export const SASE_GRAPH = {
    nodes: [
        // Events (Column 1)
        { id: 'LoginEvent', label: '🔐 Login', type: 'event' as const, x: 20, y: 50 },
        { id: 'TransactionEvent', label: '💳 Transaction', type: 'event' as const, x: 20, y: 120 },
        { id: 'PasswordChange', label: '🔑 PwdChange', type: 'event' as const, x: 20, y: 190 },
        { id: 'LogoutEvent', label: '🚪 Logout', type: 'event' as const, x: 20, y: 260 },

        // Streams (Column 2)
        { id: 'Logins', label: 'Logins', type: 'stream' as const, x: 200, y: 50 },
        { id: 'Transactions', label: 'Transactions', type: 'stream' as const, x: 200, y: 120 },
        { id: 'PasswordChanges', label: 'PwdChanges', type: 'stream' as const, x: 200, y: 190 },
        { id: 'Sessions', label: 'Sessions', type: 'stream' as const, x: 200, y: 260 },

        // Derived Streams (Column 3)
        { id: 'UserSessions', label: 'UserSessions', type: 'stream' as const, x: 380, y: 50 },
        { id: 'UserActivity', label: 'UserActivity', type: 'stream' as const, x: 380, y: 120 },
        { id: 'GeoLocations', label: 'GeoLocations', type: 'stream' as const, x: 380, y: 190 },
        { id: 'RiskScore', label: 'RiskScore', type: 'stream' as const, x: 380, y: 260 },

        // Patterns (Column 4)
        { id: 'AccountTakeover', label: '🔴 Takeover', type: 'pattern' as const, x: 560, y: 50 },
        { id: 'ImpossibleTravel', label: '🔴 ImpTravel', type: 'pattern' as const, x: 560, y: 120 },
        { id: 'FraudPattern', label: '🔴 Fraud', type: 'pattern' as const, x: 560, y: 190 },
        { id: 'BruteForce', label: '🟡 BruteForce', type: 'pattern' as const, x: 560, y: 260 },
    ],
    edges: [
        // Event to Stream
        { from: 'LoginEvent', to: 'Logins' },
        { from: 'TransactionEvent', to: 'Transactions' },
        { from: 'PasswordChange', to: 'PasswordChanges' },
        { from: 'LogoutEvent', to: 'Sessions' },

        // Stream to Derived
        { from: 'Logins', to: 'UserSessions', label: 'group by user' },
        { from: 'Logins', to: 'GeoLocations', label: 'extract geo' },
        { from: 'Transactions', to: 'UserActivity', label: 'aggregate' },
        { from: 'PasswordChanges', to: 'RiskScore', label: 'score' },
        { from: 'UserActivity', to: 'RiskScore', label: 'join' },

        // Derived to Pattern
        { from: 'UserSessions', to: 'AccountTakeover', label: 'SEQ login→pwd→txn' },
        { from: 'GeoLocations', to: 'ImpossibleTravel', label: 'geo distance' },
        { from: 'UserActivity', to: 'FraudPattern', label: 'amount > 10k' },
        { from: 'Logins', to: 'BruteForce', label: 'fails > 5' },
    ]
}

export const FINANCIAL_GRAPH = {
    nodes: [
        // Events (Column 1)
        { id: 'MarketTick', label: '📈 MarketTick', type: 'event' as const, x: 20, y: 50 },
        { id: 'OHLCV', label: '📊 OHLCV', type: 'event' as const, x: 20, y: 120 },
        { id: 'OrderBook', label: '📋 OrderBook', type: 'event' as const, x: 20, y: 190 },
        { id: 'NewsEvent', label: '📰 News', type: 'event' as const, x: 20, y: 260 },

        // Streams (Column 2)
        { id: 'Prices', label: 'Prices', type: 'stream' as const, x: 200, y: 50 },
        { id: 'Candles', label: 'Candles', type: 'stream' as const, x: 200, y: 120 },
        { id: 'Liquidity', label: 'Liquidity', type: 'stream' as const, x: 200, y: 190 },
        { id: 'Sentiment', label: 'Sentiment', type: 'stream' as const, x: 200, y: 260 },

        // Derived Streams (Column 3)
        { id: 'SMA', label: 'SMA 20/50', type: 'stream' as const, x: 380, y: 50 },
        { id: 'RSI', label: 'RSI', type: 'stream' as const, x: 380, y: 120 },
        { id: 'Bollinger', label: 'Bollinger', type: 'stream' as const, x: 380, y: 190 },
        { id: 'MACD', label: 'MACD', type: 'stream' as const, x: 380, y: 260 },

        // Patterns (Column 4)
        { id: 'GoldenCross', label: '🟢 GoldenCross', type: 'pattern' as const, x: 560, y: 50 },
        { id: 'DeathCross', label: '🔴 DeathCross', type: 'pattern' as const, x: 560, y: 120 },
        { id: 'Overbought', label: '🟡 Overbought', type: 'pattern' as const, x: 560, y: 190 },
        { id: 'Breakout', label: '🟢 Breakout', type: 'pattern' as const, x: 560, y: 260 },
    ],
    edges: [
        // Event to Stream
        { from: 'MarketTick', to: 'Prices' },
        { from: 'OHLCV', to: 'Candles' },
        { from: 'OrderBook', to: 'Liquidity' },
        { from: 'NewsEvent', to: 'Sentiment' },

        // Stream to Derived
        { from: 'Prices', to: 'SMA', label: 'window 20/50' },
        { from: 'Candles', to: 'RSI', label: 'calc RSI(14)' },
        { from: 'Prices', to: 'Bollinger', label: 'std dev' },
        { from: 'SMA', to: 'MACD', label: 'EMA diff' },

        // Derived to Pattern
        { from: 'SMA', to: 'GoldenCross', label: 'SMA20 > SMA50' },
        { from: 'SMA', to: 'DeathCross', label: 'SMA20 < SMA50' },
        { from: 'RSI', to: 'Overbought', label: 'RSI > 70' },
        { from: 'Bollinger', to: 'Breakout', label: 'price > upper' },
    ]
}
