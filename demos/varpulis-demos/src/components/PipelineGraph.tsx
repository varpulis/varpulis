/**
 * Pipeline Graph Visualization using React Flow
 * 
 * Auto-layout DAG showing how events flow through streams using dagre algorithm
 */

import {
    Background,
    BackgroundVariant,
    Handle,
    MarkerType,
    Position,
    ReactFlow,
    useEdgesState,
    useNodesState,
    type Edge,
    type Node,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import dagre from 'dagre'
import { useCallback, useEffect, useMemo } from 'react'

// Node types for the pipeline
export type NodeType = 'event' | 'stream' | 'pattern' | 'alert'

export interface PipelineNode {
    id: string
    label: string
    type: NodeType
    column?: number  // Optional: hint for column placement (0=events, 1=streams, 2=derived, 3=patterns)
}

export interface PipelineEdge {
    from: string
    to: string
    label?: string
}

export interface PipelineGraphProps {
    nodes: PipelineNode[]
    edges: PipelineEdge[]
    eventCounts: Record<string, number>
    streamCounts: Record<string, number>
    patternCounts: Record<string, number>
}

// Colors for each node type
const NODE_STYLES: Record<NodeType, { bg: string, border: string, text: string }> = {
    event: { bg: '#1e40af', border: '#3b82f6', text: '#93c5fd' },
    stream: { bg: '#6b21a8', border: '#a855f7', text: '#d8b4fe' },
    pattern: { bg: '#b45309', border: '#f59e0b', text: '#fcd34d' },
    alert: { bg: '#991b1b', border: '#ef4444', text: '#fca5a5' },
}

// Custom node component with handles for edges
function PipelineNodeComponent({ data }: { data: { label: string, type: NodeType, count: number } }) {
    const style = NODE_STYLES[data.type]
    const isActive = data.count > 0

    return (
        <div
            className="relative px-3 py-2 rounded-md border-2 text-center transition-all"
            style={{
                backgroundColor: style.bg,
                borderColor: style.border,
                opacity: isActive ? 1 : 0.6,
                boxShadow: isActive ? `0 0 10px ${style.border}` : 'none',
                minWidth: '110px',
            }}
        >
            <Handle type="target" position={Position.Left} style={{ background: '#475569' }} />
            <div className="text-xs font-medium whitespace-nowrap" style={{ color: style.text }}>
                {data.label}
            </div>
            {data.count > 0 && (
                <div
                    className="absolute -top-2 -right-2 px-1.5 py-0.5 rounded text-[10px] font-bold text-white"
                    style={{ backgroundColor: style.border }}
                >
                    {data.count > 999 ? '999+' : data.count}
                </div>
            )}
            <Handle type="source" position={Position.Right} style={{ background: '#475569' }} />
        </div>
    )
}

const nodeTypes = {
    pipeline: PipelineNodeComponent,
}

// Apply dagre layout to nodes
function getLayoutedElements(
    nodes: Node[],
    edges: Edge[],
    direction: 'LR' | 'TB' = 'LR'
): { nodes: Node[], edges: Edge[] } {
    const dagreGraph = new dagre.graphlib.Graph()
    dagreGraph.setDefaultEdgeLabel(() => ({}))

    const nodeWidth = 130
    const nodeHeight = 45

    dagreGraph.setGraph({
        rankdir: direction,
        nodesep: 50,
        ranksep: 120,
        marginx: 30,
        marginy: 30,
    })

    nodes.forEach((node) => {
        dagreGraph.setNode(node.id, { width: nodeWidth, height: nodeHeight })
    })

    edges.forEach((edge) => {
        dagreGraph.setEdge(edge.source, edge.target)
    })

    dagre.layout(dagreGraph)

    const layoutedNodes = nodes.map((node) => {
        const nodeWithPosition = dagreGraph.node(node.id)
        return {
            ...node,
            position: {
                x: nodeWithPosition.x - nodeWidth / 2,
                y: nodeWithPosition.y - nodeHeight / 2,
            },
            targetPosition: Position.Left,
            sourcePosition: Position.Right,
        }
    })

    return { nodes: layoutedNodes, edges }
}

export default function PipelineGraph({
    nodes: pipelineNodes,
    edges: pipelineEdges,
    eventCounts,
    streamCounts,
    patternCounts,
}: PipelineGraphProps) {

    // Get count for a node
    const getCount = useCallback((node: PipelineNode): number => {
        if (node.type === 'event') return eventCounts[node.id] || 0
        if (node.type === 'stream') return streamCounts[node.id] || 0
        if (node.type === 'pattern') return patternCounts[node.id] || 0
        return 0
    }, [eventCounts, streamCounts, patternCounts])

    // Convert pipeline nodes/edges to React Flow format
    const { layoutedNodes, layoutedEdges } = useMemo(() => {
        const rfNodes: Node[] = pipelineNodes.map((node) => ({
            id: node.id,
            type: 'pipeline',
            position: { x: 0, y: 0 }, // Will be set by dagre
            data: {
                label: node.label,
                type: node.type,
                count: 0,
            },
        }))

        const rfEdges: Edge[] = pipelineEdges.map((edge, i) => ({
            id: `e${i}-${edge.from}-${edge.to}`,
            source: edge.from,
            target: edge.to,
            label: edge.label,
            labelStyle: {
                fill: '#94a3b8',
                fontSize: 9,
                fontFamily: 'monospace',
            },
            labelBgStyle: {
                fill: '#1e293b',
                fillOpacity: 0.9,
            },
            labelBgPadding: [4, 2] as [number, number],
            labelBgBorderRadius: 3,
            style: { stroke: '#475569', strokeWidth: 1.5 },
            markerEnd: {
                type: MarkerType.ArrowClosed,
                color: '#475569',
                width: 15,
                height: 15,
            },
            animated: false,
        }))

        // Apply dagre layout
        const { nodes: ln, edges: le } = getLayoutedElements(rfNodes, rfEdges, 'LR')
        return { layoutedNodes: ln, layoutedEdges: le }
    }, [pipelineNodes, pipelineEdges])

    const [nodes, setNodes, onNodesChange] = useNodesState(layoutedNodes)
    const [edges, setEdges, onEdgesChange] = useEdgesState(layoutedEdges)

    // Update node counts when they change
    useEffect(() => {
        setNodes((nds) =>
            nds.map((node) => {
                const pipelineNode = pipelineNodes.find((pn) => pn.id === node.id)
                if (!pipelineNode) return node

                const count = getCount(pipelineNode)

                return {
                    ...node,
                    data: {
                        ...node.data,
                        count,
                    },
                }
            })
        )

        // Update edge styles based on activity
        setEdges((eds) =>
            eds.map((edge) => {
                const sourceNode = pipelineNodes.find((pn) => pn.id === edge.source)
                const isActive = sourceNode ? getCount(sourceNode) > 0 : false

                return {
                    ...edge,
                    style: {
                        ...edge.style,
                        stroke: isActive ? '#64748b' : '#334155',
                        strokeWidth: isActive ? 2 : 1.5,
                    },
                    animated: isActive,
                }
            })
        )
    }, [eventCounts, streamCounts, patternCounts, pipelineNodes, getCount, setNodes, setEdges])

    return (
        <div className="w-full h-[400px] bg-slate-900/50 rounded-lg">
            <ReactFlow
                nodes={nodes}
                edges={edges}
                onNodesChange={onNodesChange}
                onEdgesChange={onEdgesChange}
                nodeTypes={nodeTypes}
                fitView
                fitViewOptions={{ padding: 0.1, minZoom: 0.8, maxZoom: 1.2 }}
                minZoom={0.5}
                maxZoom={2}
                defaultViewport={{ x: 0, y: 0, zoom: 1 }}
                nodesDraggable={false}
                nodesConnectable={false}
                elementsSelectable={false}
                panOnDrag={true}
                zoomOnScroll={true}
                zoomOnPinch={true}
                zoomOnDoubleClick={false}
                preventScrolling={false}
                proOptions={{ hideAttribution: true }}
            >
                <Background variant={BackgroundVariant.Dots} gap={20} size={1} color="#334155" />
            </ReactFlow>

            {/* Legend */}
            <div className="flex gap-4 px-3 py-2 text-xs">
                <div className="flex items-center gap-1.5">
                    <div className="w-3 h-3 rounded" style={{ backgroundColor: NODE_STYLES.event.bg }} />
                    <span className="text-slate-400">Event</span>
                </div>
                <div className="flex items-center gap-1.5">
                    <div className="w-3 h-3 rounded" style={{ backgroundColor: NODE_STYLES.stream.bg }} />
                    <span className="text-slate-400">Stream</span>
                </div>
                <div className="flex items-center gap-1.5">
                    <div className="w-3 h-3 rounded" style={{ backgroundColor: NODE_STYLES.pattern.bg }} />
                    <span className="text-slate-400">Pattern</span>
                </div>
                <div className="flex items-center gap-1.5">
                    <div className="w-3 h-3 rounded" style={{ backgroundColor: NODE_STYLES.alert.bg }} />
                    <span className="text-slate-400">Alert</span>
                </div>
            </div>
        </div>
    )
}

// Graph definitions for each demo (simplified - no x/y needed, dagre handles layout)

export const FINANCIAL_PIPELINE = {
    nodes: [
        // Events
        { id: 'MarketTick', label: '📈 MarketTick', type: 'event' as const },
        { id: 'OHLCV', label: '📊 OHLCV', type: 'event' as const },
        { id: 'OrderBook', label: '📋 OrderBook', type: 'event' as const },
        { id: 'NewsEvent', label: '📰 News', type: 'event' as const },

        // Streams
        { id: 'Prices', label: 'Prices', type: 'stream' as const },
        { id: 'Candles', label: 'Candles', type: 'stream' as const },
        { id: 'Liquidity', label: 'Liquidity', type: 'stream' as const },
        { id: 'Sentiment', label: 'Sentiment', type: 'stream' as const },

        // Derived Streams
        { id: 'SMA', label: 'SMA 20/50', type: 'stream' as const },
        { id: 'RSI', label: 'RSI', type: 'stream' as const },
        { id: 'Bollinger', label: 'Bollinger', type: 'stream' as const },
        { id: 'MACD', label: 'MACD', type: 'stream' as const },

        // Patterns
        { id: 'GoldenCross', label: '🟢 GoldenCross', type: 'pattern' as const },
        { id: 'DeathCross', label: '🔴 DeathCross', type: 'pattern' as const },
        { id: 'Overbought', label: '🟡 Overbought', type: 'pattern' as const },
        { id: 'Breakout', label: '🟢 Breakout', type: 'pattern' as const },
    ],
    edges: [
        // Event to Stream
        { from: 'MarketTick', to: 'Prices' },
        { from: 'OHLCV', to: 'Candles' },
        { from: 'OrderBook', to: 'Liquidity' },
        { from: 'NewsEvent', to: 'Sentiment' },

        // Stream to Derived
        { from: 'Prices', to: 'SMA', label: 'window 20/50' },
        { from: 'Candles', to: 'RSI', label: 'RSI(14)' },
        { from: 'Prices', to: 'Bollinger', label: 'std dev' },
        { from: 'SMA', to: 'MACD', label: 'EMA diff' },

        // Derived to Pattern
        { from: 'SMA', to: 'GoldenCross', label: 'SMA20 > SMA50' },
        { from: 'SMA', to: 'DeathCross', label: 'SMA20 < SMA50' },
        { from: 'RSI', to: 'Overbought', label: 'RSI > 70' },
        { from: 'Bollinger', to: 'Breakout', label: 'price > upper' },
    ]
}

export const HVAC_PIPELINE = {
    nodes: [
        // Events
        { id: 'TemperatureReading', label: '🌡️ TempReading', type: 'event' as const },
        { id: 'HumidityReading', label: '💧 Humidity', type: 'event' as const },
        { id: 'HVACStatus', label: '❄️ HVACStatus', type: 'event' as const },
        { id: 'EnergyMeter', label: '⚡ EnergyMeter', type: 'event' as const },

        // Streams
        { id: 'Temperatures', label: 'Temperatures', type: 'stream' as const },
        { id: 'Humidity', label: 'Humidity', type: 'stream' as const },
        { id: 'HVACMetrics', label: 'HVACMetrics', type: 'stream' as const },
        { id: 'Energy', label: 'Energy', type: 'stream' as const },

        // Derived Streams
        { id: 'ZoneTemperatures', label: 'ZoneTemps', type: 'stream' as const },
        { id: 'ComfortIndex', label: 'ComfortIndex', type: 'stream' as const },
        { id: 'ServerRoom', label: 'ServerRoom', type: 'stream' as const },

        // Patterns
        { id: 'TempAnomaly', label: '🔴 TempAnomaly', type: 'pattern' as const },
        { id: 'HumidityAlert', label: '🟡 HumidityAlert', type: 'pattern' as const },
        { id: 'PowerSpike', label: '🔴 PowerSpike', type: 'pattern' as const },
        { id: 'ServerAlert', label: '🔴 ServerAlert', type: 'pattern' as const },
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
        { from: 'ZoneTemperatures', to: 'ComfortIndex', label: 'join' },
        { from: 'Humidity', to: 'ComfortIndex', label: 'join' },

        // Derived to Pattern
        { from: 'ZoneTemperatures', to: 'TempAnomaly', label: 'temp > 28' },
        { from: 'Humidity', to: 'HumidityAlert', label: 'humidity > 70' },
        { from: 'HVACMetrics', to: 'PowerSpike', label: 'power > 5kW' },
        { from: 'ServerRoom', to: 'ServerAlert', label: 'temp > 25' },
    ]
}

export const SASE_PIPELINE = {
    nodes: [
        // Events
        { id: 'Login', label: '🔐 Login', type: 'event' as const },
        { id: 'Transaction', label: '💳 Transaction', type: 'event' as const },
        { id: 'PasswordChange', label: '🔑 PwdChange', type: 'event' as const },
        { id: 'Logout', label: '🚪 Logout', type: 'event' as const },

        // Streams
        { id: 'Logins', label: 'Logins', type: 'stream' as const },
        { id: 'Transactions', label: 'Transactions', type: 'stream' as const },
        { id: 'PasswordChanges', label: 'PwdChanges', type: 'stream' as const },
        { id: 'Sessions', label: 'Sessions', type: 'stream' as const },

        // Derived Streams
        { id: 'UserSessions', label: 'UserSessions', type: 'stream' as const },
        { id: 'UserActivity', label: 'UserActivity', type: 'stream' as const },
        { id: 'GeoLocations', label: 'GeoLocations', type: 'stream' as const },
        { id: 'RiskScore', label: 'RiskScore', type: 'stream' as const },

        // Patterns
        { id: 'AccountTakeover', label: '🔴 Takeover', type: 'pattern' as const },
        { id: 'ImpossibleTravel', label: '🔴 ImpTravel', type: 'pattern' as const },
        { id: 'FraudPattern', label: '🔴 Fraud', type: 'pattern' as const },
        { id: 'BruteForce', label: '🟡 BruteForce', type: 'pattern' as const },
    ],
    edges: [
        // Event to Stream
        { from: 'Login', to: 'Logins' },
        { from: 'Transaction', to: 'Transactions' },
        { from: 'PasswordChange', to: 'PasswordChanges' },
        { from: 'Logout', to: 'Sessions' },

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
