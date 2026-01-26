import {
    Background,
    Connection,
    Controls,
    Edge,
    MiniMap,
    Node,
    ReactFlow,
    ReactFlowProvider,
    addEdge,
    useEdgesState,
    useNodesState,
    useReactFlow,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { useCallback, useEffect, useRef, useState } from 'react';

import Sidebar from './components/Sidebar';
import Toolbar from './components/Toolbar';
import { nodeTypes } from './nodes';
import ConnectorPanel from './panels/ConnectorPanel';
import type { ConnectorConfig } from './types';
import { generateVPL } from './utils/vplGenerator';

const initialNodes: Node[] = [];
const initialEdges: Edge[] = [];

let nodeId = 0;
const getNodeId = () => `node_${nodeId++}`;

function FlowEditor() {
    const reactFlowWrapper = useRef<HTMLDivElement>(null);
    const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
    const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);
    const [selectedNode, setSelectedNode] = useState<Node | null>(null);
    const [showConnectorPanel, setShowConnectorPanel] = useState(false);
    const [connectorMode, setConnectorMode] = useState<'source' | 'sink'>('source');
    const { screenToFlowPosition } = useReactFlow();

    // VSCode API for communication
    const vscodeRef = useRef<ReturnType<typeof acquireVsCodeApi> | null>(null);

    useEffect(() => {
        // Try to acquire VSCode API (will fail in standalone mode)
        try {
            if (typeof acquireVsCodeApi !== 'undefined') {
                vscodeRef.current = acquireVsCodeApi();

                // Restore state if available
                const state = vscodeRef.current.getState() as { nodes?: Node[]; edges?: Edge[] } | null;
                if (state?.nodes) {
                    setNodes(state.nodes);
                    nodeId = state.nodes.length;
                }
                if (state?.edges) {
                    setEdges(state.edges);
                }
            }
        } catch {
            console.log('Running in standalone mode');
        }
    }, [setNodes, setEdges]);

    // Save state when nodes/edges change
    useEffect(() => {
        if (vscodeRef.current) {
            vscodeRef.current.setState({ nodes, edges });
        }
    }, [nodes, edges]);

    const onConnect = useCallback(
        (params: Connection) => setEdges((eds) => addEdge({ ...params, animated: true }, eds)),
        [setEdges]
    );

    const onNodeClick = useCallback((_: React.MouseEvent, node: Node) => {
        setSelectedNode(node);
        if (node.type === 'source' || node.type === 'sink') {
            setConnectorMode(node.type as 'source' | 'sink');
            setShowConnectorPanel(true);
        }
    }, []);

    const onDragOver = useCallback((event: React.DragEvent) => {
        event.preventDefault();
        event.dataTransfer.dropEffect = 'move';
    }, []);

    const onDrop = useCallback(
        (event: React.DragEvent) => {
            event.preventDefault();

            const type = event.dataTransfer.getData('application/reactflow-type');
            const dataStr = event.dataTransfer.getData('application/reactflow-data');

            if (!type) return;

            const position = screenToFlowPosition({
                x: event.clientX,
                y: event.clientY,
            });

            const extraData = dataStr ? JSON.parse(dataStr) : {};

            const newNode: Node = {
                id: getNodeId(),
                type,
                position,
                data: {
                    label: `${type.charAt(0).toUpperCase() + type.slice(1)} ${nodeId}`,
                    ...extraData
                },
            };

            setNodes((nds) => nds.concat(newNode));
        },
        [screenToFlowPosition, setNodes]
    );

    const onDragStart = useCallback((event: React.DragEvent, nodeType: string, data?: Record<string, unknown>) => {
        event.dataTransfer.setData('application/reactflow-type', nodeType);
        event.dataTransfer.setData('application/reactflow-data', JSON.stringify(data || {}));
        event.dataTransfer.effectAllowed = 'move';
    }, []);

    const handleAddNode = useCallback((type: string) => {
        const newNode: Node = {
            id: getNodeId(),
            type,
            position: { x: 250 + Math.random() * 100, y: 100 + Math.random() * 100 },
            data: { label: `${type.charAt(0).toUpperCase() + type.slice(1)} ${nodeId}` },
        };
        setNodes((nds) => nds.concat(newNode));
    }, [setNodes]);

    const handleSave = useCallback(() => {
        const flow = { nodes, edges, metadata: { name: 'flow', version: '1.0' } };
        const json = JSON.stringify(flow, null, 2);

        if (vscodeRef.current) {
            vscodeRef.current.postMessage({ type: 'save', data: json });
        } else {
            // Standalone mode: download as file
            const blob = new Blob([json], { type: 'application/json' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'flow.json';
            a.click();
            URL.revokeObjectURL(url);
        }
    }, [nodes, edges]);

    const handleExport = useCallback(() => {
        const vplCode = generateVPL(nodes, edges);

        if (vscodeRef.current) {
            vscodeRef.current.postMessage({ type: 'export', data: vplCode });
        } else {
            // Standalone mode: download as file
            const blob = new Blob([vplCode], { type: 'text/plain' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'flow.vpl';
            a.click();
            URL.revokeObjectURL(url);
        }
    }, [nodes, edges]);

    const handleRun = useCallback(() => {
        const vplCode = generateVPL(nodes, edges);

        if (vscodeRef.current) {
            vscodeRef.current.postMessage({ type: 'run', data: vplCode });
        } else {
            console.log('Generated VPL:\n', vplCode);
            alert('VPL code generated! Check console.');
        }
    }, [nodes, edges]);

    const handleClear = useCallback(() => {
        if (confirm('Clear all nodes and edges?')) {
            setNodes([]);
            setEdges([]);
            nodeId = 0;
        }
    }, [setNodes, setEdges]);

    const handleConnectorSave = useCallback((config: ConnectorConfig) => {
        if (selectedNode) {
            setNodes((nds) =>
                nds.map((node) =>
                    node.id === selectedNode.id
                        ? { ...node, data: { ...node.data, label: config.name, connector: config } }
                        : node
                )
            );
        }
        setShowConnectorPanel(false);
        setSelectedNode(null);
    }, [selectedNode, setNodes]);

    return (
        <div className="h-screen flex flex-col bg-vscode-bg text-vscode-fg">
            <Toolbar
                onAddNode={handleAddNode}
                onSave={handleSave}
                onExport={handleExport}
                onRun={handleRun}
                onClear={handleClear}
            />

            <div className="flex-1 flex overflow-hidden">
                <Sidebar onDragStart={onDragStart} />

                <div ref={reactFlowWrapper} className="flex-1">
                    <ReactFlow
                        nodes={nodes}
                        edges={edges}
                        onNodesChange={onNodesChange}
                        onEdgesChange={onEdgesChange}
                        onConnect={onConnect}
                        onNodeClick={onNodeClick}
                        onDrop={onDrop}
                        onDragOver={onDragOver}
                        nodeTypes={nodeTypes}
                        fitView
                        className="bg-vscode-bg"
                    >
                        <Controls className="!bg-vscode-bg !border-vscode-border" />
                        <MiniMap
                            className="!bg-black/30"
                            nodeColor={(node) => {
                                switch (node.type) {
                                    case 'source': return '#22c55e';
                                    case 'sink': return '#eab308';
                                    case 'event': return '#f59e0b';
                                    case 'stream': return '#6366f1';
                                    case 'pattern': return '#14b8a6';
                                    default: return '#6b7280';
                                }
                            }}
                        />
                        <Background color="#444" gap={20} />
                    </ReactFlow>
                </div>
            </div>

            {showConnectorPanel && (
                <ConnectorPanel
                    connector={selectedNode?.data?.connector as ConnectorConfig | undefined}
                    mode={connectorMode}
                    onSave={handleConnectorSave}
                    onCancel={() => {
                        setShowConnectorPanel(false);
                        setSelectedNode(null);
                    }}
                />
            )}
        </div>
    );
}

export default function App() {
    return (
        <ReactFlowProvider>
            <FlowEditor />
        </ReactFlowProvider>
    );
}
