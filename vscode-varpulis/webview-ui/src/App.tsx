import { useCallback, useState, useMemo, useEffect } from 'react';
import {
  ReactFlow,
  MiniMap,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  addEdge,
  Connection,
  BackgroundVariant,
  ReactFlowProvider,
  useReactFlow,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';

import {
  FlowNode,
  FlowEdge,
  ConnectorType,
  PatternType,
  SinkType,
  createDefaultConnectorData,
  createDefaultEventData,
  createDefaultSourceData,
  createDefaultStreamData,
  createDefaultPatternData,
  createDefaultEmitData,
  createDefaultSinkData,
} from './types/flow';
import Sidebar from './components/Sidebar';
import VplPreview from './components/VplPreview';
import { nodeTypes } from './nodes';
import AnimatedEdge from './edges/AnimatedEdge';
import Toolbar from './components/Toolbar';
import { generateVpl } from './utils/vplGenerator';
import { parseVpl } from './utils/vplParser';

// VS Code API type
interface VsCodeApi {
  postMessage: (message: unknown) => void;
  getState: () => unknown;
  setState: (state: unknown) => void;
}

declare global {
  interface Window {
    vscode?: VsCodeApi;
  }
}

const vscode = window.vscode;

const initialNodes: FlowNode[] = [];
const initialEdges: FlowEdge[] = [];

const edgeTypes = {
  animated: AnimatedEdge,
};

// Valid node types
const VALID_NODE_TYPES = ['connector', 'event', 'source', 'stream', 'pattern', 'emit', 'sink'] as const;

interface DragData {
  type: string;
  subtype?: string;
}

function createNodeData(type: string, subtype?: string) {
  switch (type) {
    case 'connector':
      return createDefaultConnectorData((subtype || 'mqtt') as ConnectorType);
    case 'event':
      return createDefaultEventData();
    case 'source':
      return createDefaultSourceData();
    case 'stream':
      return createDefaultStreamData();
    case 'pattern': {
      const data = createDefaultPatternData();
      if (subtype) {
        data.patternType = subtype as PatternType;
        data.label = `${subtype} Pattern`;
      }
      return data;
    }
    case 'emit':
      return createDefaultEmitData();
    case 'sink':
      return createDefaultSinkData((subtype || 'console') as SinkType);
    default:
      return { label: 'Unknown' };
  }
}

function Flow() {
  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);
  const [selectedNode, setSelectedNode] = useState<FlowNode | null>(null);
  const { screenToFlowPosition } = useReactFlow();

  const onConnect = useCallback(
    (connection: Connection) => {
      setEdges((eds) => addEdge({ ...connection, type: 'animated' }, eds));
    },
    [setEdges]
  );

  const onDragOver = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
  }, []);

  const onDrop = useCallback(
    (event: React.DragEvent) => {
      event.preventDefault();

      const rawData = event.dataTransfer.getData('application/reactflow');
      if (!rawData) return;

      let dragData: DragData;
      try {
        dragData = JSON.parse(rawData);
      } catch {
        // Fallback for simple string (old format)
        dragData = { type: rawData };
      }

      // Validate node type
      if (!dragData.type || !VALID_NODE_TYPES.includes(dragData.type as typeof VALID_NODE_TYPES[number])) {
        console.warn('Invalid node type dropped:', dragData.type);
        return;
      }

      const position = screenToFlowPosition({
        x: event.clientX,
        y: event.clientY,
      });

      const newNode: FlowNode = {
        id: `${dragData.type}-${Date.now()}`,
        type: dragData.type,
        position,
        data: createNodeData(dragData.type, dragData.subtype),
      };

      setNodes((nds) => nds.concat(newNode));
    },
    [setNodes, screenToFlowPosition]
  );

  const onNodeClick = useCallback((_event: React.MouseEvent, node: FlowNode) => {
    setSelectedNode(node);
    setNodes((nds) =>
      nds.map((n) => ({
        ...n,
        selected: n.id === node.id,
      }))
    );
  }, [setNodes]);

  const onPaneClick = useCallback(() => {
    setSelectedNode(null);
    setNodes((nds) =>
      nds.map((n) => ({
        ...n,
        selected: false,
      }))
    );
  }, [setNodes]);

  const updateNodeData = useCallback(
    (nodeId: string, data: Partial<FlowNode['data']>) => {
      setNodes((nds) =>
        nds.map((node) => {
          if (node.id === nodeId) {
            return { ...node, data: { ...node.data, ...data } };
          }
          return node;
        })
      );
      // Also update selectedNode if it's the one being edited
      setSelectedNode((prev) => {
        if (prev && prev.id === nodeId) {
          return { ...prev, data: { ...prev.data, ...data } };
        }
        return prev;
      });
    },
    [setNodes]
  );

  const vplCode = useMemo(() => generateVpl(nodes, edges), [nodes, edges]);

  // VS Code message handling
  useEffect(() => {
    const handleMessage = (event: MessageEvent) => {
      const message = event.data;
      switch (message.type) {
        case 'load':
          try {
            const state = JSON.parse(message.data);
            if (state.nodes) setNodes(state.nodes);
            if (state.edges) setEdges(state.edges);
          } catch {
            // Ignore parse errors
          }
          break;
        case 'loadVPL':
          try {
            const parsed = parseVpl(message.data);
            setNodes(parsed.nodes);
            setEdges(parsed.edges);
          } catch {
            // Ignore parse errors
          }
          break;
      }
    };

    window.addEventListener('message', handleMessage);
    return () => window.removeEventListener('message', handleMessage);
  }, [setNodes, setEdges]);

  const handleSave = useCallback(() => {
    if (vscode) {
      vscode.postMessage({
        type: 'save',
        data: JSON.stringify({ nodes, edges }, null, 2),
      });
    }
  }, [nodes, edges]);

  const handleExport = useCallback(() => {
    if (vscode) {
      vscode.postMessage({
        type: 'export',
        data: vplCode,
      });
    }
  }, [vplCode]);

  const handleRun = useCallback(() => {
    if (vscode) {
      vscode.postMessage({
        type: 'run',
        data: vplCode,
      });
    }
  }, [vplCode]);

  const handleImportVPL = useCallback(() => {
    if (vscode) {
      vscode.postMessage({ type: 'importVPL' });
    }
  }, []);

  const handleClear = useCallback(() => {
    setNodes([]);
    setEdges([]);
    setSelectedNode(null);
  }, [setNodes, setEdges]);

  const handleAddNode = useCallback(
    (type: string, subtype?: string) => {
      const newNode: FlowNode = {
        id: `${type}-${Date.now()}`,
        type,
        position: { x: 300 + Math.random() * 100, y: 150 + Math.random() * 100 },
        data: createNodeData(type, subtype),
      };
      setNodes((nds) => nds.concat(newNode));
    },
    [setNodes]
  );

  return (
    <div className="app-container">
      <Toolbar
        onAddNode={handleAddNode}
        onSave={handleSave}
        onExport={handleExport}
        onRun={handleRun}
        onClear={handleClear}
        onImport={handleImportVPL}
      />
      <div className="main-content">
        <Sidebar />
        <div className="flow-container">
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            onDragOver={onDragOver}
            onDrop={onDrop}
            onNodeClick={onNodeClick}
            onPaneClick={onPaneClick}
            nodeTypes={nodeTypes}
            edgeTypes={edgeTypes}
            defaultEdgeOptions={{ type: 'animated' }}
            fitView
          >
            <Controls />
            <MiniMap
              nodeColor={(node) => {
                const colors: Record<string, string> = {
                  connector: '#00bcd4',
                  event: '#f59e0b',
                  source: '#22c55e',
                  stream: '#6366f1',
                  pattern: '#ec4899',
                  emit: '#8b5cf6',
                  sink: '#eab308',
                };
                return colors[node.type || ''] || '#888';
              }}
            />
            <Background variant={BackgroundVariant.Dots} gap={12} size={1} />
          </ReactFlow>
        </div>
        <VplPreview
          code={vplCode}
          selectedNode={selectedNode}
          onUpdateNode={updateNodeData}
        />
      </div>
    </div>
  );
}

export default function App() {
  return (
    <ReactFlowProvider>
      <Flow />
    </ReactFlowProvider>
  );
}
