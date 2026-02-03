import { FlowNode, FlowEdge, SourceNodeData, SinkNodeData, StreamNodeData, EventNodeData, PatternNodeData } from '../types/flow';
import dagre from 'dagre';

interface ParseResult {
  nodes: FlowNode[];
  edges: FlowEdge[];
}

export function parseVpl(code: string): ParseResult {
  const nodes: FlowNode[] = [];
  const edges: FlowEdge[] = [];
  
  const lines = code.split('\n').map(l => l.trim()).filter(l => l && !l.startsWith('#'));
  
  let lastNodeId: string | null = null;
  let yOffset = 0;
  let nodeCounter = 0;
  
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    
    // Parse event declaration
    if (line.startsWith('event ')) {
      const match = line.match(/^event\s+(\w+)/);
      if (match) {
        const name = match[1];
        const fields: Array<{name: string; type: string}> = [];
        
        let j = i + 1;
        while (j < lines.length && !lines[j].includes('}')) {
          const fieldMatch = lines[j].match(/(\w+):\s*(\w+)/);
          if (fieldMatch) {
            fields.push({ name: fieldMatch[1], type: fieldMatch[2] });
          }
          j++;
        }
        
        const nodeId = `event-${nodeCounter++}`;
        nodes.push({
          id: nodeId,
          type: 'event',
          position: { x: 50, y: yOffset },
          data: { label: name, fields } as EventNodeData,
        });
        yOffset += 150;
        i = j;
      }
    }
    
    // Parse pattern declaration
    else if (line.startsWith('pattern ')) {
      const match = line.match(/^pattern\s+(\w+)\s*=\s*(SEQ|AND|OR)\(/);
      if (match) {
        const name = match[1];
        const patternType = match[2] as 'SEQ' | 'AND' | 'OR';
        
        let within = '5m';
        for (let j = i; j < Math.min(i + 5, lines.length); j++) {
          const withinMatch = lines[j].match(/within\s+(\d+\w+)/);
          if (withinMatch) {
            within = withinMatch[1];
            break;
          }
        }
        
        const nodeId = `pattern-${nodeCounter++}`;
        nodes.push({
          id: nodeId,
          type: 'pattern',
          position: { x: 50, y: yOffset },
          data: { 
            label: name, 
            patternType, 
            events: [], 
            within 
          } as PatternNodeData,
        });
        yOffset += 150;
      }
    }
    
    // Parse stream declaration
    else if (line.startsWith('stream ')) {
      const match = line.match(/^stream\s+(\w+)(?:\s+from\s+"([^"]+)")?/);
      if (match) {
        const name = match[1];
        const uri = match[2];
        
        if (uri) {
          const sourceId = `source-${nodeCounter++}`;
          const sourceType = uri.startsWith('mqtt') ? 'mqtt' : 
                           uri.startsWith('kafka') ? 'kafka' : 
                           uri.startsWith('file') ? 'file' : 'http';
          
          nodes.push({
            id: sourceId,
            type: 'source',
            position: { x: 50, y: yOffset },
            data: { label: name, sourceType, uri } as SourceNodeData,
          });
          
          lastNodeId = sourceId;
        } else {
          const streamId = `stream-${nodeCounter++}`;
          nodes.push({
            id: streamId,
            type: 'stream',
            position: { x: 250, y: yOffset },
            data: { label: name } as StreamNodeData,
          });
          
          lastNodeId = streamId;
        }
        
        yOffset += 150;
      }
    }
    
    // Parse stream operations
    else if (line.startsWith('.')) {
      if (line.startsWith('.emit()') && lastNodeId) {
        const sinkId = `sink-${nodeCounter++}`;
        nodes.push({
          id: sinkId,
          type: 'sink',
          position: { x: 450, y: yOffset - 150 },
          data: { label: 'Console', sinkType: 'console' } as SinkNodeData,
        });
        
        edges.push({
          id: `edge-${lastNodeId}-${sinkId}`,
          source: lastNodeId,
          target: sinkId,
          type: 'animated',
        });
      }
    }
  }
  
  return applyDagreLayout({ nodes, edges });
}

function applyDagreLayout(result: ParseResult): ParseResult {
  if (result.nodes.length === 0) return result;
  
  const g = new dagre.graphlib.Graph();
  g.setGraph({ rankdir: 'LR', nodesep: 50, ranksep: 100 });
  g.setDefaultEdgeLabel(() => ({}));
  
  result.nodes.forEach(node => {
    g.setNode(node.id, { width: 180, height: 100 });
  });
  
  result.edges.forEach(edge => {
    g.setEdge(edge.source, edge.target);
  });
  
  dagre.layout(g);
  
  const layoutedNodes = result.nodes.map(node => {
    const nodeWithPosition = g.node(node.id);
    return {
      ...node,
      position: {
        x: nodeWithPosition.x - 90,
        y: nodeWithPosition.y - 50,
      },
    };
  });
  
  return { nodes: layoutedNodes, edges: result.edges };
}