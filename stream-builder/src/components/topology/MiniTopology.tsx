import { useMemo, useState, useRef, useCallback, useEffect } from 'react'
import dagre from 'dagre'
import { usePipelineStore } from '@/stores/pipelineStore'

interface TopologyProps {
  selectedStreamId?: string
  onSelectStream?: (id: string) => void
}

interface LayoutNode {
  id: string
  name: string
  x: number
  y: number
  w: number
  type: 'stream' | 'connector'
  opCount: number
}

interface LayoutEdge {
  from: string
  to: string
  points: Array<{ x: number; y: number }>
}

export function MiniTopology({ selectedStreamId, onSelectStream }: TopologyProps) {
  const { streams, connectors } = usePipelineStore()
  const svgRef = useRef<SVGSVGElement>(null)

  // Pan & zoom state
  const [viewBox, setViewBox] = useState({ x: 0, y: 0, w: 800, h: 200 })
  const [isPanning, setIsPanning] = useState(false)
  const panStart = useRef({ x: 0, y: 0, vx: 0, vy: 0 })

  const layout = useMemo(() => {
    if (streams.length === 0) return null

    const g = new dagre.graphlib.Graph()
    g.setGraph({ rankdir: 'LR', ranksep: 80, nodesep: 35, marginx: 30, marginy: 20 })
    g.setDefaultEdgeLabel(() => ({}))

    for (const c of connectors) {
      const w = Math.max(80, c.name.length * 8.5 + 24)
      g.setNode(`conn_${c.id}`, { label: c.name, width: w, height: 32 })
    }

    for (const s of streams) {
      const w = Math.max(80, s.name.length * 8.5 + 24)
      g.setNode(s.id, { label: s.name, width: w, height: 32 })
    }

    const streamNames = new Map(streams.map((s) => [s.name, s.id]))
    const connectorNames = new Map(connectors.map((c) => [c.name, c.id]))

    for (const s of streams) {
      if (s.source && streamNames.has(s.source)) {
        g.setEdge(streamNames.get(s.source)!, s.id)
      }
      for (const ref of s.sourceRefs) {
        if (streamNames.has(ref)) {
          g.setEdge(streamNames.get(ref)!, s.id)
        }
      }
      for (const op of s.operations) {
        if (op.type === 'from' && op.value) {
          const connName = op.value.split(',')[0].trim()
          if (connectorNames.has(connName)) {
            g.setEdge(`conn_${connectorNames.get(connName)!}`, s.id)
          }
        }
        if (op.type === 'to' && op.value) {
          const connName = op.value.split(',')[0].trim()
          if (connectorNames.has(connName)) {
            g.setEdge(s.id, `conn_${connectorNames.get(connName)!}`)
          }
        }
      }
    }

    dagre.layout(g)

    const nodes: LayoutNode[] = []
    const edges: LayoutEdge[] = []

    for (const nodeId of g.nodes()) {
      const node = g.node(nodeId)
      if (!node) continue
      const isConnector = nodeId.startsWith('conn_')
      const stream = streams.find((s) => s.id === nodeId)
      nodes.push({
        id: nodeId,
        name: node.label as string,
        x: node.x,
        y: node.y,
        w: node.width ?? 60,
        type: isConnector ? 'connector' : 'stream',
        opCount: stream?.operations.length ?? 0,
      })
    }

    for (const edge of g.edges()) {
      const edgeData = g.edge(edge)
      edges.push({ from: edge.v, to: edge.w, points: edgeData.points || [] })
    }

    if (nodes.length === 0) return null

    const pad = 30
    const minX = Math.min(...nodes.map((n) => n.x - n.w / 2)) - pad
    const maxX = Math.max(...nodes.map((n) => n.x + n.w / 2)) + pad
    const minY = Math.min(...nodes.map((n) => n.y - 18)) - pad
    const maxY = Math.max(...nodes.map((n) => n.y + 18)) + pad

    return { nodes, edges, bounds: { x: minX, y: minY, w: maxX - minX, h: maxY - minY } }
  }, [streams, connectors])

  // Fit to bounds when layout changes
  useEffect(() => {
    if (layout) {
      setViewBox({ ...layout.bounds })
    }
  }, [layout])

  // Zoom with mouse wheel
  const handleWheel = useCallback((e: React.WheelEvent) => {
    e.preventDefault()
    const svg = svgRef.current
    if (!svg) return

    const rect = svg.getBoundingClientRect()
    const mx = (e.clientX - rect.left) / rect.width
    const my = (e.clientY - rect.top) / rect.height

    const factor = e.deltaY > 0 ? 1.15 : 0.87
    setViewBox((vb) => {
      const newW = vb.w * factor
      const newH = vb.h * factor
      return {
        x: vb.x + (vb.w - newW) * mx,
        y: vb.y + (vb.h - newH) * my,
        w: newW,
        h: newH,
      }
    })
  }, [])

  // Pan with mouse drag
  const handleMouseDown = useCallback((e: React.MouseEvent) => {
    if (e.button !== 0) return
    setIsPanning(true)
    panStart.current = { x: e.clientX, y: e.clientY, vx: viewBox.x, vy: viewBox.y }
  }, [viewBox])

  const handleMouseMove = useCallback((e: React.MouseEvent) => {
    if (!isPanning) return
    const svg = svgRef.current
    if (!svg) return
    const rect = svg.getBoundingClientRect()
    const dx = (e.clientX - panStart.current.x) / rect.width * viewBox.w
    const dy = (e.clientY - panStart.current.y) / rect.height * viewBox.h
    setViewBox((vb) => ({
      ...vb,
      x: panStart.current.vx - dx,
      y: panStart.current.vy - dy,
    }))
  }, [isPanning, viewBox.w, viewBox.h])

  const handleMouseUp = useCallback(() => setIsPanning(false), [])

  // Fit all
  const fitAll = useCallback(() => {
    if (layout) setViewBox({ ...layout.bounds })
  }, [layout])

  if (!layout) {
    return (
      <div className="h-full flex items-center justify-center text-xs text-muted-foreground/40">
        Add streams to see topology
      </div>
    )
  }

  const vb = `${viewBox.x} ${viewBox.y} ${viewBox.w} ${viewBox.h}`

  return (
    <div className="h-full w-full relative">
      <svg
        ref={svgRef}
        viewBox={vb}
        className={`h-full w-full ${isPanning ? 'cursor-grabbing' : 'cursor-grab'}`}
        onWheel={handleWheel}
        onMouseDown={handleMouseDown}
        onMouseMove={handleMouseMove}
        onMouseUp={handleMouseUp}
        onMouseLeave={handleMouseUp}
      >
        <defs>
          <marker id="arrowhead" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" className="fill-muted-foreground/40" />
          </marker>
        </defs>

        {/* Edges */}
        {layout.edges.map((edge, i) => {
          if (edge.points.length < 2) return null
          const d = edge.points.map((p, j) => `${j === 0 ? 'M' : 'L'} ${p.x} ${p.y}`).join(' ')
          return (
            <path
              key={`e-${i}`}
              d={d}
              fill="none"
              stroke="currentColor"
              strokeWidth={1.5}
              className="text-muted-foreground/30"
              markerEnd="url(#arrowhead)"
            />
          )
        })}

        {/* Nodes */}
        {layout.nodes.map((node) => {
          const isSelected = node.id === selectedStreamId
          const isConnector = node.type === 'connector'
          const rx = isConnector ? 14 : 6

          return (
            <g
              key={node.id}
              className="cursor-pointer"
              onClick={(e) => {
                e.stopPropagation()
                if (!isConnector) onSelectStream?.(node.id)
              }}
            >
              <rect
                x={node.x - node.w / 2}
                y={node.y - 15}
                width={node.w}
                height={30}
                rx={rx}
                className={
                  isSelected
                    ? 'fill-primary/20 stroke-primary'
                    : isConnector
                      ? 'fill-muted/80 stroke-muted-foreground/20'
                      : node.opCount > 0
                        ? 'fill-accent stroke-border hover:fill-primary/10'
                        : 'fill-card stroke-border/50 hover:fill-primary/10'
                }
                strokeWidth={isSelected ? 2.5 : 1}
              />
              <text
                x={node.x}
                y={node.y + 5}
                textAnchor="middle"
                className={`text-[12px] select-none pointer-events-none ${
                  isSelected ? 'fill-primary font-semibold' :
                  isConnector ? 'fill-muted-foreground/60 italic' :
                  'fill-foreground/80'
                }`}
                style={{ fontFamily: 'system-ui, sans-serif' }}
              >
                {node.name}
              </text>
              {node.opCount > 0 && !isConnector && (
                <>
                  <circle
                    cx={node.x + node.w / 2 - 2}
                    cy={node.y - 13}
                    r={8}
                    className="fill-primary/80"
                  />
                  <text
                    x={node.x + node.w / 2 - 2}
                    y={node.y - 9}
                    textAnchor="middle"
                    className="fill-primary-foreground text-[9px] font-bold select-none pointer-events-none"
                    style={{ fontFamily: 'system-ui' }}
                  >
                    {node.opCount}
                  </text>
                </>
              )}
            </g>
          )
        })}
      </svg>

      {/* Fit-all button */}
      <button
        className="absolute top-1 right-1 px-1.5 py-0.5 text-[9px] text-muted-foreground bg-card/80 border border-border rounded hover:bg-accent hover:text-foreground transition-colors"
        onClick={fitAll}
        title="Fit to view"
      >
        Fit
      </button>
    </div>
  )
}
