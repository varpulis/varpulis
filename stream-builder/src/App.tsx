import { useState, useCallback, useEffect, useRef } from 'react'
import { Toolbar } from '@/components/layout/Toolbar'
import { Sidebar } from '@/components/sidebar/Sidebar'
import { PipelineCanvas } from '@/components/canvas/PipelineCanvas'
import { VplEditor } from '@/components/editor/VplEditor'
import { MiniTopology } from '@/components/topology/MiniTopology'
import { usePipelineStore } from '@/stores/pipelineStore'
import { generateVpl } from '@/utils/vplGenerator'
import { parseVpl } from '@/utils/vplParser'

export default function App() {
  const [vplCode, setVplCode] = useState('')
  const [editorWidth, setEditorWidth] = useState(420)
  const [dragging, setDragging] = useState(false)
  const [selectedStreamId, setSelectedStreamId] = useState<string | undefined>()
  const [syncDirection, setSyncDirection] = useState<'visual' | 'code' | 'idle'>('idle')
  const syncTimer = useRef<ReturnType<typeof setTimeout> | null>(null)
  const lastCodeFromVisual = useRef('')

  const pipeline = usePipelineStore()

  // Visual → Code: regenerate VPL when pipeline state changes (but not during code editing)
  useEffect(() => {
    if (syncDirection === 'code') return
    const state = {
      connectors: pipeline.connectors,
      events: pipeline.events,
      streams: pipeline.streams,
    }
    const code = generateVpl(state)
    lastCodeFromVisual.current = code
    setVplCode(code)
    if (syncDirection !== 'idle') setSyncDirection('visual')
  }, [pipeline.connectors, pipeline.events, pipeline.streams, syncDirection])

  // Code → Visual: parse VPL and update the store
  const handleCodeChange = useCallback((code: string) => {
    setVplCode(code)

    // Don't re-parse if this code came from our own visual sync
    if (code === lastCodeFromVisual.current) return

    setSyncDirection('code')

    // Debounce: parse after 800ms idle
    if (syncTimer.current) clearTimeout(syncTimer.current)
    syncTimer.current = setTimeout(() => {
      try {
        const parsed = parseVpl(code)
        // Only update if parsing produced something meaningful
        if (parsed.streams.length > 0 || parsed.connectors.length > 0 || parsed.events.length > 0) {
          pipeline.loadPipeline(parsed)
        }
      } catch {
        // Parse error — don't update visual, user is still typing
      }
      setSyncDirection('idle')
    }, 800)
  }, [pipeline])

  const handleMouseDown = useCallback(() => setDragging(true), [])
  const handleMouseMove = useCallback(
    (e: React.MouseEvent) => {
      if (!dragging) return
      setEditorWidth(Math.max(280, Math.min(800, window.innerWidth - e.clientX)))
    },
    [dragging]
  )
  const handleMouseUp = useCallback(() => setDragging(false), [])

  // Topology panel — resizable height
  const [topoExpanded, setTopoExpanded] = useState(true)
  const [topoHeight, setTopoHeight] = useState(180)
  const [topoDragging, setTopoDragging] = useState(false)

  const handleTopoMouseDown = useCallback(() => setTopoDragging(true), [])
  const handleTopoMouseMove = useCallback(
    (e: React.MouseEvent) => {
      if (topoDragging) {
        const newHeight = window.innerHeight - e.clientY
        setTopoHeight(Math.max(80, Math.min(400, newHeight)))
      }
    },
    [topoDragging]
  )

  return (
    <div
      className="flex flex-col h-screen w-screen bg-background select-none"
      onMouseMove={(e) => { handleMouseMove(e); handleTopoMouseMove(e) }}
      onMouseUp={() => { handleMouseUp(); setTopoDragging(false) }}
      onMouseLeave={() => { handleMouseUp(); setTopoDragging(false) }}
    >
      <Toolbar />

      <div className="flex flex-1 overflow-hidden">
        <Sidebar selectedStreamId={selectedStreamId} onSelectStream={setSelectedStreamId} />
        <PipelineCanvas selectedStreamId={selectedStreamId} onSelectStream={setSelectedStreamId} />

        {/* Resize handle */}
        <div
          className={`w-1.5 cursor-col-resize transition-colors flex-shrink-0 ${
            dragging ? 'bg-primary/50' : 'bg-border hover:bg-primary/30'
          }`}
          onMouseDown={handleMouseDown}
        />

        {/* Monaco Editor pane */}
        <div className="flex-shrink-0 overflow-hidden flex flex-col" style={{ width: editorWidth }}>
          <div className="h-7 bg-card border-b border-border flex items-center px-3 gap-2">
            <span className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider">VPL Source</span>
            <div className="flex-1" />
            <span className={`text-[9px] px-1.5 py-0.5 rounded font-medium ${
              syncDirection === 'code'
                ? 'bg-amber-500/10 text-amber-500'
                : 'bg-green-500/10 text-green-500'
            }`}>
              {syncDirection === 'code' ? 'parsing...' : 'synced'}
            </span>
          </div>
          <VplEditor
            value={vplCode}
            onChange={handleCodeChange}
            className="flex-1"
          />
        </div>
      </div>

      {/* Topology panel — resizable */}
      {topoExpanded && (
        <div
          className={`h-1.5 cursor-row-resize flex-shrink-0 transition-colors ${
            topoDragging ? 'bg-primary/50' : 'bg-border hover:bg-primary/30'
          }`}
          onMouseDown={handleTopoMouseDown}
        />
      )}
      <div
        className={`bg-card flex flex-col flex-shrink-0 ${
          topoExpanded ? '' : ''
        }`}
        style={{ height: topoExpanded ? topoHeight : 28 }}
      >
        <div
          className="h-7 flex items-center px-3 text-[10px] text-muted-foreground cursor-pointer hover:bg-accent/30 flex-shrink-0 border-t border-border"
          onClick={() => setTopoExpanded(!topoExpanded)}
        >
          <span className="font-semibold uppercase tracking-wider">
            {topoExpanded ? '▾' : '▸'} Topology
          </span>
          <div className="flex-1" />
          <span className="tabular-nums">
            {pipeline.streams.length} streams · {pipeline.connectors.length} connectors · {pipeline.events.length} events
          </span>
        </div>
        {topoExpanded && (
          <div className="flex-1 overflow-hidden px-3 pb-2">
            <MiniTopology
              selectedStreamId={selectedStreamId}
              onSelectStream={setSelectedStreamId}
            />
          </div>
        )}
      </div>
    </div>
  )
}
