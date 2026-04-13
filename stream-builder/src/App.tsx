import { useState, useCallback, useEffect, useRef } from 'react'
import { Toolbar } from '@/components/layout/Toolbar'
import { Sidebar } from '@/components/sidebar/Sidebar'
import { PipelineCanvas } from '@/components/canvas/PipelineCanvas'
import { VplEditor, type VplEditorHandle } from '@/components/editor/VplEditor'
import { MiniTopology } from '@/components/topology/MiniTopology'
import { usePipelineStore } from '@/stores/pipelineStore'
import { generateVplWithMap, buildSourceMapFromCode, findEntityAtLine, type SourceMapEntry } from '@/utils/vplGenerator'
import { parseVpl } from '@/utils/vplParser'
import { parseVplToGraph, generateVplFromGraph } from '@/api/varpulisClient'
import { graphToStreams, stateToGraph, generateConnEvtVpl } from '@/utils/graphConverter'
import { useTheme } from '@/hooks/useTheme'

export default function App() {
  const [vplCode, setVplCode] = useState('')
  const [editorWidth, setEditorWidth] = useState(420)
  const [dragging, setDragging] = useState(false)
  const [selectedStreamId, setSelectedStreamId] = useState<string | undefined>()
  const [syncDirection, setSyncDirection] = useState<'visual' | 'code' | 'idle'>('idle')
  const syncTimer = useRef<ReturnType<typeof setTimeout> | null>(null)
  const lastCodeFromVisual = useRef('')
  const sourceMapRef = useRef<SourceMapEntry[]>([])
  const editorRef = useRef<VplEditorHandle>(null)
  const cursorSyncLock = useRef(false)

  const pipeline = usePipelineStore()
  const { theme, toggleTheme } = useTheme()

  // Delete selected stream (used by keyboard shortcut via Toolbar)
  const handleDeleteSelected = useCallback(() => {
    if (selectedStreamId) {
      pipeline.removeStream(selectedStreamId)
      setSelectedStreamId(undefined)
      return true
    }
    return false
  }, [selectedStreamId, pipeline])

  const serverSyncTimer = useRef<ReturnType<typeof setTimeout> | null>(null)

  // Visual -> Code: regenerate VPL when pipeline state changes (but not during code editing)
  // Uses client-side generation for instant UX + source map, then optionally
  // syncs with server via POST /pipeline/generate for authoritative stream code.
  useEffect(() => {
    // Always cancel pending server sync first — prevents stale timer from overwriting
    // user edits when syncDirection transitions to 'code'.
    if (serverSyncTimer.current) clearTimeout(serverSyncTimer.current)
    if (syncDirection === 'code') return
    const state = {
      connectors: pipeline.connectors,
      events: pipeline.events,
      streams: pipeline.streams,
    }

    // 1. Client-side: instant update with source map
    const result = generateVplWithMap(state)
    lastCodeFromVisual.current = result.code
    sourceMapRef.current = result.sourceMap
    setVplCode(result.code)
    if (syncDirection !== 'idle') setSyncDirection('visual')

    // 2. Server-side: POST /pipeline/generate for authoritative stream VPL (debounced)
    const activeStreams = state.streams.filter((s) => s.source || s.operations.length > 0)
    if (activeStreams.length > 0) {
      serverSyncTimer.current = setTimeout(() => {
        const graph = stateToGraph(state.streams)
        generateVplFromGraph(graph).then((res) => {
          if (res.ok && res.vpl && syncDirection !== 'code') {
            // Combine: client-side connector/event VPL + server stream VPL
            const connEvtVpl = generateConnEvtVpl(state.connectors, state.events)
            const fullCode = (connEvtVpl + res.vpl).trimEnd() + '\n'
            if (fullCode.trimEnd() !== lastCodeFromVisual.current.trimEnd()) {
              // Server produced different stream code — update editor
              lastCodeFromVisual.current = fullCode
              sourceMapRef.current = buildSourceMapFromCode(fullCode, state)
              setVplCode(fullCode)
            }
          }
        }).catch(() => {
          // Server unavailable — client-side code is already set
        })
      }, 500)
    }
  }, [pipeline.connectors, pipeline.events, pipeline.streams, syncDirection])

  // Code -> Visual: parse VPL and update the store.
  // Uses server API (POST /pipeline/graph) as primary, falls back to client-side parsing.
  const handleCodeChange = useCallback((code: string) => {
    setVplCode(code)

    // Don't re-parse if this code came from our own visual sync
    if (code === lastCodeFromVisual.current) return

    setSyncDirection('code')

    // Debounce: parse after 800ms idle
    if (syncTimer.current) clearTimeout(syncTimer.current)
    syncTimer.current = setTimeout(async () => {
      // Always parse connectors + events client-side (server graph doesn't include them)
      let clientParsed: ReturnType<typeof parseVpl> | null = null
      try {
        clientParsed = parseVpl(code)
      } catch {
        // Client parse failed — user is still typing
      }

      try {
        // Try server-side parsing for authoritative stream extraction
        const serverResult = await parseVplToGraph(code)
        if (serverResult.ok && serverResult.graph) {
          const serverStreams = graphToStreams(serverResult.graph.nodes, serverResult.graph.edges)
          // Use server streams if meaningful, otherwise fall back to client
          const streams = serverStreams.length > 0 ? serverStreams : (clientParsed?.streams ?? [])
          const connectors = clientParsed?.connectors ?? []
          const events = clientParsed?.events ?? []
          if (streams.length > 0 || connectors.length > 0 || events.length > 0) {
            pipeline.loadPipeline({ connectors, events, streams })
          }
        } else {
          // Server returned parse error — fall back to client-side
          if (clientParsed && (clientParsed.streams.length > 0 || clientParsed.connectors.length > 0 || clientParsed.events.length > 0)) {
            pipeline.loadPipeline(clientParsed)
          }
        }
      } catch {
        // Server unavailable — fall back entirely to client-side
        if (clientParsed && (clientParsed.streams.length > 0 || clientParsed.connectors.length > 0 || clientParsed.events.length > 0)) {
          pipeline.loadPipeline(clientParsed)
        }
      }
      setSyncDirection('idle')
    }, 800)
  }, [pipeline])

  // Cursor sync: card click -> scroll Monaco to that stream's line
  const handleSelectStream = useCallback((id: string) => {
    setSelectedStreamId(id)
    cursorSyncLock.current = true
    const entry = sourceMapRef.current.find(
      (e) => e.entityType === 'stream' && e.entityId === id
    )
    if (entry && editorRef.current) {
      editorRef.current.revealLine(entry.startLine)
    }
    // Release lock after a short delay to avoid bounce-back from cursor event
    setTimeout(() => { cursorSyncLock.current = false }, 200)
  }, [])

  // Cursor sync: Monaco cursor position -> select corresponding card
  const handleCursorLine = useCallback((line: number) => {
    if (cursorSyncLock.current) return
    const entry = findEntityAtLine(sourceMapRef.current, line)
    if (entry && entry.entityType === 'stream') {
      setSelectedStreamId(entry.entityId)
    }
  }, [])

  const handleMouseDown = useCallback(() => setDragging(true), [])
  const handleMouseMove = useCallback(
    (e: React.MouseEvent) => {
      if (!dragging) return
      setEditorWidth(Math.max(280, Math.min(800, window.innerWidth - e.clientX)))
    },
    [dragging]
  )
  const handleMouseUp = useCallback(() => setDragging(false), [])

  // Topology panel -- resizable height
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
      <Toolbar editorRef={editorRef} onDeleteSelected={handleDeleteSelected} theme={theme} toggleTheme={toggleTheme} />

      <div className="flex flex-1 overflow-hidden">
        <Sidebar selectedStreamId={selectedStreamId} onSelectStream={handleSelectStream} />
        <PipelineCanvas selectedStreamId={selectedStreamId} onSelectStream={handleSelectStream} />

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
            ref={editorRef}
            value={vplCode}
            onChange={handleCodeChange}
            onCursorLine={handleCursorLine}
            className="flex-1"
            theme={theme}
          />
        </div>
      </div>

      {/* Topology panel -- resizable */}
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
            {topoExpanded ? '\u25be' : '\u25b8'} Topology
          </span>
          <div className="flex-1" />
          <span className="tabular-nums">
            {pipeline.streams.length} streams \u00b7 {pipeline.connectors.length} connectors \u00b7 {pipeline.events.length} events
          </span>
        </div>
        {topoExpanded && (
          <div className="flex-1 overflow-hidden px-3 pb-2">
            <MiniTopology
              selectedStreamId={selectedStreamId}
              onSelectStream={handleSelectStream}
            />
          </div>
        )}
      </div>
    </div>
  )
}
