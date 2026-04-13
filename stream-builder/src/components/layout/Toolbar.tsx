import { useState, useEffect, type RefObject } from 'react'
import { Plus, Play, Check, Undo2, Redo2, Trash2, Sparkles, Loader2, Settings, X, CheckCircle2, AlertCircle, Moon, Sun, FolderOpen } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Separator } from '@/components/ui/separator'
import { Input } from '@/components/ui/input'
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from '@/components/ui/dialog'
import { usePipelineStore } from '@/stores/pipelineStore'
import { DEMO_PIPELINE } from '@/utils/demoPipeline'
import { generateVpl } from '@/utils/vplGenerator'
import { validateVplServer, deployPipeline, checkHealth, getServerConfig, setServerConfig, listPipelines, getPipeline } from '@/api/varpulisClient'
import { parseVpl } from '@/utils/vplParser'
import type { VplEditorHandle } from '@/components/editor/VplEditor'

type ToastType = 'success' | 'error' | 'info'

interface ToolbarProps {
  editorRef?: RefObject<VplEditorHandle | null>
  onDeleteSelected?: () => boolean
  theme: 'light' | 'dark'
  toggleTheme: () => void
}

export function Toolbar({ editorRef, onDeleteSelected, theme, toggleTheme }: ToolbarProps) {
  const { addStream, clear, loadPipeline, streams, connectors, events, undo, redo, canUndo, canRedo } = usePipelineStore()
  const [validating, setValidating] = useState(false)
  const [deploying, setDeploying] = useState(false)
  const [toast, setToast] = useState<{ message: string; type: ToastType } | null>(null)
  const [showSettings, setShowSettings] = useState(false)
  const [serverUrl, setServerUrl] = useState(getServerConfig().url)
  const [apiKey, setApiKey] = useState(getServerConfig().apiKey)
  const [deployName, setDeployName] = useState('')
  const [showDeployDialog, setShowDeployDialog] = useState(false)
  const [showLoadDialog, setShowLoadDialog] = useState(false)
  const [loadingPipelines, setLoadingPipelines] = useState(false)
  const [pipelineList, setPipelineList] = useState<Array<{ id: string; name: string; status: string; source?: string }>>([])
  const showToast = (message: string, type: ToastType) => {
    setToast({ message, type })
    setTimeout(() => setToast(null), 5000)
  }

  const ensureServer = (): boolean => {
    const { url } = getServerConfig()
    if (!url) {
      showToast('Configure server URL first (gear icon)', 'info')
      setShowSettings(true)
      return false
    }
    return true
  }

  const handleValidate = async () => {
    if (!ensureServer()) return
    const vpl = generateVpl({ connectors, events, streams })
    if (!vpl.trim() || streams.length === 0) {
      showToast('Nothing to validate — add some streams first', 'info')
      return
    }
    setValidating(true)

    // Clear previous markers
    editorRef?.current?.clearMarkers()

    const result = await validateVplServer(vpl)
    setValidating(false)

    // Set Monaco markers from diagnostics
    if (result.diagnostics.length > 0 && editorRef?.current) {
      editorRef.current.setMarkers(result.diagnostics)

      // Scroll to first error
      const firstError = result.diagnostics.find((d) => d.severity === 'error')
      if (firstError && firstError.start_line > 0) {
        editorRef.current.revealLine(firstError.start_line)
      }
    }

    if (result.valid) {
      const warnings = result.diagnostics.filter((d) => d.severity === 'warning')
      if (warnings.length > 0) {
        showToast(`VPL valid with ${warnings.length} warning${warnings.length > 1 ? 's' : ''}`, 'info')
      } else {
        showToast('VPL syntax is valid', 'success')
      }
    } else {
      const errorCount = result.diagnostics.filter((d) => d.severity === 'error').length
      const warnCount = result.diagnostics.filter((d) => d.severity === 'warning').length
      const parts: string[] = []
      if (errorCount > 0) parts.push(`${errorCount} error${errorCount > 1 ? 's' : ''}`)
      if (warnCount > 0) parts.push(`${warnCount} warning${warnCount > 1 ? 's' : ''}`)
      showToast(`Validation: ${parts.join(', ') || result.error}`, 'error')
    }
  }

  const handleDeployClick = () => {
    if (!ensureServer()) return
    const vpl = generateVpl({ connectors, events, streams })
    if (!vpl.trim() || streams.length === 0) {
      showToast('Nothing to deploy — add some streams first', 'info')
      return
    }
    setDeployName(`pipeline-${Date.now()}`)
    setShowDeployDialog(true)
  }

  const handleDeploy = async () => {
    const vpl = generateVpl({ connectors, events, streams })
    setShowDeployDialog(false)
    setDeploying(true)
    const result = await deployPipeline(deployName, vpl)
    setDeploying(false)
    if (result.ok) {
      showToast(`Deployed! Pipeline ID: ${result.id?.slice(0, 8)}...`, 'success')
    } else {
      showToast(`Deploy failed: ${result.error}`, 'error')
    }
  }

  const handleSaveSettings = () => {
    setServerConfig(serverUrl, apiKey)
    setShowSettings(false)
    // Test connection
    checkHealth().then((h) => {
      if (h.healthy) {
        showToast(`Connected to Varpulis v${h.version}`, 'success')
      } else {
        showToast(`Cannot reach server: ${h.error}`, 'error')
      }
    })
  }

  const handleLoadClick = async () => {
    if (!ensureServer()) return
    setShowLoadDialog(true)
    setLoadingPipelines(true)
    const result = await listPipelines()
    setPipelineList(result.pipelines)
    setLoadingPipelines(false)
    if (result.error) {
      showToast(`Failed to list pipelines: ${result.error}`, 'error')
    }
  }

  const handleLoadPipeline = async (pipeline: { id: string; name: string; source?: string }) => {
    setShowLoadDialog(false)
    // If pipeline has source inline, use it directly
    let source = pipeline.source
    if (!source) {
      const result = await getPipeline(pipeline.id)
      if (!result.ok || !result.pipeline) {
        showToast(`Failed to load pipeline: ${result.error}`, 'error')
        return
      }
      source = result.pipeline.source
    }
    // Parse the VPL source and load into store
    try {
      const parsed = parseVpl(source)
      if (parsed.streams.length > 0 || parsed.connectors.length > 0 || parsed.events.length > 0) {
        loadPipeline(parsed)
        showToast(`Loaded "${pipeline.name}"`, 'success')
      } else {
        showToast('Pipeline has no parseable content', 'info')
      }
    } catch {
      showToast('Failed to parse pipeline VPL', 'error')
    }
  }

  // Keyboard shortcuts
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      const mod = e.ctrlKey || e.metaKey
      // Ignore shortcuts when typing in input/textarea
      const tag = (e.target as HTMLElement)?.tagName
      const isInput = tag === 'INPUT' || tag === 'TEXTAREA'

      if (mod && e.key === 's') {
        e.preventDefault()
        handleDeployClick()
      } else if (mod && e.shiftKey && e.key === 'z') {
        e.preventDefault()
        redo()
      } else if (mod && e.key === 'z' && !e.shiftKey) {
        e.preventDefault()
        undo()
      } else if (e.key === 'Delete' && !isInput && !mod) {
        onDeleteSelected?.()
      }
    }
    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [connectors, events, streams, onDeleteSelected])

  return (
    <>
      <div className="h-10 border-b border-border bg-card flex items-center px-3 gap-1 relative">
        <div className="flex items-center gap-1.5 mr-2">
          <div className="h-5 w-5 rounded bg-primary flex items-center justify-center">
            <span className="text-primary-foreground text-[10px] font-bold">V</span>
          </div>
          <span className="text-sm font-semibold tracking-tight">Stream Builder</span>
        </div>

        <Separator orientation="vertical" className="h-5" />

        <Button variant="ghost" size="sm" className="h-7 text-xs gap-1" onClick={() => addStream()}>
          <Plus className="h-3.5 w-3.5" />
          Stream
        </Button>

        <Button
          variant="ghost" size="sm" className="h-7 text-xs gap-1"
          onClick={() => { clear(); loadPipeline(DEMO_PIPELINE) }}
        >
          <Sparkles className="h-3.5 w-3.5" />
          Demo
        </Button>

        <Separator orientation="vertical" className="h-5" />

        <Button
          variant="ghost" size="sm" className="h-7 text-xs gap-1"
          onClick={handleValidate}
          disabled={validating}
        >
          {validating ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Check className="h-3.5 w-3.5" />}
          Validate
        </Button>

        <Button
          variant="ghost" size="sm" className="h-7 text-xs gap-1"
          onClick={handleDeployClick}
          disabled={deploying}
          title="Deploy pipeline (Ctrl+S)"
        >
          {deploying ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Play className="h-3.5 w-3.5" />}
          Deploy
        </Button>

        <Button
          variant="ghost" size="sm" className="h-7 text-xs gap-1"
          onClick={handleLoadClick}
        >
          <FolderOpen className="h-3.5 w-3.5" />
          Load
        </Button>

        <Separator orientation="vertical" className="h-5" />

        <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => setShowSettings(true)} title="Server settings">
          <Settings className="h-3.5 w-3.5" />
        </Button>

        <Button variant="ghost" size="icon" className="h-7 w-7" onClick={toggleTheme} title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`}>
          {theme === 'dark' ? <Sun className="h-3.5 w-3.5" /> : <Moon className="h-3.5 w-3.5" />}
        </Button>

        <div className="flex-1" />

        <Button variant="ghost" size="icon" className="h-7 w-7" title="Undo (Ctrl+Z)" onClick={undo} disabled={!canUndo}>
          <Undo2 className="h-3.5 w-3.5" />
        </Button>
        <Button variant="ghost" size="icon" className="h-7 w-7" title="Redo (Ctrl+Shift+Z)" onClick={redo} disabled={!canRedo}>
          <Redo2 className="h-3.5 w-3.5" />
        </Button>

        <Separator orientation="vertical" className="h-5" />

        <span className="text-[10px] text-muted-foreground tabular-nums">
          {streams.length} stream{streams.length !== 1 ? 's' : ''}
        </span>

        <Button
          variant="ghost" size="icon"
          className="h-7 w-7 text-muted-foreground hover:text-destructive"
          onClick={clear} title="Clear pipeline"
        >
          <Trash2 className="h-3.5 w-3.5" />
        </Button>

        {/* Toast notification */}
        {toast && (
          <div className={`absolute top-12 right-4 z-50 flex items-center gap-2 px-3 py-2 rounded-lg shadow-lg text-xs font-medium border ${
            toast.type === 'success' ? 'bg-green-500/10 text-green-600 border-green-500/20' :
            toast.type === 'error' ? 'bg-red-500/10 text-red-600 border-red-500/20' :
            'bg-blue-500/10 text-blue-600 border-blue-500/20'
          }`}>
            {toast.type === 'success' ? <CheckCircle2 className="h-3.5 w-3.5" /> :
             toast.type === 'error' ? <AlertCircle className="h-3.5 w-3.5" /> : null}
            {toast.message}
            <Button variant="ghost" size="icon" className="h-4 w-4 ml-1" onClick={() => setToast(null)}>
              <X className="h-3 w-3" />
            </Button>
          </div>
        )}
      </div>

      {/* Server settings dialog */}
      <Dialog open={showSettings} onOpenChange={setShowSettings}>
        <DialogContent className="max-w-sm">
          <DialogHeader>
            <DialogTitle className="text-sm">Varpulis Server</DialogTitle>
          </DialogHeader>
          <div className="space-y-3">
            <div>
              <label className="text-xs font-medium text-muted-foreground mb-1 block">Server URL</label>
              <Input className="h-8 text-xs font-mono" value={serverUrl} onChange={(e) => setServerUrl(e.target.value)} placeholder="http://localhost:19000" />
            </div>
            <div>
              <label className="text-xs font-medium text-muted-foreground mb-1 block">API Key</label>
              <Input className="h-8 text-xs font-mono" value={apiKey} onChange={(e) => setApiKey(e.target.value)} placeholder="your-api-key" type="password" />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" size="sm" className="text-xs" onClick={() => setShowSettings(false)}>Cancel</Button>
            <Button size="sm" className="text-xs" onClick={handleSaveSettings}>Save & Test</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Deploy dialog */}
      <Dialog open={showDeployDialog} onOpenChange={setShowDeployDialog}>
        <DialogContent className="max-w-sm">
          <DialogHeader>
            <DialogTitle className="text-sm">Deploy Pipeline</DialogTitle>
          </DialogHeader>
          <div>
            <label className="text-xs font-medium text-muted-foreground mb-1 block">Pipeline Name</label>
            <Input className="h-8 text-sm font-mono" value={deployName} onChange={(e) => setDeployName(e.target.value)} placeholder="my-pipeline" autoFocus />
          </div>
          <DialogFooter>
            <Button variant="outline" size="sm" className="text-xs" onClick={() => setShowDeployDialog(false)}>Cancel</Button>
            <Button size="sm" className="text-xs gap-1" onClick={handleDeploy} disabled={!deployName.trim()}>
              <Play className="h-3 w-3" />
              Deploy
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Load pipeline dialog */}
      <Dialog open={showLoadDialog} onOpenChange={setShowLoadDialog}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle className="text-sm">Load Pipeline</DialogTitle>
          </DialogHeader>
          <div className="min-h-[120px] max-h-[300px] overflow-y-auto">
            {loadingPipelines ? (
              <div className="flex items-center justify-center py-8 text-muted-foreground gap-2 text-sm">
                <Loader2 className="h-4 w-4 animate-spin" />
                Loading pipelines...
              </div>
            ) : pipelineList.length === 0 ? (
              <div className="flex items-center justify-center py-8 text-muted-foreground text-sm">
                No pipelines found on server
              </div>
            ) : (
              <div className="space-y-1">
                {pipelineList.map((p) => (
                  <button
                    key={p.id}
                    className="w-full flex items-center gap-3 px-3 py-2 rounded-md hover:bg-accent text-left transition-colors"
                    onClick={() => handleLoadPipeline(p)}
                  >
                    <div className="flex-1 min-w-0">
                      <div className="text-sm font-medium truncate">{p.name}</div>
                      <div className="text-[10px] text-muted-foreground font-mono">{p.id.slice(0, 12)}...</div>
                    </div>
                    <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium ${
                      p.status === 'running' ? 'bg-green-500/10 text-green-600' :
                      p.status === 'stopped' ? 'bg-amber-500/10 text-amber-600' :
                      'bg-muted text-muted-foreground'
                    }`}>
                      {p.status}
                    </span>
                  </button>
                ))}
              </div>
            )}
          </div>
          <DialogFooter>
            <Button variant="outline" size="sm" className="text-xs" onClick={() => setShowLoadDialog(false)}>Cancel</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  )
}
