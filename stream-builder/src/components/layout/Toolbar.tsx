import { useState } from 'react'
import { Plus, Play, Check, Undo2, Redo2, Trash2, Sparkles, Loader2, Settings, X, CheckCircle2, AlertCircle } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Separator } from '@/components/ui/separator'
import { Input } from '@/components/ui/input'
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from '@/components/ui/dialog'
import { usePipelineStore } from '@/stores/pipelineStore'
import { DEMO_PIPELINE } from '@/utils/demoPipeline'
import { generateVpl } from '@/utils/vplGenerator'
import { validateVpl, deployPipeline, checkHealth, getServerConfig, setServerConfig } from '@/api/varpulisClient'

type ToastType = 'success' | 'error' | 'info'

export function Toolbar() {
  const { addStream, clear, loadPipeline, streams, connectors, events } = usePipelineStore()
  const [validating, setValidating] = useState(false)
  const [deploying, setDeploying] = useState(false)
  const [toast, setToast] = useState<{ message: string; type: ToastType } | null>(null)
  const [showSettings, setShowSettings] = useState(false)
  const [serverUrl, setServerUrl] = useState(getServerConfig().url)
  const [apiKey, setApiKey] = useState(getServerConfig().apiKey)
  const [deployName, setDeployName] = useState('')
  const [showDeployDialog, setShowDeployDialog] = useState(false)

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
    const result = await validateVpl(vpl)
    setValidating(false)
    if (result.valid) {
      showToast('VPL syntax is valid', 'success')
    } else {
      showToast(`Validation failed: ${result.error}`, 'error')
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
        >
          {deploying ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Play className="h-3.5 w-3.5" />}
          Deploy
        </Button>

        <Separator orientation="vertical" className="h-5" />

        <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => setShowSettings(true)} title="Server settings">
          <Settings className="h-3.5 w-3.5" />
        </Button>

        <div className="flex-1" />

        <Button variant="ghost" size="icon" className="h-7 w-7" title="Undo"><Undo2 className="h-3.5 w-3.5" /></Button>
        <Button variant="ghost" size="icon" className="h-7 w-7" title="Redo"><Redo2 className="h-3.5 w-3.5" /></Button>

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
    </>
  )
}
