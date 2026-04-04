import { useState, useEffect } from 'react'
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { Trash2 } from 'lucide-react'
import { usePipelineStore } from '@/stores/pipelineStore'

const CONNECTOR_TYPES = [
  { type: 'mqtt', label: 'MQTT', fields: ['host', 'port', 'client_id', 'username', 'password', 'use_tls'] },
  { type: 'kafka', label: 'Kafka', fields: ['brokers', 'topic', 'group_id', 'security_protocol'] },
  { type: 'http', label: 'HTTP', fields: ['url', 'method', 'api_key'] },
  { type: 'nats', label: 'NATS', fields: ['url', 'subject'] },
  { type: 'redis', label: 'Redis', fields: ['url', 'channel'] },
  { type: 'database', label: 'Database', fields: ['url', 'table'] },
]

interface ConnectorDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  connectorId?: string  // null for new connector
}

export function ConnectorDialog({ open, onOpenChange, connectorId }: ConnectorDialogProps) {
  const { connectors, addConnector, updateConnector, removeConnector } = usePipelineStore()
  const existing = connectors.find((c) => c.id === connectorId)

  const [name, setName] = useState(existing?.name ?? '')
  const [type, setType] = useState(existing?.type ?? 'mqtt')
  const [config, setConfig] = useState<Record<string, string>>(existing?.config ?? {})

  useEffect(() => {
    if (existing) {
      setName(existing.name)
      setType(existing.type)
      setConfig({ ...existing.config })
    } else {
      setName('')
      setType('mqtt')
      setConfig({})
    }
  }, [existing, open])

  const typeInfo = CONNECTOR_TYPES.find((t) => t.type === type)

  const handleSave = () => {
    if (!name.trim()) return
    if (connectorId && existing) {
      updateConnector(connectorId, { name: name.trim(), type, config })
    } else {
      addConnector(name.trim(), type)
      // Update config after creation
      const newConn = usePipelineStore.getState().connectors.slice(-1)[0]
      if (newConn) {
        updateConnector(newConn.id, { config })
      }
    }
    onOpenChange(false)
  }

  const handleDelete = () => {
    if (connectorId) {
      removeConnector(connectorId)
      onOpenChange(false)
    }
  }

  const updateConfig = (key: string, value: string) => {
    setConfig((prev) => ({ ...prev, [key]: value }))
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle className="text-sm">
            {connectorId ? 'Edit Connector' : 'New Connector'}
          </DialogTitle>
        </DialogHeader>

        <div className="space-y-3">
          {/* Name */}
          <div>
            <label className="text-xs font-medium text-muted-foreground mb-1 block">Name</label>
            <Input
              className="h-8 text-sm"
              placeholder="MqttBroker"
              value={name}
              onChange={(e) => setName(e.target.value)}
              autoFocus
            />
          </div>

          {/* Type selector */}
          <div>
            <label className="text-xs font-medium text-muted-foreground mb-1 block">Type</label>
            <div className="flex flex-wrap gap-1.5">
              {CONNECTOR_TYPES.map((t) => (
                <Badge
                  key={t.type}
                  variant={type === t.type ? 'default' : 'outline'}
                  className="cursor-pointer text-xs"
                  onClick={() => setType(t.type)}
                >
                  {t.label}
                </Badge>
              ))}
            </div>
          </div>

          {/* Config fields */}
          <div>
            <label className="text-xs font-medium text-muted-foreground mb-1 block">Configuration</label>
            <div className="space-y-2">
              {(typeInfo?.fields ?? []).map((field) => (
                <div key={field} className="flex items-center gap-2">
                  <span className="text-xs text-muted-foreground w-28 flex-shrink-0 text-right">{field}:</span>
                  <Input
                    className="h-7 text-xs flex-1 font-mono"
                    placeholder={field === 'port' ? '1883' : field === 'host' ? 'localhost' : ''}
                    value={config[field] ?? ''}
                    onChange={(e) => updateConfig(field, e.target.value)}
                    type={field === 'password' ? 'password' : 'text'}
                  />
                </div>
              ))}
            </div>
          </div>
        </div>

        <DialogFooter className="flex items-center gap-2">
          {connectorId && (
            <Button variant="destructive" size="sm" className="mr-auto text-xs" onClick={handleDelete}>
              <Trash2 className="h-3 w-3 mr-1" />
              Delete
            </Button>
          )}
          <Button variant="outline" size="sm" className="text-xs" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button size="sm" className="text-xs" onClick={handleSave} disabled={!name.trim()}>
            {connectorId ? 'Save' : 'Create'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
