import { Database, Radio, Layers, Plus, ChevronDown, ChevronRight, Settings } from 'lucide-react'
import { useState } from 'react'
import { Button } from '@/components/ui/button'
import { ScrollArea } from '@/components/ui/scroll-area'
import { usePipelineStore } from '@/stores/pipelineStore'
import { ConnectorDialog } from '@/components/dialogs/ConnectorDialog'
import { EventTypeDialog } from '@/components/dialogs/EventTypeDialog'

interface SidebarProps {
  selectedStreamId?: string
  onSelectStream?: (id: string) => void
}

function SidebarSection({ title, icon: Icon, children, onAdd, defaultOpen = true }: {
  title: string
  icon: React.ElementType
  children: React.ReactNode
  onAdd?: () => void
  defaultOpen?: boolean
}) {
  const [open, setOpen] = useState(defaultOpen)
  return (
    <div className="mb-0.5">
      <div
        className="flex items-center gap-1.5 px-3 py-1.5 text-[10px] font-semibold text-muted-foreground uppercase tracking-wider cursor-pointer hover:text-foreground transition-colors"
        onClick={() => setOpen(!open)}
      >
        {open ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
        <Icon className="h-3 w-3" />
        <span className="flex-1">{title}</span>
        {onAdd && (
          <Button
            variant="ghost"
            size="icon"
            className="h-5 w-5 hover:bg-accent"
            onClick={(e) => { e.stopPropagation(); onAdd() }}
          >
            <Plus className="h-3 w-3" />
          </Button>
        )}
      </div>
      {open && <div className="px-1.5 space-y-0.5">{children}</div>}
    </div>
  )
}

function SidebarItem({ name, active, badge, onClick, onEdit }: {
  name: string
  active?: boolean
  badge?: string
  onClick?: () => void
  onEdit?: () => void
}) {
  return (
    <div
      className={`flex items-center gap-1.5 px-2.5 py-1 text-xs rounded-md cursor-pointer truncate transition-colors group ${
        active
          ? 'bg-primary/10 text-primary font-medium'
          : 'text-muted-foreground hover:bg-accent/50 hover:text-foreground'
      }`}
      onClick={onClick}
    >
      <span className="flex-1 truncate">{name}</span>
      {badge && (
        <span className="text-[9px] text-muted-foreground/60 flex-shrink-0">{badge}</span>
      )}
      {onEdit && (
        <Button
          variant="ghost"
          size="icon"
          className="h-4 w-4 opacity-0 group-hover:opacity-100"
          onClick={(e) => { e.stopPropagation(); onEdit() }}
        >
          <Settings className="h-2.5 w-2.5" />
        </Button>
      )}
    </div>
  )
}

export function Sidebar({ selectedStreamId, onSelectStream }: SidebarProps) {
  const { streams, connectors, events, addStream } = usePipelineStore()
  const [connectorDialog, setConnectorDialog] = useState<{ open: boolean; id?: string }>({ open: false })
  const [eventDialog, setEventDialog] = useState<{ open: boolean; id?: string }>({ open: false })

  return (
    <div className="w-48 border-r border-border bg-card flex flex-col h-full">
      <div className="px-3 py-2 border-b border-border">
        <h2 className="text-xs font-semibold tracking-tight text-foreground">Pipeline</h2>
      </div>
      <ScrollArea className="flex-1">
        <div className="py-1">
          <SidebarSection title="Streams" icon={Layers} onAdd={() => addStream()}>
            {streams.length === 0 && (
              <div className="px-2.5 py-1.5 text-[10px] text-muted-foreground/50 italic">
                Click + to add a stream
              </div>
            )}
            {streams.map((s) => (
              <SidebarItem
                key={s.id}
                name={s.name}
                active={s.id === selectedStreamId}
                badge={s.operations.length > 0 ? `${s.operations.length}` : undefined}
                onClick={() => onSelectStream?.(s.id)}
              />
            ))}
          </SidebarSection>

          <SidebarSection
            title="Connectors"
            icon={Radio}
            onAdd={() => setConnectorDialog({ open: true })}
          >
            {connectors.length === 0 && (
              <div className="px-2.5 py-1.5 text-[10px] text-muted-foreground/50 italic">
                No connectors
              </div>
            )}
            {connectors.map((c) => (
              <SidebarItem
                key={c.id}
                name={c.name}
                badge={c.type}
                onEdit={() => setConnectorDialog({ open: true, id: c.id })}
                onClick={() => setConnectorDialog({ open: true, id: c.id })}
              />
            ))}
          </SidebarSection>

          <SidebarSection
            title="Event Types"
            icon={Database}
            onAdd={() => setEventDialog({ open: true })}
          >
            {events.length === 0 && (
              <div className="px-2.5 py-1.5 text-[10px] text-muted-foreground/50 italic">
                No event types
              </div>
            )}
            {events.map((e) => (
              <SidebarItem
                key={e.id}
                name={e.name}
                badge={`${e.fields.length}`}
                onEdit={() => setEventDialog({ open: true, id: e.id })}
                onClick={() => setEventDialog({ open: true, id: e.id })}
              />
            ))}
          </SidebarSection>
        </div>
      </ScrollArea>

      <ConnectorDialog
        open={connectorDialog.open}
        onOpenChange={(open) => setConnectorDialog({ open, id: connectorDialog.id })}
        connectorId={connectorDialog.id}
      />
      <EventTypeDialog
        open={eventDialog.open}
        onOpenChange={(open) => setEventDialog({ open, id: eventDialog.id })}
        eventId={eventDialog.id}
      />
    </div>
  )
}
