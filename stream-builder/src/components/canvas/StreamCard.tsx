import { GripVertical, X, Plus, Pencil } from 'lucide-react'
import { useState, useRef } from 'react'
import { Card, CardContent, CardHeader } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { usePipelineStore } from '@/stores/pipelineStore'
import type { StreamDef } from '@/types/pipeline'
import { OperationStep } from '@/components/operations/OperationStep'
import { OperationPicker } from '@/components/operations/OperationPicker'

interface StreamCardProps {
  stream: StreamDef
  selected?: boolean
  onSelect?: () => void
}

export function StreamCard({ stream, selected, onSelect }: StreamCardProps) {
  const { updateStream, removeStream, addOperation, streams, events } = usePipelineStore()
  const [showPicker, setShowPicker] = useState(false)
  const [editingName, setEditingName] = useState(false)
  const [editingSource, setEditingSource] = useState(false)
  const nameRef = useRef<HTMLInputElement>(null)

  // Available sources: other streams + event types
  const availableSources = [
    ...streams.filter((s) => s.id !== stream.id).map((s) => ({ name: s.name, type: 'stream' as const })),
    ...events.map((e) => ({ name: e.name, type: 'event' as const })),
  ]

  const sourceColor = stream.sourceType === 'merge' ? 'bg-blue-500/10 text-blue-500' :
    stream.sourceType === 'join' ? 'bg-amber-500/10 text-amber-500' :
    stream.sourceType === 'sequence' ? 'bg-red-500/10 text-red-500' :
    'bg-muted text-muted-foreground'

  return (
    <Card
      className={`w-72 shadow-sm transition-all cursor-pointer ${
        selected ? 'ring-2 ring-primary shadow-md' : 'hover:shadow-md'
      }`}
      onClick={onSelect}
    >
      <CardHeader className="p-3 pb-2 space-y-1.5">
        {/* Stream name */}
        <div className="flex items-center gap-1.5">
          <GripVertical className="h-4 w-4 text-muted-foreground/50 flex-shrink-0" />
          {editingName ? (
            <div className="flex-1 flex items-center gap-1">
              <Input
                ref={nameRef}
                className="h-6 text-sm font-semibold px-1 flex-1"
                value={stream.name}
                onChange={(e) => updateStream(stream.id, { name: e.target.value })}
                onBlur={() => setEditingName(false)}
                onKeyDown={(e) => e.key === 'Enter' && setEditingName(false)}
                autoFocus
              />
            </div>
          ) : (
            <div className="flex-1 flex items-center gap-1 min-w-0">
              <span
                className="text-sm font-semibold truncate cursor-text hover:text-primary"
                onClick={(e) => { e.stopPropagation(); setEditingName(true) }}
              >
                {stream.name}
              </span>
              <Pencil className="h-3 w-3 text-muted-foreground/40 flex-shrink-0" />
            </div>
          )}
          <Button
            variant="ghost"
            size="icon"
            className="h-5 w-5 text-muted-foreground/50 hover:text-destructive flex-shrink-0"
            onClick={(e) => { e.stopPropagation(); removeStream(stream.id) }}
          >
            <X className="h-3 w-3" />
          </Button>
        </div>

        {/* Source selector */}
        <div className="flex items-center gap-1.5 pl-5">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">from</span>
          {editingSource ? (
            <div className="flex-1">
              <Input
                className="h-6 text-xs px-1.5"
                placeholder="Type source name..."
                value={stream.source}
                onChange={(e) => updateStream(stream.id, { source: e.target.value })}
                onBlur={() => setEditingSource(false)}
                onKeyDown={(e) => e.key === 'Enter' && setEditingSource(false)}
                list={`sources-${stream.id}`}
                autoFocus
              />
              <datalist id={`sources-${stream.id}`}>
                {availableSources.map((s) => (
                  <option key={s.name} value={s.name} />
                ))}
              </datalist>
            </div>
          ) : (
            <Badge
              variant="outline"
              className={`text-[11px] px-1.5 py-0 h-5 cursor-pointer hover:bg-accent ${sourceColor}`}
              onClick={(e) => { e.stopPropagation(); setEditingSource(true) }}
            >
              {stream.source || '(click to set source)'}
            </Badge>
          )}
        </div>

        {/* Merge/join source refs */}
        {stream.sourceRefs.length > 0 && (
          <div className="flex flex-wrap gap-1 pl-5">
            {stream.sourceRefs.map((ref) => (
              <Badge key={ref} variant="secondary" className="text-[10px] px-1 py-0 h-4">
                {ref}
              </Badge>
            ))}
          </div>
        )}
      </CardHeader>

      <CardContent className="p-2 pt-0">
        {/* Operations */}
        {stream.operations.length > 0 && (
          <div className="space-y-0.5 mb-1">
            {stream.operations.map((op, _index) => (
              <OperationStep
                key={op.id}
                operation={op}
                streamId={stream.id}
              />
            ))}
          </div>
        )}

        {/* Add operation */}
        {showPicker ? (
          <OperationPicker
            onSelect={(type) => {
              addOperation(stream.id, type)
              setShowPicker(false)
            }}
            onClose={() => setShowPicker(false)}
          />
        ) : (
          <Button
            variant="ghost"
            size="sm"
            className="w-full h-7 text-xs text-muted-foreground hover:text-foreground border border-dashed border-border/50 hover:border-border"
            onClick={(e) => { e.stopPropagation(); setShowPicker(true) }}
          >
            <Plus className="h-3 w-3 mr-1" />
            Add Operation
          </Button>
        )}
      </CardContent>
    </Card>
  )
}
