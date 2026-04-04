import { useState, useEffect } from 'react'
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { Trash2, Plus, GripVertical } from 'lucide-react'
import { usePipelineStore } from '@/stores/pipelineStore'
import type { EventField } from '@/types/pipeline'

const FIELD_TYPES = ['str', 'float', 'int', 'bool', 'timestamp', 'duration']

interface EventTypeDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  eventId?: string
}

export function EventTypeDialog({ open, onOpenChange, eventId }: EventTypeDialogProps) {
  const { events, addEvent, updateEvent, removeEvent } = usePipelineStore()
  const existing = events.find((e) => e.id === eventId)

  const [name, setName] = useState(existing?.name ?? '')
  const [extendsType, setExtendsType] = useState(existing?.extends ?? '')
  const [fields, setFields] = useState<EventField[]>(existing?.fields ?? [])

  useEffect(() => {
    if (existing) {
      setName(existing.name)
      setExtendsType(existing.extends ?? '')
      setFields([...existing.fields])
    } else {
      setName('')
      setExtendsType('')
      setFields([])
    }
  }, [existing, open])

  const addField = () => {
    setFields([...fields, { name: '', type: 'str', optional: false }])
  }

  const updateField = (index: number, patch: Partial<EventField>) => {
    setFields(fields.map((f, i) => (i === index ? { ...f, ...patch } : f)))
  }

  const removeField = (index: number) => {
    setFields(fields.filter((_, i) => i !== index))
  }

  const handleSave = () => {
    if (!name.trim()) return
    const validFields = fields.filter((f) => f.name.trim())
    if (eventId && existing) {
      updateEvent(eventId, { name: name.trim(), fields: validFields, extends: extendsType || undefined })
    } else {
      const id = addEvent(name.trim())
      updateEvent(id, { fields: validFields, extends: extendsType || undefined })
    }
    onOpenChange(false)
  }

  const handleDelete = () => {
    if (eventId) {
      removeEvent(eventId)
      onOpenChange(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle className="text-sm">
            {eventId ? 'Edit Event Type' : 'New Event Type'}
          </DialogTitle>
        </DialogHeader>

        <div className="space-y-3">
          {/* Name + extends */}
          <div className="flex gap-2">
            <div className="flex-1">
              <label className="text-xs font-medium text-muted-foreground mb-1 block">Event Name</label>
              <Input
                className="h-8 text-sm font-mono"
                placeholder="SensorReading"
                value={name}
                onChange={(e) => setName(e.target.value)}
                autoFocus
              />
            </div>
            <div className="w-36">
              <label className="text-xs font-medium text-muted-foreground mb-1 block">Extends</label>
              <Input
                className="h-8 text-sm font-mono"
                placeholder="(optional)"
                value={extendsType}
                onChange={(e) => setExtendsType(e.target.value)}
              />
            </div>
          </div>

          {/* Fields */}
          <div>
            <div className="flex items-center justify-between mb-1">
              <label className="text-xs font-medium text-muted-foreground">Fields</label>
              <Button variant="ghost" size="sm" className="h-6 text-xs gap-1" onClick={addField}>
                <Plus className="h-3 w-3" />
                Add Field
              </Button>
            </div>

            {fields.length === 0 && (
              <div className="text-xs text-muted-foreground/50 italic py-2 text-center border border-dashed border-border rounded-md">
                No fields yet — click Add Field
              </div>
            )}

            <div className="space-y-1.5 max-h-60 overflow-y-auto">
              {fields.map((field, idx) => (
                <div key={idx} className="flex items-center gap-1.5 group">
                  <GripVertical className="h-3 w-3 text-muted-foreground/30 flex-shrink-0" />
                  <Input
                    className="h-7 text-xs font-mono flex-1"
                    placeholder="field_name"
                    value={field.name}
                    onChange={(e) => updateField(idx, { name: e.target.value })}
                  />
                  <div className="flex gap-0.5 flex-shrink-0">
                    {FIELD_TYPES.map((t) => (
                      <Badge
                        key={t}
                        variant={field.type === t ? 'default' : 'outline'}
                        className="cursor-pointer text-[9px] px-1 py-0 h-5"
                        onClick={() => updateField(idx, { type: t })}
                      >
                        {t}
                      </Badge>
                    ))}
                  </div>
                  <Badge
                    variant={field.optional ? 'default' : 'outline'}
                    className="cursor-pointer text-[9px] px-1 py-0 h-5 flex-shrink-0"
                    onClick={() => updateField(idx, { optional: !field.optional })}
                  >
                    ?
                  </Badge>
                  <Button
                    variant="ghost"
                    size="icon"
                    className="h-5 w-5 opacity-0 group-hover:opacity-100 text-muted-foreground hover:text-destructive flex-shrink-0"
                    onClick={() => removeField(idx)}
                  >
                    <Trash2 className="h-3 w-3" />
                  </Button>
                </div>
              ))}
            </div>
          </div>
        </div>

        <DialogFooter className="flex items-center gap-2">
          {eventId && (
            <Button variant="destructive" size="sm" className="mr-auto text-xs" onClick={handleDelete}>
              <Trash2 className="h-3 w-3 mr-1" />
              Delete
            </Button>
          )}
          <Button variant="outline" size="sm" className="text-xs" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button size="sm" className="text-xs" onClick={handleSave} disabled={!name.trim()}>
            {eventId ? 'Save' : 'Create'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
