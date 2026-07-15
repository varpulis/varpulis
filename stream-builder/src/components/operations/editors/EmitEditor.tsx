import { Plus, Trash2 } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { ExpressionInput } from './ExpressionInput'
import { usePipelineStore } from '@/stores/pipelineStore'

interface EmitRow {
  outputField: string
  expression: string
}

interface EmitEditorProps {
  value: string
  onChange: (value: string) => void
  streamId: string
}

/** Parse `alert_type = "high", device_id = sensor_id` into rows */
function parseEmitValue(val: string): EmitRow[] {
  if (!val.trim()) return [{ outputField: '', expression: '' }]

  const parts = val.split(',').map((s) => s.trim()).filter(Boolean)
  const rows: EmitRow[] = []
  for (const part of parts) {
    const eqIdx = part.indexOf('=')
    if (eqIdx !== -1) {
      rows.push({
        outputField: part.slice(0, eqIdx).trim(),
        expression: part.slice(eqIdx + 1).trim(),
      })
    } else {
      rows.push({ outputField: part, expression: part })
    }
  }
  return rows.length > 0 ? rows : [{ outputField: '', expression: '' }]
}

function serializeEmitRows(rows: EmitRow[]): string {
  return rows
    .filter((r) => r.outputField.trim())
    .map((r) => {
      if (r.expression && r.expression !== r.outputField) {
        return `${r.outputField} = ${r.expression}`
      }
      return r.outputField
    })
    .join(', ')
}

export function EmitEditor({ value, onChange, streamId }: EmitEditorProps) {
  const { events, streams } = usePipelineStore()
  const rows = parseEmitValue(value)

  const stream = streams.find((s) => s.id === streamId)
  const evt = stream?.source ? events.find((e) => e.name === stream.source) : undefined
  const fields = evt?.fields.map((f) => f.name) ?? []

  const updateRow = (idx: number, patch: Partial<EmitRow>) => {
    const updated = rows.map((r, i) => (i === idx ? { ...r, ...patch } : r))
    onChange(serializeEmitRows(updated))
  }

  const addRow = () => {
    const updated = [...rows, { outputField: '', expression: '' }]
    onChange(serializeEmitRows(updated))
  }

  const removeRow = (idx: number) => {
    if (rows.length <= 1) return
    const updated = rows.filter((_, i) => i !== idx)
    onChange(serializeEmitRows(updated))
  }

  const addFieldMapping = (fieldName: string) => {
    if (rows.some((r) => r.outputField === fieldName)) return
    const updated = [
      ...rows.filter((r) => r.outputField.trim()),
      { outputField: fieldName, expression: fieldName },
    ]
    onChange(serializeEmitRows(updated))
  }

  return (
    <div className="space-y-1.5">
      <div className="flex items-center justify-between">
        <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Output Fields</span>
        <Button variant="ghost" size="sm" className="h-5 text-[10px] gap-0.5 px-1" onClick={addRow}>
          <Plus className="h-3 w-3" />
          Add
        </Button>
      </div>

      {/* Quick-add from schema */}
      {fields.length > 0 && (
        <div className="flex gap-0.5 flex-wrap">
          {fields.map((f) => {
            const used = rows.some((r) => r.outputField === f)
            return (
              <button
                key={f}
                className={`text-[9px] font-mono px-1 py-0 h-4 rounded border ${
                  used
                    ? 'bg-primary/10 border-primary/30 text-primary cursor-default'
                    : 'border-border text-muted-foreground hover:bg-accent cursor-pointer'
                }`}
                onClick={() => !used && addFieldMapping(f)}
                disabled={used}
              >
                {f}
              </button>
            )
          })}
        </div>
      )}

      {/* Rows */}
      <div className="space-y-1">
        {rows.map((row, idx) => (
          <div key={idx} className="flex items-center gap-1 group">
            <Input
              className="h-7 text-xs font-mono w-24 flex-shrink-0"
              placeholder="output_name"
              value={row.outputField}
              onChange={(e) => updateRow(idx, { outputField: e.target.value })}
            />
            <span className="text-[10px] text-muted-foreground flex-shrink-0">=</span>
            <div className="flex-1 min-w-0">
              <ExpressionInput
                value={row.expression}
                onChange={(v) => updateRow(idx, { expression: v })}
                placeholder="source expression"
                streamId={streamId}
                showOperators={false}
              />
            </div>
            <Button
              variant="ghost"
              size="icon"
              className="h-5 w-5 opacity-0 group-hover:opacity-100 text-muted-foreground hover:text-destructive flex-shrink-0"
              onClick={() => removeRow(idx)}
              disabled={rows.length <= 1}
            >
              <Trash2 className="h-3 w-3" />
            </Button>
          </div>
        ))}
      </div>
    </div>
  )
}
