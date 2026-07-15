import { Plus, Trash2 } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { usePipelineStore } from '@/stores/pipelineStore'

const AGG_FUNCTIONS = ['avg', 'sum', 'count', 'min', 'max', 'first', 'last'] as const

interface AggRow {
  func: string
  field: string
  alias: string
}

interface AggregateEditorProps {
  value: string
  onChange: (value: string) => void
  streamId: string
}

/** Parse `avg(temperature) as avg_temp, count(*) as total` into rows */
function parseAggValue(val: string): AggRow[] {
  if (!val.trim()) return [{ func: 'avg', field: '', alias: '' }]

  const parts = val.split(',').map((s) => s.trim()).filter(Boolean)
  const rows: AggRow[] = []
  for (const part of parts) {
    const m = part.match(/^(\w+)\(([^)]*)\)(?:\s+as\s+(\w+))?$/i)
    if (m) {
      rows.push({ func: m[1].toLowerCase(), field: m[2], alias: m[3] ?? '' })
    } else {
      rows.push({ func: 'avg', field: part, alias: '' })
    }
  }
  return rows.length > 0 ? rows : [{ func: 'avg', field: '', alias: '' }]
}

function serializeAggRows(rows: AggRow[]): string {
  return rows
    .filter((r) => r.field.trim())
    .map((r) => {
      const base = `${r.func}(${r.field})`
      return r.alias ? `${base} as ${r.alias}` : base
    })
    .join(', ')
}

export function AggregateEditor({ value, onChange, streamId }: AggregateEditorProps) {
  const { events, streams } = usePipelineStore()
  const rows = parseAggValue(value)

  const stream = streams.find((s) => s.id === streamId)
  const evt = stream?.source ? events.find((e) => e.name === stream.source) : undefined
  const fields = evt?.fields.map((f) => f.name) ?? []

  const updateRow = (idx: number, patch: Partial<AggRow>) => {
    const updated = rows.map((r, i) => (i === idx ? { ...r, ...patch } : r))
    onChange(serializeAggRows(updated))
  }

  const addRow = () => {
    const updated = [...rows, { func: 'avg', field: '', alias: '' }]
    onChange(serializeAggRows(updated))
  }

  const removeRow = (idx: number) => {
    if (rows.length <= 1) return
    const updated = rows.filter((_, i) => i !== idx)
    onChange(serializeAggRows(updated))
  }

  return (
    <div className="space-y-1.5">
      <div className="flex items-center justify-between">
        <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Aggregations</span>
        <Button variant="ghost" size="sm" className="h-5 text-[10px] gap-0.5 px-1" onClick={addRow}>
          <Plus className="h-3 w-3" />
          Add
        </Button>
      </div>

      <div className="space-y-1">
        {rows.map((row, idx) => (
          <div key={idx} className="flex items-center gap-1 group">
            {/* Function picker */}
            <div className="flex gap-0.5 flex-shrink-0 flex-wrap">
              {AGG_FUNCTIONS.map((fn) => (
                <Badge
                  key={fn}
                  variant={row.func === fn ? 'default' : 'outline'}
                  className="cursor-pointer text-[8px] px-0.5 py-0 h-4 font-mono"
                  onClick={() => updateRow(idx, { func: fn })}
                >
                  {fn}
                </Badge>
              ))}
            </div>

            {/* Field input */}
            <div className="flex-1 min-w-0">
              {fields.length > 0 ? (
                <select
                  className="w-full h-7 text-xs font-mono bg-background border border-input rounded-md px-1"
                  value={row.field}
                  onChange={(e) => updateRow(idx, { field: e.target.value })}
                >
                  <option value="">field...</option>
                  <option value="*">*</option>
                  {fields.map((f) => (
                    <option key={f} value={f}>{f}</option>
                  ))}
                </select>
              ) : (
                <Input
                  className="h-7 text-xs font-mono"
                  placeholder="field or *"
                  value={row.field}
                  onChange={(e) => updateRow(idx, { field: e.target.value })}
                />
              )}
            </div>

            {/* Alias */}
            <span className="text-[10px] text-muted-foreground flex-shrink-0">as</span>
            <Input
              className="h-7 text-xs font-mono w-20 flex-shrink-0"
              placeholder="alias"
              value={row.alias}
              onChange={(e) => updateRow(idx, { alias: e.target.value })}
            />

            {/* Delete */}
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
