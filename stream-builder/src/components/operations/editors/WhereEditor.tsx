import { useState } from 'react'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
import { ExpressionInput } from './ExpressionInput'
import { usePipelineStore } from '@/stores/pipelineStore'
import { Code, LayoutList } from 'lucide-react'

const OPERATORS = ['>', '<', '>=', '<=', '==', '!=', 'LIKE', 'IN'] as const

interface WhereEditorProps {
  value: string
  onChange: (value: string) => void
  streamId: string
}

interface StructuredFilter {
  field: string
  operator: string
  rhs: string
}

/** Parse simple expressions like `temperature > 100` into structured form */
function parseSimpleExpr(val: string): StructuredFilter | null {
  const trimmed = val.trim()
  if (!trimmed) return { field: '', operator: '>', rhs: '' }

  for (const op of OPERATORS) {
    const idx = trimmed.indexOf(` ${op} `)
    if (idx !== -1) {
      return {
        field: trimmed.slice(0, idx).trim(),
        operator: op,
        rhs: trimmed.slice(idx + op.length + 2).trim(),
      }
    }
  }
  return null
}

function serializeFilter(f: StructuredFilter): string {
  if (!f.field && !f.rhs) return ''
  return `${f.field} ${f.operator} ${f.rhs}`.trim()
}

export function WhereEditor({ value, onChange, streamId }: WhereEditorProps) {
  const parsed = parseSimpleExpr(value)
  const [mode, setMode] = useState<'expression' | 'builder'>(parsed ? 'builder' : 'expression')
  const { events, streams } = usePipelineStore()

  const stream = streams.find((s) => s.id === streamId)
  const evt = stream?.source ? events.find((e) => e.name === stream.source) : undefined
  const fields = evt?.fields.map((f) => f.name) ?? []

  if (mode === 'expression') {
    return (
      <div className="space-y-1">
        <div className="flex items-center justify-between">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Expression</span>
          <Button
            variant="ghost"
            size="icon"
            className="h-5 w-5"
            title="Switch to builder"
            onClick={() => { if (parseSimpleExpr(value)) setMode('builder') }}
          >
            <LayoutList className="h-3 w-3" />
          </Button>
        </div>
        <ExpressionInput
          value={value}
          onChange={onChange}
          placeholder={'e.g. temperature > 100 AND status == "active"'}
          streamId={streamId}
        />
      </div>
    )
  }

  const filter = parsed ?? { field: '', operator: '>', rhs: '' }

  const updateFilter = (patch: Partial<StructuredFilter>) => {
    const updated = { ...filter, ...patch }
    onChange(serializeFilter(updated))
  }

  return (
    <div className="space-y-1.5">
      <div className="flex items-center justify-between">
        <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Condition</span>
        <Button
          variant="ghost"
          size="icon"
          className="h-5 w-5"
          title="Switch to expression"
          onClick={() => setMode('expression')}
        >
          <Code className="h-3 w-3" />
        </Button>
      </div>

      <div className="flex items-center gap-1">
        {/* Field selector */}
        <div className="flex-1">
          {fields.length > 0 ? (
            <select
              className="w-full h-7 text-xs font-mono bg-background border border-input rounded-md px-1.5"
              value={filter.field}
              onChange={(e) => updateFilter({ field: e.target.value })}
            >
              <option value="">field...</option>
              {fields.map((f) => (
                <option key={f} value={f}>{f}</option>
              ))}
            </select>
          ) : (
            <Input
              className="h-7 text-xs font-mono"
              placeholder="field"
              value={filter.field}
              onChange={(e) => updateFilter({ field: e.target.value })}
            />
          )}
        </div>

        {/* Operator picker */}
        <div className="flex gap-0.5 flex-shrink-0">
          {OPERATORS.slice(0, 6).map((op) => (
            <Badge
              key={op}
              variant={filter.operator === op ? 'default' : 'outline'}
              className="cursor-pointer text-[9px] px-1 py-0 h-5 font-mono"
              onClick={() => updateFilter({ operator: op })}
            >
              {op}
            </Badge>
          ))}
        </div>

        {/* Value input */}
        <div className="w-20 flex-shrink-0">
          <Input
            className="h-7 text-xs font-mono"
            placeholder="value"
            value={filter.rhs}
            onChange={(e) => updateFilter({ rhs: e.target.value })}
          />
        </div>
      </div>

      {/* Extra operators on second row */}
      <div className="flex gap-0.5">
        {OPERATORS.slice(6).map((op) => (
          <Badge
            key={op}
            variant={filter.operator === op ? 'default' : 'outline'}
            className="cursor-pointer text-[9px] px-1 py-0 h-5 font-mono"
            onClick={() => updateFilter({ operator: op })}
          >
            {op}
          </Badge>
        ))}
      </div>
    </div>
  )
}
