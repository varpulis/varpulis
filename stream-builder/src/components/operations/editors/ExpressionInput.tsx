import { useState, useRef, useEffect, useCallback } from 'react'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { usePipelineStore } from '@/stores/pipelineStore'

const COMPARISON_OPS = ['>', '<', '>=', '<=', '==', '!=', 'AND', 'OR', 'NOT', 'IN', 'LIKE']

interface ExpressionInputProps {
  value: string
  onChange: (value: string) => void
  placeholder?: string
  streamId?: string
  showOperators?: boolean
  className?: string
}

function useEventFields(streamId?: string): string[] {
  const { events, streams } = usePipelineStore()

  if (streamId) {
    const stream = streams.find((s) => s.id === streamId)
    if (stream?.source) {
      const evt = events.find((e) => e.name === stream.source)
      if (evt && evt.fields.length > 0) {
        return evt.fields.map((f) => f.name)
      }
    }
  }

  const fieldSet = new Set<string>()
  for (const evt of events) {
    for (const f of evt.fields) {
      fieldSet.add(f.name)
    }
  }
  return Array.from(fieldSet).sort()
}

export function ExpressionInput({
  value,
  onChange,
  placeholder = 'Expression...',
  streamId,
  showOperators = true,
  className,
}: ExpressionInputProps) {
  const fields = useEventFields(streamId)
  const [open, setOpen] = useState(false)
  const [selectedIdx, setSelectedIdx] = useState(0)
  const inputRef = useRef<HTMLInputElement>(null)

  const getCurrentToken = useCallback((): string => {
    const cursorPos = inputRef.current?.selectionStart ?? value.length
    const beforeCursor = value.slice(0, cursorPos)
    const match = beforeCursor.match(/[\w.]*$/)
    return match ? match[0] : ''
  }, [value])

  const token = getCurrentToken()

  const suggestions = (() => {
    const items: { label: string; kind: 'field' | 'operator' }[] = []
    for (const f of fields) {
      items.push({ label: f, kind: 'field' })
    }
    if (showOperators) {
      for (const op of COMPARISON_OPS) {
        items.push({ label: op, kind: 'operator' })
      }
    }
    if (!token) return items.slice(0, 12)
    const lower = token.toLowerCase()
    return items.filter((s) => s.label.toLowerCase().startsWith(lower)).slice(0, 8)
  })()

  useEffect(() => {
    setSelectedIdx(0)
  }, [token])

  const insertSuggestion = (suggestion: string) => {
    const cursorPos = inputRef.current?.selectionStart ?? value.length
    const beforeCursor = value.slice(0, cursorPos)
    const afterCursor = value.slice(cursorPos)
    const prefix = beforeCursor.replace(/[\w.]*$/, '')
    const needsSpace = suggestion.length > 2
    const newValue = prefix + suggestion + (needsSpace ? ' ' : '') + afterCursor
    onChange(newValue.trimEnd())
    setOpen(false)
    requestAnimationFrame(() => inputRef.current?.focus())
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (!open || suggestions.length === 0) return

    if (e.key === 'ArrowDown') {
      e.preventDefault()
      setSelectedIdx((i) => Math.min(i + 1, suggestions.length - 1))
    } else if (e.key === 'ArrowUp') {
      e.preventDefault()
      setSelectedIdx((i) => Math.max(i - 1, 0))
    } else if (e.key === 'Enter' || e.key === 'Tab') {
      if (suggestions[selectedIdx]) {
        e.preventDefault()
        insertSuggestion(suggestions[selectedIdx].label)
      }
    } else if (e.key === 'Escape') {
      setOpen(false)
    }
  }

  return (
    <div className="relative">
      <Input
        ref={inputRef}
        className={`h-7 text-xs font-mono ${className ?? ''}`}
        placeholder={placeholder}
        value={value}
        onChange={(e) => { onChange(e.target.value); setOpen(true) }}
        onFocus={() => setOpen(true)}
        onBlur={() => {
          setTimeout(() => setOpen(false), 150)
        }}
        onKeyDown={handleKeyDown}
      />
      {open && suggestions.length > 0 && (
        <div className="absolute z-50 top-full left-0 right-0 mt-0.5 bg-popover border border-border rounded-md shadow-md max-h-40 overflow-y-auto">
          {suggestions.map((s, i) => (
            <div
              key={`${s.kind}-${s.label}`}
              className={`flex items-center gap-1.5 px-2 py-1 text-xs cursor-pointer ${
                i === selectedIdx ? 'bg-accent text-accent-foreground' : 'hover:bg-accent/50'
              }`}
              onMouseDown={(e) => { e.preventDefault(); insertSuggestion(s.label) }}
              onMouseEnter={() => setSelectedIdx(i)}
            >
              <Badge
                variant="outline"
                className={`text-[9px] px-1 py-0 h-4 ${
                  s.kind === 'field' ? 'text-blue-500 border-blue-500/30' : 'text-amber-500 border-amber-500/30'
                }`}
              >
                {s.kind === 'field' ? 'field' : 'op'}
              </Badge>
              <span className="font-mono">{s.label}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
