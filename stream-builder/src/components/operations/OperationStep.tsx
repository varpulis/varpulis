import { X } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { usePipelineStore } from '@/stores/pipelineStore'
import type { StreamOperation } from '@/types/pipeline'
import { OPERATION_CATALOG, CATEGORY_COLORS, type OperationCategory } from '@/types/pipeline'
import { WhereEditor } from './editors/WhereEditor'
import { AggregateEditor } from './editors/AggregateEditor'
import { WindowEditor } from './editors/WindowEditor'
import { EmitEditor } from './editors/EmitEditor'
import { ExpressionInput } from './editors/ExpressionInput'

interface OperationStepProps {
  operation: StreamOperation
  streamId: string
}

export function OperationStep({ operation, streamId }: OperationStepProps) {
  const { updateOperation, removeOperation } = usePipelineStore()
  const meta = OPERATION_CATALOG.find((m) => m.name === operation.type)
  const category = meta?.category ?? 'io'
  const color = CATEGORY_COLORS[category as OperationCategory] ?? '#6b7280'

  const handleValueChange = (value: string) => {
    updateOperation(streamId, operation.id, { value })
  }

  const renderEditor = () => {
    switch (operation.type) {
      case 'where':
      case 'filter':
      case 'having':
        return (
          <WhereEditor
            value={operation.value}
            onChange={handleValueChange}
            streamId={streamId}
          />
        )
      case 'aggregate':
        return (
          <AggregateEditor
            value={operation.value}
            onChange={handleValueChange}
            streamId={streamId}
          />
        )
      case 'window':
        return (
          <WindowEditor
            value={operation.value}
            onChange={handleValueChange}
          />
        )
      case 'emit':
        return (
          <EmitEditor
            value={operation.value}
            onChange={handleValueChange}
            streamId={streamId}
          />
        )
      default:
        // Use ExpressionInput with autocomplete for remaining operations
        return (
          <ExpressionInput
            value={operation.value}
            onChange={handleValueChange}
            placeholder={meta?.description ?? `${operation.type} expression`}
            streamId={streamId}
            showOperators={false}
          />
        )
    }
  }

  return (
    <div className="group relative">
      <div
        className="flex items-center gap-1.5 px-2 py-1 rounded-md hover:bg-accent/50 cursor-pointer text-xs"
        onClick={() => updateOperation(streamId, operation.id, { expanded: !operation.expanded })}
      >
        <div
          className="w-1 h-4 rounded-full flex-shrink-0"
          style={{ backgroundColor: color }}
        />
        <span className="font-mono font-medium text-muted-foreground" style={{ color }}>
          .{operation.type}
        </span>
        <span className="flex-1 truncate text-foreground/70 font-mono">
          {operation.value ? `(${operation.value})` : '()'}
        </span>
        <Button
          variant="ghost"
          size="icon"
          className="h-4 w-4 opacity-0 group-hover:opacity-100 text-muted-foreground hover:text-destructive"
          onClick={(e) => { e.stopPropagation(); removeOperation(streamId, operation.id) }}
        >
          <X className="h-2.5 w-2.5" />
        </Button>
      </div>
      {operation.expanded && (
        <div
          className="px-2 py-1.5 ml-3 border-l-2 border-border"
          onClick={(e) => e.stopPropagation()}
        >
          {renderEditor()}
        </div>
      )}
    </div>
  )
}
