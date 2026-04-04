import { X } from 'lucide-react'
import { useState } from 'react'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { OPERATION_CATALOG, CATEGORY_LABELS, CATEGORY_COLORS, type OperationCategory } from '@/types/pipeline'

interface OperationPickerProps {
  onSelect: (type: string) => void
  onClose: () => void
}

export function OperationPicker({ onSelect, onClose }: OperationPickerProps) {
  const [activeCategory, setActiveCategory] = useState<OperationCategory | null>(null)

  const categories = Object.entries(CATEGORY_LABELS) as [OperationCategory, string][]
  const filtered = activeCategory
    ? OPERATION_CATALOG.filter((op) => op.category === activeCategory)
    : OPERATION_CATALOG

  return (
    <div className="border border-border rounded-lg p-2 bg-popover shadow-lg">
      <div className="flex items-center justify-between mb-2">
        <span className="text-xs font-semibold text-muted-foreground">Add Operation</span>
        <Button variant="ghost" size="icon" className="h-4 w-4" onClick={onClose}>
          <X className="h-3 w-3" />
        </Button>
      </div>

      {/* Category tabs */}
      <div className="flex flex-wrap gap-1 mb-2">
        <Badge
          variant={activeCategory === null ? 'default' : 'outline'}
          className="text-[10px] cursor-pointer px-1.5 py-0"
          onClick={() => setActiveCategory(null)}
        >
          All
        </Badge>
        {categories.map(([key, label]) => (
          <Badge
            key={key}
            variant={activeCategory === key ? 'default' : 'outline'}
            className="text-[10px] cursor-pointer px-1.5 py-0"
            style={activeCategory === key ? { backgroundColor: CATEGORY_COLORS[key] } : {}}
            onClick={() => setActiveCategory(key)}
          >
            {label}
          </Badge>
        ))}
      </div>

      {/* Operation list */}
      <div className="max-h-40 overflow-y-auto space-y-0.5">
        {filtered.map((op) => (
          <div
            key={op.name}
            className="flex items-center gap-2 px-2 py-1 rounded-md hover:bg-accent cursor-pointer text-xs"
            onClick={() => onSelect(op.name)}
          >
            <div
              className="w-1.5 h-1.5 rounded-full flex-shrink-0"
              style={{ backgroundColor: CATEGORY_COLORS[op.category] }}
            />
            <span className="font-mono font-medium">{op.label}</span>
            <span className="flex-1 text-muted-foreground truncate">{op.description}</span>
          </div>
        ))}
      </div>
    </div>
  )
}
