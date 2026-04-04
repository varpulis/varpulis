import { ScrollArea } from '@/components/ui/scroll-area'
import { usePipelineStore } from '@/stores/pipelineStore'
import { StreamCard } from './StreamCard'

interface PipelineCanvasProps {
  selectedStreamId?: string
  onSelectStream?: (id: string) => void
}

export function PipelineCanvas({ selectedStreamId, onSelectStream }: PipelineCanvasProps) {
  const { streams, addStream } = usePipelineStore()

  if (streams.length === 0) {
    return (
      <div className="flex-1 flex items-center justify-center bg-muted/20">
        <div className="text-center space-y-3">
          <div className="text-4xl opacity-20">{ }</div>
          <p className="text-muted-foreground text-sm font-medium">No streams yet</p>
          <p className="text-muted-foreground/60 text-xs max-w-xs">
            Click <strong>+ Stream</strong> in the toolbar to create your first stream,
            then add operations like <code>.where()</code>, <code>.aggregate()</code>, and <code>.emit()</code>
          </p>
          <button
            className="mt-2 text-xs text-primary hover:underline"
            onClick={() => addStream('Telemetry')}
          >
            or click here to start with a demo stream
          </button>
        </div>
      </div>
    )
  }

  return (
    <ScrollArea className="flex-1 bg-muted/20">
      <div className="p-4 flex flex-wrap gap-3 content-start min-h-full items-start">
        {streams.map((stream) => (
          <StreamCard
            key={stream.id}
            stream={stream}
            selected={stream.id === selectedStreamId}
            onSelect={() => onSelectStream?.(stream.id)}
          />
        ))}
      </div>
    </ScrollArea>
  )
}
