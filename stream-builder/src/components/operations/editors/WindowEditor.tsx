import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'

const WINDOW_TYPES = ['tumbling', 'sliding', 'session'] as const
type WindowType = (typeof WINDOW_TYPES)[number]

const DURATION_PRESETS = ['1s', '5s', '10s', '30s', '1m', '5m', '10m', '30m', '1h'] as const

interface WindowEditorProps {
  value: string
  onChange: (value: string) => void
}

interface WindowConfig {
  type: WindowType
  duration: string
  slide: string
}

/** Parse `tumbling, 5m` or `sliding, 5m, 1m` or `session, 10m` */
function parseWindowValue(val: string): WindowConfig {
  const trimmed = val.trim()
  if (!trimmed) return { type: 'tumbling', duration: '', slide: '' }

  const parts = trimmed.split(',').map((s) => s.trim())

  const rawType = (parts[0] ?? '').toLowerCase()
  const type: WindowType = WINDOW_TYPES.includes(rawType as WindowType)
    ? (rawType as WindowType)
    : 'tumbling'
  const duration = parts[1] ?? ''
  const slide = parts[2] ?? ''

  return { type, duration, slide }
}

function serializeWindow(config: WindowConfig): string {
  const parts: string[] = [config.type]
  if (config.duration) parts.push(config.duration)
  if (config.type === 'sliding' && config.slide) parts.push(config.slide)
  return parts.join(', ')
}

export function WindowEditor({ value, onChange }: WindowEditorProps) {
  const config = parseWindowValue(value)

  const update = (patch: Partial<WindowConfig>) => {
    const updated = { ...config, ...patch }
    if (patch.type && patch.type !== 'sliding') {
      updated.slide = ''
    }
    onChange(serializeWindow(updated))
  }

  return (
    <div className="space-y-2">
      {/* Window type */}
      <div>
        <span className="text-[10px] text-muted-foreground uppercase tracking-wider block mb-1">Type</span>
        <div className="flex gap-1">
          {WINDOW_TYPES.map((wt) => (
            <Badge
              key={wt}
              variant={config.type === wt ? 'default' : 'outline'}
              className="cursor-pointer text-[10px] px-1.5 py-0.5 h-5 font-mono capitalize"
              onClick={() => update({ type: wt })}
            >
              {wt}
            </Badge>
          ))}
        </div>
      </div>

      {/* Duration */}
      <div>
        <span className="text-[10px] text-muted-foreground uppercase tracking-wider block mb-1">Duration</span>
        <div className="flex items-center gap-1">
          <Input
            className="h-7 text-xs font-mono w-20"
            placeholder="e.g. 5m"
            value={config.duration}
            onChange={(e) => update({ duration: e.target.value })}
          />
          <div className="flex gap-0.5 flex-wrap">
            {DURATION_PRESETS.map((d) => (
              <Badge
                key={d}
                variant={config.duration === d ? 'default' : 'outline'}
                className="cursor-pointer text-[8px] px-1 py-0 h-4 font-mono"
                onClick={() => update({ duration: d })}
              >
                {d}
              </Badge>
            ))}
          </div>
        </div>
      </div>

      {/* Slide interval (sliding only) */}
      {config.type === 'sliding' && (
        <div>
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider block mb-1">
            Slide Interval
          </span>
          <div className="flex items-center gap-1">
            <Input
              className="h-7 text-xs font-mono w-20"
              placeholder="e.g. 1m"
              value={config.slide}
              onChange={(e) => update({ slide: e.target.value })}
            />
            <div className="flex gap-0.5 flex-wrap">
              {DURATION_PRESETS.slice(0, 6).map((d) => (
                <Badge
                  key={d}
                  variant={config.slide === d ? 'default' : 'outline'}
                  className="cursor-pointer text-[8px] px-1 py-0 h-4 font-mono"
                  onClick={() => update({ slide: d })}
                >
                  {d}
                </Badge>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Preview */}
      <div className="text-[10px] font-mono text-muted-foreground bg-muted/50 rounded px-2 py-1">
        .window({serializeWindow(config)})
      </div>
    </div>
  )
}
