import { useRef, useCallback, useEffect, useImperativeHandle, forwardRef } from 'react'
import Editor, { type OnMount } from '@monaco-editor/react'
import type { editor } from 'monaco-editor'
import { VPL_LANGUAGE_ID, vplLanguageConfig, vplTokensProvider, vplTheme, vplThemeLight } from './vplLanguage'
import type { VplDiagnostic } from '@/api/varpulisClient'

export interface VplEditorHandle {
  revealLine: (line: number) => void
  setMarkers: (diagnostics: VplDiagnostic[]) => void
  clearMarkers: () => void
}

interface VplEditorProps {
  value: string
  onChange: (value: string) => void
  onCursorLine?: (line: number) => void
  className?: string
  theme?: 'light' | 'dark'
}

const SAMPLE_VPL = `# Varpulis Stream Builder
# Define connectors, events, and streams below

connector Mqtt = mqtt (
    host: "localhost",
    port: 1883,
    client_id: "varpulis-builder"
)

event SensorReading:
    sensor_id: str
    temperature: float
    humidity: float
    ts: timestamp

stream Telemetry = SensorReading
    .from(Mqtt, topic: "sensors/#")

stream HighTemp = Telemetry
    .where(temperature > 85.0)
    .emit(
        sensor: sensor_id,
        temp: temperature,
        severity: "warning"
    )
`

const SEVERITY_MAP: Record<string, number> = {
  error: 8,    // Monaco MarkerSeverity.Error
  warning: 4,  // Monaco MarkerSeverity.Warning
  info: 2,     // Monaco MarkerSeverity.Info
  hint: 1,     // Monaco MarkerSeverity.Hint
}

export const VplEditor = forwardRef<VplEditorHandle, VplEditorProps>(
  function VplEditor({ value, onChange, onCursorLine, className, theme: appTheme }, ref) {
    const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null)
    const monacoRef = useRef<typeof import('monaco-editor') | null>(null)
    const activeTheme = appTheme === 'light' ? 'vpl-light' : 'vpl-dark'

    useImperativeHandle(ref, () => ({
      revealLine(line: number) {
        const ed = editorRef.current
        if (!ed) return
        ed.revealLineInCenter(line)
        ed.setPosition({ lineNumber: line, column: 1 })
      },
      setMarkers(diagnostics: VplDiagnostic[]) {
        const monaco = monacoRef.current
        const ed = editorRef.current
        if (!monaco || !ed) return
        const model = ed.getModel()
        if (!model) return

        const markers = diagnostics.map((d) => ({
          severity: SEVERITY_MAP[d.severity] ?? 8,
          message: d.hint ? `${d.message}\nHint: ${d.hint}` : d.message,
          startLineNumber: Math.max(1, d.start_line),
          startColumn: Math.max(1, d.start_col + 1),
          endLineNumber: Math.max(1, d.end_line || d.start_line),
          endColumn: Math.max(1, (d.end_col || d.start_col) + 1),
          source: 'varpulis',
          code: d.code || undefined,
        }))
        monaco.editor.setModelMarkers(model, 'varpulis', markers)
      },
      clearMarkers() {
        const monaco = monacoRef.current
        const ed = editorRef.current
        if (!monaco || !ed) return
        const model = ed.getModel()
        if (!model) return
        monaco.editor.setModelMarkers(model, 'varpulis', [])
      },
    }))

    const handleMount: OnMount = useCallback((editor, monaco) => {
      editorRef.current = editor
      monacoRef.current = monaco as unknown as typeof import('monaco-editor')

      // Register VPL language
      if (!monaco.languages.getLanguages().some((l: { id: string }) => l.id === VPL_LANGUAGE_ID)) {
        monaco.languages.register({ id: VPL_LANGUAGE_ID })
        monaco.languages.setLanguageConfiguration(VPL_LANGUAGE_ID, vplLanguageConfig)
        monaco.languages.setMonarchTokensProvider(VPL_LANGUAGE_ID, vplTokensProvider)
        monaco.editor.defineTheme('vpl-dark', vplTheme)
        monaco.editor.defineTheme('vpl-light', vplThemeLight)
      }

      monaco.editor.setTheme(activeTheme)

      // Cursor position tracking for bidirectional sync
      if (onCursorLine) {
        editor.onDidChangeCursorPosition((e) => {
          onCursorLine(e.position.lineNumber)
        })
      }
    }, [onCursorLine])

    // Switch Monaco theme when app theme changes
    useEffect(() => {
      const monaco = monacoRef.current
      if (monaco) {
        monaco.editor.setTheme(activeTheme)
      }
    }, [activeTheme])

    // Clean up markers when component unmounts
    useEffect(() => {
      return () => {
        const monaco = monacoRef.current
        const ed = editorRef.current
        if (monaco && ed) {
          const model = ed.getModel()
          if (model) {
            monaco.editor.setModelMarkers(model, 'varpulis', [])
          }
        }
      }
    }, [])

    return (
      <div className={className}>
        <Editor
          height="100%"
          language={VPL_LANGUAGE_ID}
          theme={activeTheme}
          value={value || SAMPLE_VPL}
          onChange={(v) => onChange(v ?? '')}
          onMount={handleMount}
          options={{
            fontSize: 13,
            lineHeight: 20,
            fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
            minimap: { enabled: false },
            scrollBeyondLastLine: false,
            wordWrap: 'on',
            tabSize: 4,
            insertSpaces: true,
            renderLineHighlight: 'line',
            overviewRulerBorder: false,
            padding: { top: 12 },
            glyphMargin: true,
          }}
        />
      </div>
    )
  }
)
