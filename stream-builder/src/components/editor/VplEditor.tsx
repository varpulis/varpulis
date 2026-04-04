import { useRef, useCallback } from 'react'
import Editor, { type OnMount } from '@monaco-editor/react'
import type { editor } from 'monaco-editor'
import { VPL_LANGUAGE_ID, vplLanguageConfig, vplTokensProvider, vplTheme } from './vplLanguage'

interface VplEditorProps {
  value: string
  onChange: (value: string) => void
  className?: string
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

export function VplEditor({ value, onChange, className }: VplEditorProps) {
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null)

  const handleMount: OnMount = useCallback((editor, monaco) => {
    editorRef.current = editor

    // Register VPL language
    if (!monaco.languages.getLanguages().some((l: { id: string }) => l.id === VPL_LANGUAGE_ID)) {
      monaco.languages.register({ id: VPL_LANGUAGE_ID })
      monaco.languages.setLanguageConfiguration(VPL_LANGUAGE_ID, vplLanguageConfig)
      monaco.languages.setMonarchTokensProvider(VPL_LANGUAGE_ID, vplTokensProvider)
      monaco.editor.defineTheme('vpl-dark', vplTheme)
    }

    monaco.editor.setTheme('vpl-dark')
  }, [])

  return (
    <div className={className}>
      <Editor
        height="100%"
        language={VPL_LANGUAGE_ID}
        theme="vpl-dark"
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
        }}
      />
    </div>
  )
}
