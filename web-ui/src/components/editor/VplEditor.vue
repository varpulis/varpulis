<script setup lang="ts">
import { ref, watch, computed, onUnmounted } from 'vue'
import { VueMonacoEditor } from '@guolao/vue-monaco-editor'
import type { editor } from 'monaco-editor'
import { registerVplLanguage } from './vpl-language'
import { validateVpl } from '@/api/cluster'
import { useConnectorsStore } from '@/stores/connectors'

const props = defineProps<{
  modelValue: string
  height?: number | string
  readOnly?: boolean
}>()

const emit = defineEmits<{
  'update:modelValue': [value: string]
  validate: [result: { valid: boolean; errors?: string[]; isAuto?: boolean }]
}>()

const connectorsStore = useConnectorsStore()

const editorRef = ref<editor.IStandaloneCodeEditor | null>(null)
const internalValue = ref(props.modelValue)

// Module-level flag — survives component unmount/remount
let languageRegistered = false
const isValidating = ref(false)
let debounceTimer: ReturnType<typeof setTimeout> | null = null
let validationTimer: ReturnType<typeof setTimeout> | null = null

onUnmounted(() => {
  if (debounceTimer) {
    clearTimeout(debounceTimer)
  }
  if (validationTimer) {
    clearTimeout(validationTimer)
  }
})

// Compute height as string with px
const editorHeight = computed(() => {
  if (!props.height) return '500px'
  if (typeof props.height === 'number') return `${props.height}px`
  return props.height
})

// Monaco editor options
const editorOptions: editor.IStandaloneEditorConstructionOptions = {
  theme: 'vpl-dark',
  fontSize: 14,
  fontFamily: "'JetBrains Mono', 'Fira Code', Consolas, monospace",
  fontLigatures: true,
  lineNumbers: 'on',
  minimap: { enabled: false },
  scrollBeyondLastLine: false,
  automaticLayout: true,
  tabSize: 4,
  insertSpaces: true,
  wordWrap: 'on',
  renderWhitespace: 'selection',
  bracketPairColorization: { enabled: true },
  guides: {
    bracketPairs: true,
    indentation: true,
  },
  suggest: {
    showKeywords: true,
    showSnippets: true,
  },
  readOnly: props.readOnly,
}

function handleEditorMount(editor: editor.IStandaloneCodeEditor): void {
  editorRef.value = editor

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const monaco = (window as any).monaco
  if (!monaco) return

  // Register VPL language once globally (survives remounts)
  if (!languageRegistered) {
    connectorsStore.fetchConnectors()
    registerVplLanguage(() => connectorsStore.connectorNames)
    languageRegistered = true
  }

  // Set the model language to VPL
  const model = editor.getModel()
  if (model) {
    monaco.editor.setModelLanguage(model, 'vpl')
  }

  // Add keyboard shortcuts for Ctrl/Cmd + S
  editor.addCommand(
    monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyS,
    () => {
      validate()
    }
  )
}

function handleChange(value: string | undefined): void {
  const newValue = value || ''

  // If this change matches the current prop, it was triggered by a programmatic
  // setValue (e.g., New/Open). Don't echo it back to the parent.
  if (newValue === props.modelValue) return

  internalValue.value = newValue

  // Debounce the emit to prevent freezing on rapid typing
  if (debounceTimer) {
    clearTimeout(debounceTimer)
  }
  debounceTimer = setTimeout(() => {
    emit('update:modelValue', newValue)
  }, 100)

  // Schedule validation after typing stops
  scheduleValidation()
}

async function validate(isAuto = false): Promise<void> {
  if (isValidating.value) return

  const source = internalValue.value
  if (!source.trim()) {
    emit('validate', { valid: true, isAuto })
    clearMarkers()
    return
  }

  isValidating.value = true

  try {
    const result = await validateVpl(source)

    // Update Monaco editor markers
    setMarkers(result.diagnostics)

    // Emit result
    const errors = result.diagnostics
      .filter(d => d.severity === 'error')
      .map(d => {
        let msg = `Line ${d.line}: ${d.message}`
        if (d.hint) msg += ` (${d.hint})`
        return msg
      })

    emit('validate', {
      valid: result.valid,
      errors: errors.length > 0 ? errors : undefined,
      isAuto,
    })
  } catch (error) {
    // Fallback to basic validation if API is unavailable
    const errors = basicValidation(source)
    emit('validate', {
      valid: errors.length === 0,
      errors: errors.length > 0 ? errors : undefined,
      isAuto,
    })
  } finally {
    isValidating.value = false
  }
}

function basicValidation(source: string): string[] {
  const errors: string[] = []

  // Check for unclosed braces
  const openBraces = (source.match(/\{/g) || []).length
  const closeBraces = (source.match(/\}/g) || []).length
  if (openBraces !== closeBraces) {
    errors.push(`Mismatched braces: ${openBraces} opening, ${closeBraces} closing`)
  }

  // Check for unclosed parentheses
  const openParens = (source.match(/\(/g) || []).length
  const closeParens = (source.match(/\)/g) || []).length
  if (openParens !== closeParens) {
    errors.push(`Mismatched parentheses: ${openParens} opening, ${closeParens} closing`)
  }

  return errors
}

function setMarkers(diagnostics: Array<{ severity: string; line: number; column: number; message: string; hint?: string }>): void {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const monaco = (window as any).monaco
  if (!monaco || !editorRef.value) return

  const model = editorRef.value.getModel()
  if (!model) return

  const markers = diagnostics.map(d => ({
    severity: d.severity === 'error'
      ? monaco.MarkerSeverity.Error
      : d.severity === 'warning'
        ? monaco.MarkerSeverity.Warning
        : monaco.MarkerSeverity.Info,
    startLineNumber: d.line,
    startColumn: d.column,
    endLineNumber: d.line,
    endColumn: d.column + 1,
    message: d.hint ? `${d.message}\n\nHint: ${d.hint}` : d.message,
    source: 'vpl',
  }))

  monaco.editor.setModelMarkers(model, 'vpl', markers)
}

function clearMarkers(): void {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const monaco = (window as any).monaco
  if (!monaco || !editorRef.value) return

  const model = editorRef.value.getModel()
  if (model) {
    monaco.editor.setModelMarkers(model, 'vpl', [])
  }
}

// Debounced auto-validation on content change (markers only, no output)
function scheduleValidation(): void {
  if (validationTimer) {
    clearTimeout(validationTimer)
  }
  validationTimer = setTimeout(() => {
    validate(true) // isAuto=true: only update markers, skip output panel
  }, 1000) // Validate 1 second after last keystroke
}

// Expose validate method and validating state
defineExpose({ validate, isValidating })

// Watch for external changes (e.g., loading from storage or "New" button)
// Only update internalValue — VueMonacoEditor handles editor.setValue() via its :value prop.
// The guard in handleChange prevents echoing prop changes back to the parent.
watch(
  () => props.modelValue,
  (newValue) => {
    if (newValue !== internalValue.value) {
      internalValue.value = newValue
    }
  }
)
</script>

<template>
  <div class="vpl-editor" :style="{ height: editorHeight }">
    <VueMonacoEditor
      :value="internalValue"
      :height="editorHeight"
      language="vpl"
      :options="editorOptions"
      @mount="handleEditorMount"
      @change="handleChange"
    />
  </div>
</template>

<style scoped>
.vpl-editor {
  border-radius: 4px;
  overflow: hidden;
  min-height: 400px;
}
</style>
