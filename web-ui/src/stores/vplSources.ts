import { defineStore } from 'pinia'
import { ref, computed } from 'vue'

export interface SavedVplSource {
  id: string
  name: string
  source: string
  savedAt: string
  description?: string
}

export interface ParsedEvents {
  inputs: EventDefinition[]
  outputs: EventDefinition[]
}

export interface EventDefinition {
  name: string
  fields: { name: string; type: string }[]
}

const STORAGE_KEY = 'varpulis_saved_sources'

export const useVplSourcesStore = defineStore('vplSources', () => {
  const sources = ref<SavedVplSource[]>([])

  // Load from localStorage on init
  function loadFromStorage(): void {
    try {
      const stored = localStorage.getItem(STORAGE_KEY)
      if (stored) {
        sources.value = JSON.parse(stored)
      }
    } catch (e) {
      console.error('Failed to load saved sources:', e)
      sources.value = []
    }
  }

  function saveToStorage(): void {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(sources.value))
  }

  const sortedSources = computed(() =>
    [...sources.value].sort((a, b) =>
      new Date(b.savedAt).getTime() - new Date(a.savedAt).getTime()
    )
  )

  function saveSource(name: string, source: string, description?: string): SavedVplSource {
    // Check if name already exists, update if so
    const existing = sources.value.find(s => s.name === name)

    if (existing) {
      existing.source = source
      existing.savedAt = new Date().toISOString()
      existing.description = description
      saveToStorage()
      return existing
    }

    const newSource: SavedVplSource = {
      id: `vpl-${Date.now()}`,
      name,
      source,
      savedAt: new Date().toISOString(),
      description,
    }
    sources.value.push(newSource)
    saveToStorage()
    return newSource
  }

  function deleteSource(id: string): void {
    sources.value = sources.value.filter(s => s.id !== id)
    saveToStorage()
  }

  function getSource(id: string): SavedVplSource | undefined {
    return sources.value.find(s => s.id === id)
  }

  function renameSource(id: string, newName: string): void {
    const source = sources.value.find(s => s.id === id)
    if (source) {
      source.name = newName
      saveToStorage()
    }
  }

  // Parse VPL source to extract event definitions and categorize as input/output
  function parseEvents(source: string): ParsedEvents {
    const inputs: EventDefinition[] = []
    const outputs: EventDefinition[] = []

    // 1. Parse all event schema definitions: event EventName:\n    field: type
    const eventSchemas = new Map<string, { name: string; type: string }[]>()
    const eventRegex = /event\s+(\w+)\s*:\s*((?:\n\s+\w+\s*:\s*\w+)*)/g
    let match

    while ((match = eventRegex.exec(source)) !== null) {
      const eventName = match[1]
      const fieldsBlock = match[2]
      const fields: { name: string; type: string }[] = []

      const fieldRegex = /(\w+)\s*:\s*(\w+)/g
      let fieldMatch
      while ((fieldMatch = fieldRegex.exec(fieldsBlock)) !== null) {
        fields.push({ name: fieldMatch[1], type: fieldMatch[2] })
      }

      eventSchemas.set(eventName, fields)
    }

    // 2. Find input event types: stream X = EventName
    const streamRegex = /stream\s+\w+\s*=\s*(\w+)/g
    const inputNames = new Set<string>()

    while ((match = streamRegex.exec(source)) !== null) {
      inputNames.add(match[1])
    }

    for (const name of inputNames) {
      inputs.push({ name, fields: eventSchemas.get(name) || [] })
    }

    // 3. Find output event types: .emit() with event_type field or .to() sinks
    const emitRegex = /\.emit\s*\(\s*[\s\S]*?event_type\s*:\s*["'](\w+)["']/g
    const outputNames = new Set<string>()

    while ((match = emitRegex.exec(source)) !== null) {
      outputNames.add(match[1])
    }

    // Also detect streams with .to() sinks as outputs
    const toRegex = /stream\s+(\w+)\s*=[\s\S]*?\.to\s*\(/g
    while ((match = toRegex.exec(source)) !== null) {
      // The stream itself produces output
    }

    for (const name of outputNames) {
      outputs.push({ name, fields: eventSchemas.get(name) || [] })
    }

    return { inputs, outputs }
  }

  // Initialize
  loadFromStorage()

  return {
    sources,
    sortedSources,
    saveSource,
    deleteSource,
    getSource,
    renameSource,
    parseEvents,
    loadFromStorage,
  }
})
