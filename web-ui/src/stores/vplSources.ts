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

  // Parse VPL source to extract event definitions
  function parseEvents(source: string): ParsedEvents {
    const inputs: EventDefinition[] = []
    const outputs: EventDefinition[] = []

    // Parse event definitions (input event types)
    // Pattern: event EventName:\n    field1: type1\n    field2: type2
    const eventRegex = /event\s+(\w+)\s*:\s*((?:\n\s+\w+\s*:\s*\w+)*)/g
    let match

    while ((match = eventRegex.exec(source)) !== null) {
      const eventName = match[1]
      const fieldsBlock = match[2]
      const fields: { name: string; type: string }[] = []

      // Parse fields
      const fieldRegex = /(\w+)\s*:\s*(\w+)/g
      let fieldMatch
      while ((fieldMatch = fieldRegex.exec(fieldsBlock)) !== null) {
        fields.push({
          name: fieldMatch[1],
          type: fieldMatch[2],
        })
      }

      inputs.push({ name: eventName, fields })
    }

    // Parse output events from .emit() calls
    // Pattern: .emit(\n    event_type: "EventName",
    const emitRegex = /\.emit\s*\(\s*\n?\s*event_type\s*:\s*["'](\w+)["']/g
    const outputNames = new Set<string>()

    while ((match = emitRegex.exec(source)) !== null) {
      outputNames.add(match[1])
    }

    // Also check for simple emit patterns
    const simpleEmitRegex = /emit\s*\(\s*event_type\s*:\s*["'](\w+)["']/g
    while ((match = simpleEmitRegex.exec(source)) !== null) {
      outputNames.add(match[1])
    }

    // Create output event definitions (fields unknown from emit)
    for (const name of outputNames) {
      // Try to find if this event type is defined in the source
      const defined = inputs.find(e => e.name === name)
      outputs.push({
        name,
        fields: defined?.fields || [],
      })
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
