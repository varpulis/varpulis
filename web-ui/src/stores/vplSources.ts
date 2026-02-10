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

    // 2. Parse all stream declarations: stream Name = Source
    const streamDefs = new Map<string, string>() // stream name → source name
    const streamRegex = /stream\s+(\w+)\s*=\s*(\w+)/g

    while ((match = streamRegex.exec(source)) !== null) {
      streamDefs.set(match[1], match[2])
    }

    // 3. Find input events: streams whose source is an event type (not another stream)
    const streamNames = new Set(streamDefs.keys())
    const inputNames = new Set<string>()

    for (const [, sourceName] of streamDefs) {
      if (!streamNames.has(sourceName)) {
        inputNames.add(sourceName)
      }
    }

    for (const name of inputNames) {
      inputs.push({ name, fields: eventSchemas.get(name) || [] })
    }

    // 4. Find output streams: streams with .emit() or .to()
    // Split source into per-stream blocks to avoid cross-stream regex matching
    const streamBlockRegex = /stream\s+(\w+)\s*=([\s\S]*?)(?=\nstream\s|\nevent\s|$)/g
    const outputNames = new Set<string>()

    while ((match = streamBlockRegex.exec(source)) !== null) {
      const streamName = match[1]
      const block = match[2]
      if (/\.emit\s*\(/.test(block) || /\.to\s*\(/.test(block)) {
        outputNames.add(streamName)
      }
    }

    for (const name of outputNames) {
      outputs.push({ name, fields: [] })
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
