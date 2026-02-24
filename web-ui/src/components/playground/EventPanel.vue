<script lang="ts">
export default { name: 'EventPanel' }
</script>

<script setup lang="ts">
import { ref, watch, computed } from 'vue'

const props = defineProps<{
  events: Record<string, unknown>[]
}>()

const emit = defineEmits<{
  (e: 'update:events', events: Record<string, unknown>[]): void
}>()

const rawText = ref('')
const parseError = ref<string | null>(null)
const editMode = ref(false)

// Sync incoming events to raw text
watch(() => props.events, (newEvents) => {
  if (!editMode.value) {
    rawText.value = newEvents.map(e => JSON.stringify(e)).join('\n')
    parseError.value = null
  }
}, { immediate: true })

const eventCount = computed(() => props.events.length)

function startEditing() {
  editMode.value = true
  rawText.value = props.events.map(e => JSON.stringify(e, null, 2)).join('\n')
}

function applyEdits() {
  try {
    // Try parsing as JSON array first
    const trimmed = rawText.value.trim()
    let parsed: Record<string, unknown>[]
    if (trimmed.startsWith('[')) {
      parsed = JSON.parse(trimmed)
    } else {
      // Parse as newline-delimited JSON
      parsed = trimmed
        .split('\n')
        .map(line => line.trim())
        .filter(line => line.length > 0)
        .map(line => JSON.parse(line))
    }
    emit('update:events', parsed)
    parseError.value = null
    editMode.value = false
  } catch (err: unknown) {
    parseError.value = err instanceof Error ? err.message : 'Invalid JSON'
  }
}

function cancelEditing() {
  editMode.value = false
  rawText.value = props.events.map(e => JSON.stringify(e)).join('\n')
  parseError.value = null
}
</script>

<template>
  <div class="event-panel">
    <div class="d-flex align-center justify-space-between mb-2">
      <span class="text-subtitle-2">
        Input Events
        <v-chip size="x-small" class="ml-1">{{ eventCount }}</v-chip>
      </span>
      <div>
        <v-btn
          v-if="!editMode"
          size="x-small"
          variant="text"
          icon="mdi-pencil"
          @click="startEditing"
          title="Edit events"
        />
        <template v-else>
          <v-btn size="x-small" variant="text" color="success" icon="mdi-check" @click="applyEdits" title="Apply" />
          <v-btn size="x-small" variant="text" color="error" icon="mdi-close" @click="cancelEditing" title="Cancel" />
        </template>
      </div>
    </div>

    <v-alert v-if="parseError" type="error" density="compact" class="mb-2">
      {{ parseError }}
    </v-alert>

    <div v-if="editMode" class="event-editor">
      <v-textarea
        v-model="rawText"
        variant="outlined"
        density="compact"
        rows="8"
        auto-grow
        hide-details
        class="mono-text"
        placeholder='{"event_type": "login", "user_id": "alice"}'
      />
    </div>
    <div v-else class="event-list">
      <div
        v-for="(event, idx) in events"
        :key="idx"
        class="event-item pa-2 mb-1 rounded"
      >
        <span class="text-caption text-medium-emphasis mr-2">#{{ idx + 1 }}</span>
        <v-chip size="x-small" color="primary" class="mr-2">{{ (event as Record<string, unknown>).event_type }}</v-chip>
        <span class="text-caption mono-text">{{ JSON.stringify(Object.fromEntries(Object.entries(event).filter(([k]) => k !== 'event_type'))) }}</span>
      </div>
      <div v-if="events.length === 0" class="text-center text-medium-emphasis pa-4">
        No events. Select an example or add events manually.
      </div>
    </div>
  </div>
</template>

<style scoped>
.event-panel {
  height: 100%;
  overflow-y: auto;
}
.event-list {
  max-height: 300px;
  overflow-y: auto;
}
.event-item {
  background: rgba(var(--v-theme-surface-variant), 0.3);
  font-size: 0.8rem;
  word-break: break-all;
}
.mono-text {
  font-family: 'JetBrains Mono', 'Fira Code', 'Consolas', monospace;
  font-size: 0.8rem;
}
.mono-text :deep(textarea) {
  font-family: 'JetBrains Mono', 'Fira Code', 'Consolas', monospace;
  font-size: 0.8rem;
}
</style>
