<script lang="ts">
export default { name: 'ResultsPanel' }
</script>

<script setup lang="ts">
import { ref, computed } from 'vue'
import type { PlaygroundRunResponse } from '@/api/playground'

const props = defineProps<{
  result: PlaygroundRunResponse | null
  loading: boolean
}>()

const viewMode = ref<'table' | 'json'>('table')

const outputColumns = computed(() => {
  if (!props.result?.output_events?.length) return []
  const keys = new Set<string>()
  for (const evt of props.result.output_events) {
    for (const k of Object.keys(evt)) {
      keys.add(k)
    }
  }
  return Array.from(keys)
})
</script>

<template>
  <div class="results-panel">
    <div class="d-flex align-center justify-space-between mb-2">
      <span class="text-subtitle-2">Results</span>
      <div v-if="result?.output_events?.length">
        <v-btn-toggle v-model="viewMode" mandatory density="compact" variant="outlined">
          <v-btn value="table" size="x-small">
            <v-icon size="small">mdi-table</v-icon>
          </v-btn>
          <v-btn value="json" size="x-small">
            <v-icon size="small">mdi-code-json</v-icon>
          </v-btn>
        </v-btn-toggle>
      </div>
    </div>

    <!-- Loading state -->
    <div v-if="loading" class="text-center pa-8">
      <v-progress-circular indeterminate color="primary" />
      <div class="text-caption mt-2">Running pipeline...</div>
    </div>

    <!-- No result yet -->
    <div v-else-if="!result" class="text-center text-medium-emphasis pa-8">
      <v-icon size="48" color="grey">mdi-play-circle-outline</v-icon>
      <div class="mt-2">Click <strong>Run</strong> to execute the pipeline</div>
    </div>

    <!-- Error result -->
    <div v-else-if="!result.ok" class="pa-2">
      <v-alert type="error" density="compact">
        {{ result.error || 'Execution failed' }}
      </v-alert>
      <div v-if="result.diagnostics?.length" class="mt-2">
        <div v-for="(d, i) in result.diagnostics" :key="i" class="diagnostic-item pa-2 mb-1 rounded">
          <v-icon size="small" :color="d.severity === 'error' ? 'error' : 'warning'" class="mr-1">
            {{ d.severity === 'error' ? 'mdi-close-circle' : 'mdi-alert' }}
          </v-icon>
          <span class="text-caption">L{{ d.start_line + 1 }}:{{ d.start_col + 1 }}</span>
          <span class="ml-2">{{ d.message }}</span>
          <div v-if="d.hint" class="text-caption text-medium-emphasis ml-6">{{ d.hint }}</div>
        </div>
      </div>
    </div>

    <!-- Success result -->
    <div v-else class="pa-2">
      <div class="d-flex ga-4 mb-3">
        <v-chip color="success" size="small" variant="tonal">
          <v-icon start size="small">mdi-check-circle</v-icon>
          {{ result.output_events.length }} matches
        </v-chip>
        <v-chip size="small" variant="tonal">
          {{ result.events_processed }} events processed
        </v-chip>
        <v-chip size="small" variant="tonal">
          {{ result.latency_ms }}ms
        </v-chip>
      </div>

      <!-- No matches -->
      <div v-if="!result.output_events.length" class="text-center text-medium-emphasis pa-4">
        Pipeline executed successfully but produced no output events.
      </div>

      <!-- Table view -->
      <div v-else-if="viewMode === 'table'" class="results-table">
        <v-table density="compact" hover>
          <thead>
            <tr>
              <th v-for="col in outputColumns" :key="col" class="text-caption">{{ col }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(evt, idx) in result.output_events" :key="idx">
              <td v-for="col in outputColumns" :key="col" class="text-caption mono-text">
                {{ typeof evt[col] === 'object' ? JSON.stringify(evt[col]) : evt[col] ?? '' }}
              </td>
            </tr>
          </tbody>
        </v-table>
      </div>

      <!-- JSON view -->
      <div v-else class="json-view">
        <pre class="pa-3 rounded mono-text">{{ JSON.stringify(result.output_events, null, 2) }}</pre>
      </div>
    </div>
  </div>
</template>

<style scoped>
.results-panel {
  height: 100%;
  overflow-y: auto;
}
.results-table {
  max-height: 400px;
  overflow: auto;
}
.diagnostic-item {
  background: rgba(var(--v-theme-surface-variant), 0.3);
  font-size: 0.85rem;
}
.json-view pre {
  background: rgba(var(--v-theme-surface-variant), 0.3);
  font-size: 0.8rem;
  max-height: 400px;
  overflow: auto;
  white-space: pre-wrap;
  word-break: break-word;
}
.mono-text {
  font-family: 'JetBrains Mono', 'Fira Code', 'Consolas', monospace;
}
</style>
