<script lang="ts">
export default { name: 'PlaygroundView' }
</script>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import ExampleSelector from '@/components/playground/ExampleSelector.vue'
import EventPanel from '@/components/playground/EventPanel.vue'
import ResultsPanel from '@/components/playground/ResultsPanel.vue'
import ShareButton from '@/components/playground/ShareButton.vue'
import { playgroundRun, type PlaygroundRunResponse, type PlaygroundExampleDetail } from '@/api/playground'

const route = useRoute()

const vplSource = ref(`# Welcome to the Varpulis Playground!
# Select an example or write your own VPL below.

stream HighTemp = TempReading
    .where(temperature > 30)
    .emit(alert: "High temperature", sensor: sensor_id, temp: temperature)
`)

const events = ref(`# Welcome events — edit or select an example
@0s TempReading { sensor_id: "HVAC-01", temperature: 22 }
@1s TempReading { sensor_id: "HVAC-02", temperature: 35 }
@2s TempReading { sensor_id: "HVAC-03", temperature: 28 }
@3s TempReading { sensor_id: "HVAC-01", temperature: 41 }
`)

const result = ref<PlaygroundRunResponse | null>(null)
const running = ref(false)
const currentExample = ref<string | null>(null)

onMounted(() => {
  // Check for shared code in URL
  const code = route.query.code as string | undefined
  if (code) {
    vplSource.value = decodeURIComponent(code)
  }
})

function onSelectExample(example: PlaygroundExampleDetail) {
  vplSource.value = example.vpl
  events.value = example.events
  currentExample.value = example.name
  result.value = null
}

async function runPipeline() {
  running.value = true
  result.value = null
  try {
    result.value = await playgroundRun(vplSource.value, events.value)
  } catch (err: unknown) {
    result.value = {
      ok: false,
      events_processed: 0,
      output_events: [],
      latency_ms: 0,
      diagnostics: [],
      error: err instanceof Error ? err.message : 'Failed to connect to server',
    }
  } finally {
    running.value = false
  }
}
</script>

<template>
  <v-container fluid class="playground-container pa-0">
    <!-- Toolbar -->
    <v-toolbar density="compact" class="playground-toolbar" flat>
      <ExampleSelector @select="onSelectExample" />

      <v-spacer />

      <v-btn
        color="success"
        variant="flat"
        size="small"
        :loading="running"
        @click="runPipeline"
        prepend-icon="mdi-play"
        class="mr-2"
      >
        Run
      </v-btn>

      <ShareButton :vpl="vplSource" class="mr-2" />

      <v-btn
        variant="outlined"
        size="small"
        href="https://github.com/varpulis/varpulis"
        target="_blank"
        prepend-icon="mdi-star-outline"
      >
        Star on GitHub
      </v-btn>
    </v-toolbar>

    <!-- Main content: 3-panel layout -->
    <div class="playground-layout">
      <!-- Left panel: VPL Editor -->
      <div class="editor-panel">
        <div class="panel-header text-subtitle-2 pa-2">
          <v-icon size="small" class="mr-1">mdi-code-tags</v-icon>
          VPL Editor
          <v-chip v-if="currentExample" size="x-small" class="ml-2" closable @click:close="currentExample = null">
            {{ currentExample }}
          </v-chip>
        </div>
        <div class="editor-content">
          <v-textarea
            v-model="vplSource"
            variant="solo-filled"
            density="compact"
            auto-grow
            rows="20"
            hide-details
            class="mono-editor"
            placeholder="Write your VPL code here..."
          />
        </div>
      </div>

      <!-- Right panels: Events + Results -->
      <div class="right-panels">
        <!-- Events panel -->
        <div class="events-section pa-3">
          <EventPanel v-model="events" />
        </div>

        <!-- Divider -->
        <v-divider />

        <!-- Results panel -->
        <div class="results-section pa-3">
          <ResultsPanel :result="result" :loading="running" />
        </div>
      </div>
    </div>
  </v-container>
</template>

<style scoped>
.playground-container {
  height: 100vh;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.playground-toolbar {
  flex-shrink: 0;
  border-bottom: 1px solid rgba(var(--v-border-color), var(--v-border-opacity));
}

.playground-layout {
  flex: 1;
  display: flex;
  overflow: hidden;
}

.editor-panel {
  flex: 1;
  display: flex;
  flex-direction: column;
  border-right: 1px solid rgba(var(--v-border-color), var(--v-border-opacity));
  min-width: 0;
}

.panel-header {
  flex-shrink: 0;
  border-bottom: 1px solid rgba(var(--v-border-color), var(--v-border-opacity));
  background: rgba(var(--v-theme-surface-variant), 0.15);
}

.editor-content {
  flex: 1;
  overflow: auto;
}

.right-panels {
  flex: 1;
  display: flex;
  flex-direction: column;
  min-width: 0;
}

.events-section {
  flex: 0 0 auto;
  max-height: 40%;
  overflow-y: auto;
}

.results-section {
  flex: 1;
  overflow-y: auto;
}

.mono-editor :deep(textarea) {
  font-family: 'JetBrains Mono', 'Fira Code', 'Consolas', monospace !important;
  font-size: 0.85rem !important;
  line-height: 1.5 !important;
  tab-size: 4;
}

.mono-editor :deep(.v-field) {
  border-radius: 0;
}
</style>
