<script setup lang="ts">
import { ref, onMounted, onUnmounted, watch } from 'vue'
import { useRouter } from 'vue-router'
import { useClusterStore } from '@/stores/cluster'
import { useDemoStore } from '@/stores/demo'
import type { ScenarioDefinition } from '@/types/scenario'
import type { InjectBatchResponse } from '@/types/pipeline'
import AlertBanner from './AlertBanner.vue'
import NarrationPanel from './NarrationPanel.vue'
import StatsBar from './StatsBar.vue'

const props = defineProps<{
  scenario: ScenarioDefinition
}>()

const emit = defineEmits<{
  alerts: [alerts: Array<Record<string, unknown>>]
  'step-changed': [index: number]
  injected: [eventsSent: number, outputEvents: Array<Record<string, unknown>>]
}>()

const router = useRouter()
const clusterStore = useClusterStore()
const demoStore = useDemoStore()

const currentStep = ref(0)
const isFullscreen = ref(false)
const deploying = ref(false)
const deployed = ref(false)
const deployError = ref<string | null>(null)
const injecting = ref(false)
const groupId = ref<string | null>(null)
const alerts = ref<Array<{
  type: string
  severity: 'critical' | 'warning' | 'info'
  title: string
  fields: Record<string, unknown>
}>>([])
const alertCount = ref(0)
const showVpl = ref(false)
const showEventLog = ref(false)
const totalEventsSent = ref(0)
const totalMatches = ref(0)
const totalTraditional = ref(0)
const totalProcessingMs = ref(0)
const seenAlertTypes = ref(new Set<string>())
const allOutputEvents = ref<Array<Record<string, unknown>>>([])

interface EventLogEntry {
  timestamp: number
  stepTitle: string
  eventsSent: number
  eventsFailed: number
  outputEvents: Array<Record<string, unknown>>
}
const eventLog = ref<EventLogEntry[]>([])

const step = () => props.scenario.steps[currentStep.value]

/** Normalize output events: flatten `fields` to top level if nested */
function normalizeOutputEvents(events: Array<Record<string, unknown>>): Array<Record<string, unknown>> {
  return events.map((evt) => {
    if (evt.fields && typeof evt.fields === 'object' && !Array.isArray(evt.fields)) {
      const flat: Record<string, unknown> = {}
      if (evt.event_type) flat.event_type = evt.event_type
      const fields = evt.fields as Record<string, unknown>
      for (const [k, v] of Object.entries(fields)) {
        flat[k] = v
      }
      return flat
    }
    return evt
  })
}

// Deploy on mount — reuse existing session if available
onMounted(async () => {
  const existing = demoStore.getSession(props.scenario.id)
  if (existing) {
    // Reconnect to existing deployment
    groupId.value = existing.groupId
    deployed.value = true
    return
  }

  deploying.value = true
  deployError.value = null
  try {
    const group = await clusterStore.deployGroup({
      name: `demo-${props.scenario.id}`,
      pipelines: [{
        name: props.scenario.id,
        source: props.scenario.vplSource,
      }],
    })
    groupId.value = group.id
    deployed.value = true
    demoStore.setSession(props.scenario.id, group.id)
  } catch (e) {
    deployError.value = e instanceof Error ? e.message : 'Failed to deploy'
  } finally {
    deploying.value = false
  }
})

// Only exit fullscreen on unmount — pipeline stays deployed
onUnmounted(() => {
  if (document.fullscreenElement) {
    document.exitFullscreen()
  }
})

async function endDemo() {
  if (groupId.value) {
    try {
      await clusterStore.teardownGroup(groupId.value)
    } catch {
      // Ignore teardown errors
    }
    demoStore.removeSession(props.scenario.id)
  }
  router.push('/scenarios')
}

watch(currentStep, (val) => {
  emit('step-changed', val)
})

async function injectEvents() {
  const s = step()
  if (!s || !groupId.value) return

  injecting.value = true
  try {
    const rawResponse: InjectBatchResponse = await clusterStore.injectBatch(groupId.value, s.eventsText)
    const outputEvents = normalizeOutputEvents(rawResponse.output_events)

    // Log the injection result
    eventLog.value.unshift({
      timestamp: Date.now(),
      stepTitle: s.title,
      eventsSent: rawResponse.events_sent,
      eventsFailed: rawResponse.events_failed,
      outputEvents,
    })

    // Update cumulative stats
    totalEventsSent.value += rawResponse.events_sent
    totalMatches.value += outputEvents.length
    totalProcessingMs.value += (rawResponse.processing_time_us ?? 0) / 1000
    for (const evt of outputEvents) {
      const alertType = String(evt.alert_type || evt.event_type || 'alert')
      seenAlertTypes.value.add(alertType)
    }
    totalTraditional.value = seenAlertTypes.value.size

    // Accumulate all output events for showcase demos
    allOutputEvents.value.push(...outputEvents)

    // Process output events as alerts
    if (outputEvents.length > 0) {
      const newAlerts = outputEvents.map((evt) => ({
        type: String(evt.alert_type || evt.event_type || 'alert'),
        severity: 'critical' as const,
        title: String(evt.alert_type || evt.event_type || 'Alert Detected'),
        fields: evt,
      }))
      alerts.value.push(...newAlerts)
      alertCount.value += newAlerts.length
      emit('alerts', outputEvents)
    }

    // Always emit injected (even with 0 outputs)
    emit('injected', rawResponse.events_sent, outputEvents)
  } catch (e) {
    console.error('Inject failed:', e)
  } finally {
    injecting.value = false
  }
}

function nextStep() {
  if (currentStep.value < props.scenario.steps.length - 1) {
    currentStep.value++
  }
}

function prevStep() {
  if (currentStep.value > 0) {
    currentStep.value--
  }
}

function copyVplSource() {
  navigator.clipboard.writeText(props.scenario.vplSource)
}

async function toggleFullscreen() {
  if (!document.fullscreenElement) {
    await document.documentElement.requestFullscreen()
    isFullscreen.value = true
  } else {
    await document.exitFullscreen()
    isFullscreen.value = false
  }
}
</script>

<template>
  <div class="demo-shell" :class="{ 'cinema-mode': isFullscreen }">
    <!-- Deploy Overlay -->
    <v-overlay
      :model-value="deploying"
      persistent
      class="d-flex align-center justify-center"
      scrim="black"
    >
      <div class="text-center">
        <v-progress-circular indeterminate size="64" color="primary" class="mb-4" />
        <div class="text-h6 text-white">Deploying pipeline...</div>
        <div class="text-body-2 text-medium-emphasis mt-2">{{ scenario.title }}</div>
      </div>
    </v-overlay>

    <!-- Deploy Error -->
    <v-alert v-if="deployError" type="error" class="mb-4">
      {{ deployError }}
      <template #append>
        <v-btn variant="text" @click="router.push('/scenarios')">Back to Scenarios</v-btn>
      </template>
    </v-alert>

    <!-- Presentation Bar -->
    <div class="presentation-bar d-flex align-center px-4 py-2">
      <v-btn
        icon="mdi-arrow-left"
        variant="text"
        color="white"
        title="Back (pipeline stays running)"
        @click="router.push('/scenarios')"
      />

      <span class="text-h5 font-weight-bold text-white ml-2">
        {{ scenario.title }}
      </span>

      <v-chip
        v-if="deployed"
        color="success"
        size="small"
        variant="tonal"
        class="ml-3"
        label
      >
        <v-icon start size="small">mdi-check-circle</v-icon>
        Pipeline Running
      </v-chip>

      <v-spacer />

      <!-- Step Indicator -->
      <div class="d-flex align-center mr-4">
        <v-chip
          v-for="(_s, i) in scenario.steps"
          :key="i"
          :color="i === currentStep ? 'primary' : 'grey-darken-2'"
          size="small"
          class="mx-1"
          :class="{ 'step-active': i === currentStep }"
          @click="currentStep = i"
        >
          {{ i + 1 }}
        </v-chip>
      </div>

      <!-- Alert Badge -->
      <v-badge
        v-if="alertCount > 0"
        :content="alertCount"
        color="error"
        class="mr-3"
      >
        <v-icon color="white">mdi-bell</v-icon>
      </v-badge>

      <v-btn
        icon="mdi-code-braces"
        variant="text"
        color="white"
        title="View VPL Patterns"
        @click="showVpl = !showVpl; showEventLog = false"
      />
      <v-badge
        :content="eventLog.length"
        :model-value="eventLog.length > 0"
        color="primary"
      >
        <v-btn
          icon="mdi-format-list-bulleted"
          variant="text"
          color="white"
          title="Event Log"
          @click="showEventLog = !showEventLog; showVpl = false"
        />
      </v-badge>

      <v-btn
        :icon="isFullscreen ? 'mdi-fullscreen-exit' : 'mdi-fullscreen'"
        variant="text"
        color="white"
        @click="toggleFullscreen"
      />

      <v-btn
        variant="tonal"
        color="error"
        size="small"
        class="ml-2"
        title="Tear down pipeline and end demo"
        @click="endDemo"
      >
        <v-icon start size="small">mdi-stop</v-icon>
        End Demo
      </v-btn>
    </div>

    <!-- Stats Bar (overridable via #stats slot) -->
    <slot
      name="stats"
      :events-sent="totalEventsSent"
      :match-count="totalMatches"
      :processing-time-ms="totalProcessingMs"
      :output-events="allOutputEvents"
    >
      <StatsBar
        v-if="totalEventsSent > 0"
        :events-sent="totalEventsSent"
        :match-count="totalMatches"
        :traditional-count="totalTraditional"
        :processing-time-ms="totalProcessingMs"
      />
    </slot>

    <!-- Alert Banners -->
    <div class="px-4">
      <AlertBanner :alerts="alerts.slice(-3)" />
    </div>

    <!-- Hero Visualization Area -->
    <div class="hero-area flex-grow-1">
      <slot name="hero" />
    </div>

    <!-- Narration Panel -->
    <NarrationPanel
      v-if="step()"
      :step="step()!"
      :step-index="currentStep"
      :total-steps="scenario.steps.length"
      :injecting="injecting"
      :can-go-back="currentStep > 0"
      :can-go-forward="currentStep < scenario.steps.length - 1"
      @inject="injectEvents"
      @next="nextStep"
      @prev="prevStep"
    />

    <!-- VPL Source Drawer -->
    <v-navigation-drawer
      v-model="showVpl"
      location="right"
      temporary
      width="560"
      class="vpl-drawer"
      scrim="rgba(0,0,0,0.5)"
    >
      <div class="pa-4">
        <div class="d-flex align-center mb-4">
          <v-icon class="mr-2" color="primary">mdi-code-braces</v-icon>
          <span class="text-h6 font-weight-bold">VPL Pipeline</span>
          <v-spacer />
          <v-btn icon="mdi-close" variant="text" size="small" @click="showVpl = false" />
        </div>

        <!-- Pattern descriptions -->
        <div v-for="pattern in scenario.patterns" :key="pattern.name" class="mb-4">
          <v-chip color="primary" size="small" label class="mb-2">{{ pattern.name }}</v-chip>
          <div class="text-body-2 text-medium-emphasis mb-2">{{ pattern.description }}</div>
          <pre class="vpl-snippet pa-3 rounded">{{ pattern.vplSnippet }}</pre>
        </div>

        <v-divider class="my-4" />

        <!-- Full VPL source -->
        <div class="d-flex align-center mb-2">
          <span class="text-subtitle-2 font-weight-bold">Full Source</span>
          <v-spacer />
          <v-btn
            size="x-small"
            variant="tonal"
            color="primary"
            prepend-icon="mdi-content-copy"
            @click="copyVplSource"
          >
            Copy
          </v-btn>
        </div>
        <pre class="vpl-source pa-3 rounded">{{ scenario.vplSource }}</pre>
      </div>
    </v-navigation-drawer>

    <!-- Event Log Drawer -->
    <v-navigation-drawer
      v-model="showEventLog"
      location="right"
      temporary
      width="560"
      class="event-log-drawer"
      scrim="rgba(0,0,0,0.5)"
    >
      <div class="pa-4">
        <div class="d-flex align-center mb-4">
          <v-icon class="mr-2" color="primary">mdi-format-list-bulleted</v-icon>
          <span class="text-h6 font-weight-bold">Event Log</span>
          <v-spacer />
          <v-btn icon="mdi-close" variant="text" size="small" @click="showEventLog = false" />
        </div>

        <div v-if="eventLog.length === 0" class="text-center text-medium-emphasis pa-8">
          <v-icon size="48" class="mb-3" color="grey-darken-1">mdi-tray-arrow-down</v-icon>
          <div class="text-body-2">No events injected yet</div>
          <div class="text-caption mt-1">Click "Inject Events" to start</div>
        </div>

        <div v-for="(entry, idx) in eventLog" :key="idx" class="log-entry mb-4">
          <div class="d-flex align-center mb-2">
            <v-chip color="primary" size="small" label class="mr-2">
              {{ entry.stepTitle }}
            </v-chip>
            <span class="text-caption text-medium-emphasis">
              {{ new Date(entry.timestamp).toLocaleTimeString() }}
            </span>
          </div>

          <div class="d-flex gap-3 mb-2">
            <v-chip size="x-small" variant="tonal" color="success">
              {{ entry.eventsSent }} sent
            </v-chip>
            <v-chip v-if="entry.eventsFailed > 0" size="x-small" variant="tonal" color="error">
              {{ entry.eventsFailed }} failed
            </v-chip>
            <v-chip size="x-small" variant="tonal" :color="entry.outputEvents.length > 0 ? 'warning' : 'grey'">
              {{ entry.outputEvents.length }} output
            </v-chip>
          </div>

          <!-- Output events detail -->
          <div v-for="(evt, evtIdx) in entry.outputEvents" :key="evtIdx" class="output-event pa-3 rounded mb-2">
            <div class="d-flex align-center mb-2">
              <v-icon size="small" color="error" class="mr-2">mdi-alert-circle</v-icon>
              <span class="text-body-2 font-weight-bold" style="color: #FF5252">
                {{ evt.alert_type || evt.event_type || 'Output Event' }}
              </span>
            </div>
            <div
              v-for="(value, key) in evt"
              :key="String(key)"
              class="d-flex justify-space-between py-1"
              style="border-bottom: 1px solid rgba(255,255,255,0.05)"
            >
              <span class="text-caption text-medium-emphasis">{{ key }}</span>
              <span class="text-caption font-weight-medium">{{ value }}</span>
            </div>
          </div>

          <v-divider v-if="idx < eventLog.length - 1" class="mt-3" />
        </div>
      </div>
    </v-navigation-drawer>
  </div>
</template>

<style scoped>
.demo-shell {
  display: flex;
  flex-direction: column;
  min-height: calc(100vh - 100px);
  background: radial-gradient(ellipse at center, #1a1a2e 0%, #0a0a0a 70%);
  border-radius: 12px;
  overflow: hidden;
  margin: -16px;
}

.cinema-mode {
  position: fixed;
  inset: 0;
  z-index: 2000;
  margin: 0;
  border-radius: 0;
  min-height: 100vh;
}

.presentation-bar {
  background: rgba(0, 0, 0, 0.5);
  border-bottom: 1px solid rgba(255, 255, 255, 0.1);
  min-height: 56px;
}

.hero-area {
  flex: 1;
  padding: 16px;
  overflow: auto;
}

@keyframes step-pulse {
  0%, 100% { box-shadow: 0 0 0 0 rgba(124, 77, 255, 0.4); }
  50% { box-shadow: 0 0 0 8px rgba(124, 77, 255, 0); }
}

.step-active {
  animation: step-pulse 2s ease-in-out infinite;
}

.vpl-drawer :deep(.v-navigation-drawer__content),
.event-log-drawer :deep(.v-navigation-drawer__content) {
  background: #121212;
  overflow-y: auto;
}

.vpl-snippet {
  background: rgba(124, 77, 255, 0.08);
  border: 1px solid rgba(124, 77, 255, 0.2);
  font-family: 'JetBrains Mono', 'Fira Code', 'Consolas', monospace;
  font-size: 13px;
  line-height: 1.5;
  color: #E0E0E0;
  white-space: pre-wrap;
  word-break: break-word;
  overflow-x: auto;
}

.vpl-source {
  background: rgba(255, 255, 255, 0.04);
  border: 1px solid rgba(255, 255, 255, 0.08);
  font-family: 'JetBrains Mono', 'Fira Code', 'Consolas', monospace;
  font-size: 12px;
  line-height: 1.6;
  color: #BDBDBD;
  white-space: pre-wrap;
  word-break: break-word;
  overflow-x: auto;
  max-height: 400px;
}

.log-entry {
  padding: 12px;
  background: rgba(255, 255, 255, 0.03);
  border-radius: 8px;
  border: 1px solid rgba(255, 255, 255, 0.06);
}

.output-event {
  background: rgba(255, 82, 82, 0.06);
  border: 1px solid rgba(255, 82, 82, 0.15);
}
</style>
