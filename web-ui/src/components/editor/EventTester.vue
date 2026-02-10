<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { usePipelinesStore } from '@/stores/pipelines'
import { useClusterStore } from '@/stores/cluster'
import { useWebSocketStore } from '@/stores/websocket'

const pipelinesStore = usePipelinesStore()
const clusterStore = useClusterStore()
const wsStore = useWebSocketStore()

const selectedGroupId = ref<string | null>(null)
const eventType = ref('UserLogin')
const eventDataJson = ref(`{
  "user_id": "user-123",
  "timestamp": "${new Date().toISOString()}",
  "ip_address": "192.168.1.100"
}`)
const loading = ref(false)
const response = ref<{ success: boolean; routed_to?: string; worker_id?: string; output_events?: Array<Record<string, unknown>>; error?: string } | null>(null)
const history = ref<Array<{
  id: string
  timestamp: Date
  eventType: string
  data: Record<string, unknown>
  response: { success: boolean; routed_to?: string; worker_id?: string; output_events?: Array<Record<string, unknown>>; error?: string }
}>>([])

const groups = computed(() => pipelinesStore.groups)
const eventLog = computed(() => wsStore.eventLog)

const jsonValid = computed(() => {
  try {
    JSON.parse(eventDataJson.value)
    return true
  } catch {
    return false
  }
})

async function injectEvent(): Promise<void> {
  if (!selectedGroupId.value || !jsonValid.value) return

  loading.value = true
  response.value = null

  try {
    const fields = JSON.parse(eventDataJson.value)
    const result = await clusterStore.injectEvent(selectedGroupId.value, {
      event_type: eventType.value,
      fields,
    })

    const mapped = {
      success: result.worker_response?.accepted ?? true,
      routed_to: result.routed_to,
      worker_id: result.worker_id,
      output_events: result.worker_response?.output_events ?? [],
    }

    response.value = mapped

    // Push output events to the shared event log
    for (const evt of mapped.output_events ?? []) {
      const evtType = (evt.event_type as string) ?? 'Unknown'
      const { event_type: _, ...data } = evt
      wsStore.eventLog.push({
        id: `inject-out-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
        direction: 'out',
        eventType: evtType,
        data,
        timestamp: new Date(),
        pipelineId: mapped.routed_to,
      })
    }

    // Add to history
    history.value.unshift({
      id: `inject-${Date.now()}`,
      timestamp: new Date(),
      eventType: eventType.value,
      data: fields,
      response: mapped,
    })

    // Keep last 50 entries
    if (history.value.length > 50) {
      history.value = history.value.slice(0, 50)
    }
  } catch (err: unknown) {
    // Extract detailed error from Axios response if available
    let errorMsg = 'Unknown error'
    if (err && typeof err === 'object' && 'response' in err) {
      const axiosErr = err as { response?: { data?: { error?: string; message?: string } }; message?: string }
      errorMsg = axiosErr.response?.data?.error || axiosErr.response?.data?.message || axiosErr.message || 'Request failed'
    } else if (err instanceof Error) {
      errorMsg = err.message
    }
    response.value = {
      success: false,
      error: errorMsg,
    }
  } finally {
    loading.value = false
  }
}

function clearHistory(): void {
  history.value = []
}

function formatJson(data: unknown): string {
  return JSON.stringify(data, null, 2)
}

function stripEventType(evt: Record<string, unknown>): Record<string, unknown> {
  const { event_type: _, ...rest } = evt
  return rest
}

onMounted(() => {
  pipelinesStore.fetchGroups()
})
</script>

<template>
  <v-row>
    <!-- Inject Panel -->
    <v-col cols="12" md="6">
      <v-card>
        <v-card-title>
          <v-icon class="mr-2">mdi-flask</v-icon>
          Inject Test Event
        </v-card-title>

        <v-card-text>
          <v-alert
            v-if="groups.length === 0"
            type="info"
            variant="tonal"
            class="mb-4"
          >
            <div class="font-weight-medium">No pipeline groups available</div>
            <div class="text-body-2 mt-1">
              A pipeline group is a collection of related pipelines deployed together.
              Go to the <strong>Pipelines</strong> page to deploy one first.
            </div>
          </v-alert>

          <v-select
            v-model="selectedGroupId"
            label="Pipeline Group"
            :items="groups.map(g => ({ title: g.name, value: g.id }))"
            :disabled="groups.length === 0"
            hint="Select the pipeline group to receive the test event"
            persistent-hint
            class="mb-4"
          />

          <v-text-field
            v-model="eventType"
            label="Event Type"
            placeholder="UserLogin"
            class="mb-4"
          />

          <v-textarea
            v-model="eventDataJson"
            label="Event Data (JSON)"
            :error="!jsonValid && eventDataJson.length > 0"
            :error-messages="!jsonValid && eventDataJson.length > 0 ? 'Invalid JSON' : ''"
            rows="8"
            class="font-monospace"
          />
        </v-card-text>

        <v-card-actions>
          <v-spacer />
          <v-btn
            color="primary"
            :loading="loading"
            :disabled="!selectedGroupId || !jsonValid || !eventType"
            @click="injectEvent"
          >
            <v-icon start>mdi-send</v-icon>
            Inject Event
          </v-btn>
        </v-card-actions>
      </v-card>

      <!-- Response Panel -->
      <v-card v-if="response" class="mt-4">
        <v-card-title>
          <v-icon
            class="mr-2"
            :color="response.success ? 'success' : 'error'"
          >
            {{ response.success ? 'mdi-check-circle' : 'mdi-alert-circle' }}
          </v-icon>
          Response
        </v-card-title>

        <v-card-text>
          <v-alert
            :type="response.success ? 'success' : 'error'"
            variant="tonal"
          >
            {{ response.success ? 'Event accepted' : response.error }}
          </v-alert>

          <div v-if="response.routed_to" class="mt-3 d-flex align-center ga-2">
            <span class="text-subtitle-2">Routed to:</span>
            <v-chip size="small" color="primary" variant="tonal">
              {{ response.routed_to }}
            </v-chip>
            <v-chip v-if="response.worker_id" size="small" variant="outlined">
              {{ response.worker_id }}
            </v-chip>
          </div>

          <div v-if="response.output_events && response.output_events.length > 0" class="mt-3">
            <div class="text-subtitle-2 mb-2">Output Events ({{ response.output_events.length }}):</div>
            <v-card
              v-for="(evt, idx) in response.output_events"
              :key="idx"
              variant="outlined"
              class="mb-2"
            >
              <v-card-subtitle v-if="evt.event_type" class="pt-2 pb-0 text-caption font-weight-medium">
                {{ evt.event_type }}
              </v-card-subtitle>
              <v-card-text class="pa-2">
                <pre class="font-monospace text-caption">{{ formatJson(stripEventType(evt)) }}</pre>
              </v-card-text>
            </v-card>
          </div>

          <div v-else-if="response.success" class="mt-3 text-medium-emphasis text-body-2">
            No output events emitted (event may be buffered in a window)
          </div>
        </v-card-text>
      </v-card>
    </v-col>

    <!-- Event Log Panel -->
    <v-col cols="12" md="6">
      <v-card class="h-100">
        <v-card-title class="d-flex align-center">
          <v-icon class="mr-2">mdi-history</v-icon>
          Event Log
          <v-badge
            v-if="eventLog.length > 0"
            :content="eventLog.length"
            color="primary"
            inline
            class="ml-2"
          />
          <v-spacer />
          <v-btn
            v-if="history.length > 0"
            icon
            size="small"
            variant="text"
            @click="clearHistory"
          >
            <v-icon>mdi-delete</v-icon>
            <v-tooltip activator="parent" location="top">Clear history</v-tooltip>
          </v-btn>
        </v-card-title>

        <v-card-text class="event-log-container">
          <v-tabs density="compact" class="mb-2">
            <v-tab>Injected ({{ history.length }})</v-tab>
            <v-tab>Output ({{ eventLog.length }})</v-tab>
          </v-tabs>

          <!-- Injection History -->
          <v-list v-if="history.length > 0" density="compact" class="py-0">
            <v-list-item
              v-for="entry in history"
              :key="entry.id"
              lines="two"
            >
              <template #prepend>
                <v-icon
                  :color="entry.response.success ? 'success' : 'error'"
                  size="small"
                >
                  {{ entry.response.success ? 'mdi-check' : 'mdi-close' }}
                </v-icon>
              </template>

              <v-list-item-title class="text-body-2 font-weight-medium">
                {{ entry.eventType }}
              </v-list-item-title>

              <v-list-item-subtitle class="text-caption">
                {{ entry.timestamp.toLocaleTimeString() }}
              </v-list-item-subtitle>

              <template #append>
                <v-dialog max-width="500">
                  <template #activator="{ props: dialogProps }">
                    <v-btn
                      icon
                      size="small"
                      variant="text"
                      v-bind="dialogProps"
                    >
                      <v-icon>mdi-eye</v-icon>
                    </v-btn>
                  </template>
                  <v-card>
                    <v-card-title>Event Details</v-card-title>
                    <v-card-text>
                      <pre class="font-monospace text-caption">{{ formatJson(entry.data) }}</pre>
                    </v-card-text>
                  </v-card>
                </v-dialog>
              </template>
            </v-list-item>
          </v-list>

          <div v-else class="text-center text-medium-emphasis pa-8">
            <v-icon size="48" class="mb-2">mdi-inbox-outline</v-icon>
            <div>No events injected yet</div>
            <div class="text-caption">Inject an event to see it here</div>
          </div>
        </v-card-text>
      </v-card>
    </v-col>
  </v-row>
</template>

<style scoped>
.event-log-container {
  max-height: 600px;
  overflow-y: auto;
}
</style>
