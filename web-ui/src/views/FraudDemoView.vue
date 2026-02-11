<script setup lang="ts">
import { ref } from 'vue'
import DemoShell from '@/components/demos/DemoShell.vue'
import AttackChainGraph from '@/components/demos/fraud/AttackChainGraph.vue'
import { fraudDetectionScenario } from '@/data/scenarios/fraud-detection'

const activeEvents = ref<string[]>([])
const alertTypes = ref<string[]>([])

const detectedAlerts = ref<Array<{
  type: string
  title: string
  fields: Record<string, unknown>
  timestamp: number
}>>([])

// Map step index to the event types that step injects
const stepEventTypes: Record<number, string[]> = {
  0: ['Login', 'Purchase'],
  1: ['Login', 'PasswordChange', 'Purchase'],
  2: ['SmallPurchase', 'LargePurchase'],
  3: ['Login'],
}

function onStepChanged(index: number) {
  // Accumulate active event types from all steps up to and including the current one
  const allEvents = new Set<string>()
  for (let i = 0; i <= index; i++) {
    const events = stepEventTypes[i]
    if (events) {
      events.forEach((e) => allEvents.add(e))
    }
  }
  activeEvents.value = Array.from(allEvents)
}

function onAlerts(alerts: Array<Record<string, unknown>>) {
  for (const alert of alerts) {
    const alertType = String(alert.alert_type || alert.event_type || 'unknown')

    // Track alert type for graph highlighting
    if (!alertTypes.value.includes(alertType)) {
      alertTypes.value.push(alertType)
    }

    // Add to detected alerts list
    detectedAlerts.value.unshift({
      type: alertType,
      title: formatAlertTitle(alertType),
      fields: alert,
      timestamp: Date.now(),
    })
  }
}

function formatAlertTitle(alertType: string): string {
  switch (alertType) {
    case 'account_takeover':
      return 'Account Takeover Detected'
    case 'card_testing':
      return 'Card Testing Detected'
    case 'impossible_travel':
      return 'Impossible Travel Detected'
    default:
      return alertType.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())
  }
}

function alertColor(alertType: string): string {
  switch (alertType) {
    case 'account_takeover':
      return '#FF5252'
    case 'card_testing':
      return '#FFC107'
    case 'impossible_travel':
      return '#7C4DFF'
    default:
      return '#FF5252'
  }
}

function alertIcon(alertType: string): string {
  switch (alertType) {
    case 'account_takeover':
      return 'mdi-account-alert'
    case 'card_testing':
      return 'mdi-credit-card-search'
    case 'impossible_travel':
      return 'mdi-airplane-alert'
    default:
      return 'mdi-alert-circle'
  }
}

function formatTimestamp(ts: number): string {
  return new Date(ts).toLocaleTimeString()
}
</script>

<template>
  <DemoShell
    :scenario="fraudDetectionScenario"
    @alerts="onAlerts"
    @step-changed="onStepChanged"
  >
    <template #hero>
      <v-row class="h-100 ma-0">
        <!-- Left: Attack Chain Graph -->
        <v-col cols="12" md="8" class="pa-0" style="min-height: 500px">
          <AttackChainGraph
            :active-events="activeEvents"
            :alert-types="alertTypes"
          />
        </v-col>

        <!-- Right: Detected Alerts List -->
        <v-col cols="12" md="4" class="pa-0 pl-md-3">
          <v-card
            color="rgba(0, 0, 0, 0.5)"
            variant="flat"
            class="h-100"
            style="backdrop-filter: blur(8px); border: 1px solid rgba(255, 255, 255, 0.08)"
          >
            <v-card-title class="d-flex align-center text-white">
              <v-icon class="mr-2" color="error">mdi-shield-alert</v-icon>
              Detected Alerts
              <v-spacer />
              <v-chip
                v-if="detectedAlerts.length > 0"
                color="error"
                size="small"
                variant="flat"
              >
                {{ detectedAlerts.length }}
              </v-chip>
            </v-card-title>

            <v-card-text class="pa-0" style="overflow-y: auto; max-height: 420px">
              <v-list
                v-if="detectedAlerts.length > 0"
                bg-color="transparent"
                density="comfortable"
              >
                <TransitionGroup name="alert-slide">
                  <v-list-item
                    v-for="(alert, index) in detectedAlerts"
                    :key="`${alert.type}-${alert.timestamp}-${index}`"
                    class="alert-list-item mb-1"
                  >
                    <template #prepend>
                      <v-icon :color="alertColor(alert.type)" size="28">
                        {{ alertIcon(alert.type) }}
                      </v-icon>
                    </template>

                    <v-list-item-title class="text-white font-weight-medium text-body-2">
                      {{ alert.title }}
                    </v-list-item-title>

                    <v-list-item-subtitle class="text-medium-emphasis">
                      <span class="text-caption">{{ formatTimestamp(alert.timestamp) }}</span>
                      <template v-if="alert.fields.user_id">
                        <span class="mx-1">|</span>
                        <span class="text-caption">User: {{ alert.fields.user_id }}</span>
                      </template>
                      <template v-if="alert.fields.card_id">
                        <span class="mx-1">|</span>
                        <span class="text-caption">Card: {{ alert.fields.card_id }}</span>
                      </template>
                    </v-list-item-subtitle>

                    <template #append>
                      <v-chip
                        :color="alertColor(alert.type)"
                        size="x-small"
                        variant="tonal"
                        label
                      >
                        {{ alert.type }}
                      </v-chip>
                    </template>
                  </v-list-item>
                </TransitionGroup>
              </v-list>

              <div
                v-else
                class="d-flex flex-column align-center justify-center text-medium-emphasis pa-8"
              >
                <v-icon size="48" class="mb-3" color="grey-darken-1">
                  mdi-shield-check-outline
                </v-icon>
                <div class="text-body-2">No alerts yet</div>
                <div class="text-caption mt-1">Inject events to trigger detection</div>
              </div>
            </v-card-text>
          </v-card>
        </v-col>
      </v-row>
    </template>
  </DemoShell>
</template>

<style scoped>
.alert-list-item {
  border-left: 3px solid transparent;
  transition: border-color 0.3s ease;
}

.alert-list-item:hover {
  background: rgba(255, 255, 255, 0.03);
}

.alert-slide-enter-active {
  transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
}

.alert-slide-leave-active {
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
}

.alert-slide-enter-from {
  opacity: 0;
  transform: translateX(30px);
}

.alert-slide-leave-to {
  opacity: 0;
  transform: translateX(-10px);
}
</style>
