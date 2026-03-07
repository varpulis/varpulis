<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { useWebSocketStore } from '@/stores/websocket'
import { usePipelinesStore } from '@/stores/pipelines'
import { getApiKey } from '@/api'
import { deployPipelineGroup } from '@/api/cluster'

const emit = defineEmits<{ dismiss: [] }>()

const router = useRouter()
const wsStore = useWebSocketStore()
const pipelinesStore = usePipelinesStore()

const step = ref(1)
const deploying = ref(false)
const deployError = ref<string | null>(null)
const deploySuccess = ref(false)

const SAMPLE_VPL = `connector = stdin()

stream HighTemp = Temperature as t
    .where(t.value > 100)
    .within(30s)
    .emit(alert: "High temperature detected", value: t.value)`

const isConnected = computed(() => wsStore.connected || !!getApiKey() || !!localStorage.getItem('varpulis_token'))
const hasPipelines = computed(() => pipelinesStore.groups.length > 0)

const checklist = computed(() => [
  { label: 'Connected to coordinator', done: isConnected.value, icon: 'mdi-server-network' },
  { label: 'Pipeline deployed', done: hasPipelines.value || deploySuccess.value, icon: 'mdi-pipe' },
])

async function deploySample() {
  deploying.value = true
  deployError.value = null
  try {
    await deployPipelineGroup({
      name: 'getting-started',
      pipelines: [{ name: 'getting-started', source: SAMPLE_VPL }],
    })
    deploySuccess.value = true
    await pipelinesStore.fetchGroups()
  } catch (e: unknown) {
    deployError.value = e instanceof Error ? e.message : 'Deploy failed'
  } finally {
    deploying.value = false
  }
}

function goToSettings() {
  completeOnboarding()
  router.push('/settings')
}

function goToEditor() {
  completeOnboarding()
  router.push('/editor')
}

function completeOnboarding() {
  localStorage.setItem('varpulis_onboarding_complete', '1')
  emit('dismiss')
}

onMounted(async () => {
  await pipelinesStore.fetchGroups()
})
</script>

<template>
  <v-dialog :model-value="true" max-width="640" persistent>
    <v-card>
      <v-card-title class="text-h5 pa-6 pb-2">
        <v-icon class="mr-2" color="primary">mdi-rocket-launch</v-icon>
        Welcome to Varpulis
      </v-card-title>

      <v-stepper v-model="step" flat alt-labels>
        <v-stepper-header>
          <v-stepper-item :value="1" title="Welcome" />
          <v-divider />
          <v-stepper-item :value="2" title="Connect" />
          <v-divider />
          <v-stepper-item :value="3" title="Deploy" />
          <v-divider />
          <v-stepper-item :value="4" title="Ready" />
        </v-stepper-header>

        <v-stepper-window>
          <!-- Step 1: Welcome -->
          <v-stepper-window-item :value="1">
            <div class="pa-4 text-center">
              <v-icon size="64" color="primary" class="mb-4">mdi-shield-check</v-icon>
              <h3 class="text-h6 mb-2">Real-time Complex Event Processing</h3>
              <p class="text-body-1 text-medium-emphasis mb-4">
                Varpulis detects patterns across streaming data in real-time.
                Let's get you set up in a few quick steps.
              </p>
              <div class="d-flex justify-center gap-4 mb-2">
                <div class="text-center">
                  <v-icon color="info" size="32">mdi-code-braces</v-icon>
                  <div class="text-caption mt-1">Write VPL</div>
                </div>
                <div class="text-center">
                  <v-icon color="success" size="32">mdi-pipe</v-icon>
                  <div class="text-caption mt-1">Deploy Pipelines</div>
                </div>
                <div class="text-center">
                  <v-icon color="warning" size="32">mdi-bell-ring</v-icon>
                  <div class="text-caption mt-1">Get Alerts</div>
                </div>
              </div>
            </div>
          </v-stepper-window-item>

          <!-- Step 2: Connect -->
          <v-stepper-window-item :value="2">
            <div class="pa-4">
              <div class="d-flex align-center mb-3">
                <v-icon :color="isConnected ? 'success' : 'warning'" class="mr-2">
                  {{ isConnected ? 'mdi-check-circle' : 'mdi-alert-circle' }}
                </v-icon>
                <span class="text-body-1">
                  {{ isConnected ? 'Connected to coordinator' : 'Not connected yet' }}
                </span>
              </div>

              <v-alert v-if="isConnected" type="success" variant="tonal" class="mb-3">
                Your connection is active. You can proceed to deploy a pipeline.
              </v-alert>

              <v-alert v-else type="info" variant="tonal" class="mb-3">
                Configure your coordinator connection in Settings to get started.
                If running locally, the default proxy handles this automatically.
              </v-alert>

              <v-btn
                v-if="!isConnected"
                color="primary"
                variant="outlined"
                prepend-icon="mdi-cog"
                @click="goToSettings"
              >
                Open Settings
              </v-btn>
            </div>
          </v-stepper-window-item>

          <!-- Step 3: Deploy -->
          <v-stepper-window-item :value="3">
            <div class="pa-4">
              <p class="text-body-2 text-medium-emphasis mb-3">
                Deploy a sample pipeline to verify everything works.
                You can remove it later or edit it in the VPL Editor.
              </p>

              <v-sheet rounded="lg" class="pa-3 mb-3 bg-surface-light">
                <code class="text-body-2" style="white-space: pre-wrap">{{ SAMPLE_VPL }}</code>
              </v-sheet>

              <v-alert v-if="deployError" type="error" variant="tonal" class="mb-3" closable @click:close="deployError = null">
                {{ deployError }}
              </v-alert>

              <v-alert v-if="deploySuccess" type="success" variant="tonal" class="mb-3">
                Pipeline "getting-started" deployed successfully!
              </v-alert>

              <div class="d-flex gap-2">
                <v-btn
                  v-if="!deploySuccess && !hasPipelines"
                  color="primary"
                  :loading="deploying"
                  prepend-icon="mdi-rocket-launch"
                  @click="deploySample"
                >
                  Deploy Sample
                </v-btn>
                <v-btn
                  v-if="hasPipelines && !deploySuccess"
                  color="success"
                  variant="tonal"
                  prepend-icon="mdi-check"
                  disabled
                >
                  You already have pipelines
                </v-btn>
              </div>
            </div>
          </v-stepper-window-item>

          <!-- Step 4: Ready -->
          <v-stepper-window-item :value="4">
            <div class="pa-4 text-center">
              <v-icon size="64" color="success" class="mb-4">mdi-check-decagram</v-icon>
              <h3 class="text-h6 mb-4">You're all set!</h3>

              <v-list density="compact" class="text-left mx-auto" style="max-width: 320px">
                <v-list-item
                  v-for="item in checklist"
                  :key="item.label"
                  :prepend-icon="item.done ? 'mdi-check-circle' : 'mdi-circle-outline'"
                  :class="item.done ? 'text-success' : 'text-medium-emphasis'"
                >
                  <v-list-item-title>{{ item.label }}</v-list-item-title>
                </v-list-item>
              </v-list>

              <div class="d-flex justify-center gap-2 mt-4">
                <v-btn color="primary" variant="outlined" prepend-icon="mdi-code-braces" @click="goToEditor">
                  Open Editor
                </v-btn>
                <v-btn color="primary" prepend-icon="mdi-view-dashboard" @click="completeOnboarding">
                  Go to Dashboard
                </v-btn>
              </div>
            </div>
          </v-stepper-window-item>
        </v-stepper-window>
      </v-stepper>

      <v-card-actions class="px-6 pb-4">
        <v-btn v-if="step > 1" variant="text" @click="step--">Back</v-btn>
        <v-spacer />
        <v-btn variant="text" color="medium-emphasis" @click="completeOnboarding">Skip</v-btn>
        <v-btn v-if="step < 4" color="primary" @click="step++">
          {{ step === 1 ? 'Get Started' : 'Next' }}
        </v-btn>
      </v-card-actions>
    </v-card>
  </v-dialog>
</template>
