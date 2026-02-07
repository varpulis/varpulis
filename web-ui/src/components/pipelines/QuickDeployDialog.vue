<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { usePipelinesStore } from '@/stores/pipelines'
import { useVplSourcesStore } from '@/stores/vplSources'

const props = defineProps<{
  modelValue: boolean
  initialSourceId?: string | null
}>()

const emit = defineEmits<{
  'update:modelValue': [value: boolean]
}>()

const pipelinesStore = usePipelinesStore()
const vplSourcesStore = useVplSourcesStore()

const dialogOpen = computed({
  get: () => props.modelValue,
  set: (val) => emit('update:modelValue', val),
})

const savedSources = computed(() => vplSourcesStore.sortedSources)

// Form state
const selectedSourceId = ref<string | null>(props.initialSourceId || null)
const pipelineName = ref('')
const deploying = ref(false)
const error = ref<string | null>(null)

// Parse events from selected source
const parsedEvents = computed(() => {
  if (!selectedSourceId.value) return { inputs: [], outputs: [] }
  const source = vplSourcesStore.getSource(selectedSourceId.value)
  if (!source) return { inputs: [], outputs: [] }
  return vplSourcesStore.parseEvents(source.source)
})

const selectedSource = computed(() => {
  if (!selectedSourceId.value) return null
  return vplSourcesStore.getSource(selectedSourceId.value)
})

// Auto-generate pipeline name from source name
watch(selectedSourceId, (newId) => {
  if (newId) {
    const source = vplSourcesStore.getSource(newId)
    if (source) {
      // Convert source name to valid pipeline name
      pipelineName.value = source.name
        .toLowerCase()
        .replace(/[^a-z0-9-]/g, '-')
        .replace(/-+/g, '-')
        .replace(/^-|-$/g, '')
    }
  }
})

// Reset when dialog opens
watch(dialogOpen, (open) => {
  if (open) {
    selectedSourceId.value = props.initialSourceId || null
    pipelineName.value = ''
    error.value = null
  }
})

const canDeploy = computed(() => {
  return selectedSourceId.value && pipelineName.value && /^[a-z0-9-]+$/.test(pipelineName.value)
})

async function deploy(): Promise<void> {
  if (!selectedSourceId.value || !pipelineName.value) return

  const source = vplSourcesStore.getSource(selectedSourceId.value)
  if (!source) return

  deploying.value = true
  error.value = null

  try {
    // Create a single-pipeline group with the same name as the pipeline
    pipelinesStore.deployForm.name = pipelineName.value
    pipelinesStore.deployForm.pipelines = [{
      name: pipelineName.value,
      vpl_source: source.source,
      affinityType: 'any',
      affinityValue: '',
    }]
    pipelinesStore.deployForm.routes = []

    await pipelinesStore.deploy()

    if (!pipelinesStore.error) {
      dialogOpen.value = false
    } else {
      error.value = pipelinesStore.error
    }
  } catch (e) {
    error.value = e instanceof Error ? e.message : 'Deployment failed'
  } finally {
    deploying.value = false
  }
}

function close(): void {
  dialogOpen.value = false
}
</script>

<template>
  <v-dialog
    v-model="dialogOpen"
    max-width="500"
  >
    <v-card>
      <v-card-title class="d-flex align-center">
        <v-icon start>mdi-rocket-launch</v-icon>
        Quick Deploy
      </v-card-title>

      <v-card-text>
        <!-- No saved sources -->
        <v-alert v-if="savedSources.length === 0" type="info" variant="tonal" class="mb-4">
          <div class="font-weight-medium">No saved VPL sources</div>
          <div class="text-body-2 mt-1">
            Go to the <strong>Editor</strong> page to create and save a VPL source first.
          </div>
        </v-alert>

        <template v-else>
          <!-- Source Selection -->
          <v-select
            v-model="selectedSourceId"
            label="Select VPL Source"
            :items="savedSources.map(s => ({ title: s.name, value: s.id, subtitle: s.description }))"
            item-title="title"
            item-value="value"
            class="mb-4"
          >
            <template #item="{ item, props: itemProps }">
              <v-list-item v-bind="itemProps">
                <v-list-item-subtitle v-if="item.raw.subtitle">
                  {{ item.raw.subtitle }}
                </v-list-item-subtitle>
              </v-list-item>
            </template>
          </v-select>

          <!-- Pipeline Name -->
          <v-text-field
            v-model="pipelineName"
            label="Pipeline Name"
            placeholder="my-pipeline"
            :rules="[
              v => !!v || 'Name is required',
              v => /^[a-z0-9-]+$/.test(v) || 'Lowercase alphanumeric with hyphens only',
            ]"
            hint="This will be used as both the pipeline and group name"
            persistent-hint
            class="mb-4"
          />

          <!-- Events Preview -->
          <div v-if="selectedSource" class="mb-4">
            <div class="text-subtitle-2 mb-2">Events</div>
            <div class="d-flex gap-2">
              <v-chip size="small" color="success" variant="tonal">
                <v-icon start size="small">mdi-import</v-icon>
                {{ parsedEvents.inputs.length }} input
              </v-chip>
              <v-chip size="small" color="warning" variant="tonal">
                <v-icon start size="small">mdi-export</v-icon>
                {{ parsedEvents.outputs.length }} output
              </v-chip>
            </div>

            <!-- Event details (collapsed) -->
            <v-expansion-panels v-if="parsedEvents.inputs.length > 0 || parsedEvents.outputs.length > 0" class="mt-2">
              <v-expansion-panel>
                <v-expansion-panel-title class="text-body-2 py-2">
                  View event details
                </v-expansion-panel-title>
                <v-expansion-panel-text>
                  <div v-if="parsedEvents.inputs.length > 0" class="mb-3">
                    <div class="text-caption text-success font-weight-medium mb-1">Input Events</div>
                    <div v-for="event in parsedEvents.inputs" :key="event.name" class="ml-2">
                      <span class="font-weight-medium">{{ event.name }}</span>
                      <span class="text-caption text-medium-emphasis ml-1">
                        ({{ event.fields.map(f => f.name).join(', ') }})
                      </span>
                    </div>
                  </div>
                  <div v-if="parsedEvents.outputs.length > 0">
                    <div class="text-caption text-warning font-weight-medium mb-1">Output Events</div>
                    <div v-for="event in parsedEvents.outputs" :key="event.name" class="ml-2">
                      <span class="font-weight-medium">{{ event.name }}</span>
                    </div>
                  </div>
                </v-expansion-panel-text>
              </v-expansion-panel>
            </v-expansion-panels>
          </div>
        </template>

        <!-- Error -->
        <v-alert v-if="error" type="error" variant="tonal" class="mt-4">
          {{ error }}
        </v-alert>
      </v-card-text>

      <v-card-actions>
        <v-spacer />
        <v-btn variant="text" @click="close">Cancel</v-btn>
        <v-btn
          color="primary"
          :loading="deploying"
          :disabled="!canDeploy"
          @click="deploy"
        >
          Deploy
        </v-btn>
      </v-card-actions>
    </v-card>
  </v-dialog>
</template>
