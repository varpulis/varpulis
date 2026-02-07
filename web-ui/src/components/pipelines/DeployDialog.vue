<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { usePipelinesStore } from '@/stores/pipelines'
import { useVplSourcesStore, type ParsedEvents } from '@/stores/vplSources'

const pipelinesStore = usePipelinesStore()
const vplSourcesStore = useVplSourcesStore()

// Source selection types
type SourceType = 'saved' | 'file' | 'template'

interface PipelineFormState {
  name: string
  sourceType: SourceType
  savedSourceId: string | null
  fileContent: string | null
  fileName: string | null
  templateIndex: number | null
  affinityType: 'any' | 'worker_id' | 'label'
  affinityValue: string
  parsedEvents: ParsedEvents
  showInputEvents: boolean
  showOutputEvents: boolean
}

// VPL templates
const vplTemplates = [
  {
    name: 'Simple Filter',
    description: 'Filter events based on a condition',
    code: `event InputEvent:
    user_id: str
    action: str
    timestamp: str

stream Input from InputEvent

stream Filtered = Input
    .where(action == "login")
    .emit(
        event_type: "FilteredEvent",
        user_id: user_id,
        action: action
    )
`,
  },
  {
    name: 'Aggregation Window',
    description: 'Aggregate metrics over a time window',
    code: `event MetricEvent:
    value: float
    source: str
    timestamp: str

stream Metrics from MetricEvent

stream Aggregated = Metrics
    .window(60, sliding: 10)
    .aggregate(
        avg_value: avg(value),
        max_value: max(value),
        count: count()
    )
    .emit(
        event_type: "AggregatedMetric",
        average: avg_value,
        maximum: max_value,
        sample_count: count
    )
`,
  },
  {
    name: 'Pattern Detection',
    description: 'Detect a sequence of events',
    code: `event UserAction:
    user_id: str
    action: str
    timestamp: str

stream Actions from UserAction

stream LoginLogout = Actions
    .pattern(
        SEQ(
            a: action == "login",
            b: action == "logout"
        )
        within 300
    )
    .emit(
        event_type: "SessionEvent",
        user_id: a.user_id,
        session_duration: b.timestamp - a.timestamp
    )
`,
  },
]

function createDefaultPipelineState(): PipelineFormState {
  return {
    name: '',
    sourceType: 'saved',
    savedSourceId: null,
    fileContent: null,
    fileName: null,
    templateIndex: null,
    affinityType: 'any',
    affinityValue: '',
    parsedEvents: { inputs: [], outputs: [] },
    showInputEvents: true,
    showOutputEvents: true,
  }
}

const pipelines = ref<PipelineFormState[]>([createDefaultPipelineState()])

const dialogOpen = computed({
  get: () => pipelinesStore.deployDialogOpen,
  set: (val) => {
    if (!val) pipelinesStore.closeDeployDialog()
  },
})

const form = computed(() => pipelinesStore.deployForm)
const deploying = computed(() => pipelinesStore.deploying)
const error = computed(() => pipelinesStore.error)
const savedSources = computed(() => vplSourcesStore.sortedSources)

const step = ref(1)

const nameRules = [
  (v: string) => !!v || 'Name is required',
  (v: string) => /^[a-z0-9-]+$/.test(v) || 'Name must be lowercase alphanumeric with hyphens',
  (v: string) => v.length <= 63 || 'Name must be 63 characters or less',
]

const pipelineNameRules = [
  (v: string) => !!v || 'Pipeline name is required',
  (v: string) => /^[a-z0-9-]+$/.test(v) || 'Name must be lowercase alphanumeric with hyphens',
]

// Get the VPL source for a pipeline
function getVplSource(pipeline: PipelineFormState): string {
  switch (pipeline.sourceType) {
    case 'saved':
      if (pipeline.savedSourceId) {
        const source = vplSourcesStore.getSource(pipeline.savedSourceId)
        return source?.source || ''
      }
      return ''
    case 'file':
      return pipeline.fileContent || ''
    case 'template':
      if (pipeline.templateIndex !== null && vplTemplates[pipeline.templateIndex]) {
        return vplTemplates[pipeline.templateIndex].code
      }
      return ''
    default:
      return ''
  }
}

// Update parsed events when source changes
function updateParsedEvents(pipeline: PipelineFormState): void {
  const source = getVplSource(pipeline)
  pipeline.parsedEvents = vplSourcesStore.parseEvents(source)
}

// Handle file upload
async function handleFileUpload(pipeline: PipelineFormState, event: Event): Promise<void> {
  const input = event.target as HTMLInputElement
  const file = input.files?.[0]
  if (!file) return

  pipeline.fileName = file.name
  pipeline.fileContent = await file.text()
  updateParsedEvents(pipeline)
}

// Watch for source changes
watch(
  () => pipelines.value.map(p => ({ type: p.sourceType, saved: p.savedSourceId, template: p.templateIndex })),
  () => {
    pipelines.value.forEach(updateParsedEvents)
  },
  { deep: true }
)

function addPipeline(): void {
  pipelines.value.push(createDefaultPipelineState())
}

function removePipeline(index: number): void {
  if (pipelines.value.length > 1) {
    pipelines.value.splice(index, 1)
  }
}

function addRoute(): void {
  pipelinesStore.addRoute()
}

function removeRoute(index: number): void {
  pipelinesStore.removeRoute(index)
}

async function deploy(): Promise<void> {
  // Transfer pipeline data to store
  pipelinesStore.deployForm.pipelines = pipelines.value.map(p => ({
    name: p.name,
    vpl_source: getVplSource(p),
    affinityType: p.affinityType,
    affinityValue: p.affinityValue,
  }))

  await pipelinesStore.deploy()

  if (!pipelinesStore.error) {
    // Reset local state
    pipelines.value = [createDefaultPipelineState()]
    step.value = 1
  }
}

function close(): void {
  pipelinesStore.closeDeployDialog()
  pipelines.value = [createDefaultPipelineState()]
  step.value = 1
}

function nextStep(): void {
  if (step.value < 3) {
    step.value++
  }
}

function prevStep(): void {
  if (step.value > 1) {
    step.value--
  }
}

// Check if a pipeline has a valid source selected
function hasValidSource(pipeline: PipelineFormState): boolean {
  switch (pipeline.sourceType) {
    case 'saved':
      return !!pipeline.savedSourceId
    case 'file':
      return !!pipeline.fileContent
    case 'template':
      return pipeline.templateIndex !== null
    default:
      return false
  }
}

// Check if all pipelines are valid for deployment
const canDeploy = computed(() => {
  return (
    form.value.name &&
    pipelines.value.length > 0 &&
    pipelines.value.every(p => p.name && hasValidSource(p))
  )
})
</script>

<template>
  <v-dialog
    v-model="dialogOpen"
    max-width="900"
    persistent
  >
    <v-card>
      <v-toolbar flat density="comfortable" color="primary">
        <v-toolbar-title>Deploy Pipeline Group</v-toolbar-title>
        <v-spacer />
        <v-btn icon @click="close">
          <v-icon>mdi-close</v-icon>
        </v-btn>
      </v-toolbar>

      <v-stepper v-model="step" flat>
        <v-stepper-header>
          <v-stepper-item :value="1" title="Basic Info" />
          <v-divider />
          <v-stepper-item :value="2" title="Pipelines" />
          <v-divider />
          <v-stepper-item :value="3" title="Routes" />
        </v-stepper-header>

        <v-stepper-window>
          <!-- Step 1: Basic Info -->
          <v-stepper-window-item :value="1">
            <v-card-text>
              <v-text-field
                v-model="form.name"
                label="Group Name"
                placeholder="my-pipeline-group"
                :rules="nameRules"
                hint="Lowercase alphanumeric with hyphens"
                persistent-hint
              />
            </v-card-text>
          </v-stepper-window-item>

          <!-- Step 2: Pipelines -->
          <v-stepper-window-item :value="2">
            <v-card-text>
              <div class="d-flex align-center mb-4">
                <span class="text-subtitle-1">Pipelines</span>
                <v-spacer />
                <v-btn
                  variant="outlined"
                  size="small"
                  prepend-icon="mdi-plus"
                  @click="addPipeline"
                >
                  Add Pipeline
                </v-btn>
              </div>

              <v-card
                v-for="(pipeline, index) in pipelines"
                :key="index"
                variant="outlined"
                class="mb-4"
              >
                <v-card-text>
                  <div class="d-flex align-center mb-3">
                    <span class="text-subtitle-2">Pipeline {{ index + 1 }}</span>
                    <v-spacer />
                    <v-btn
                      v-if="pipelines.length > 1"
                      icon
                      size="small"
                      variant="text"
                      color="error"
                      @click="removePipeline(index)"
                    >
                      <v-icon>mdi-delete</v-icon>
                    </v-btn>
                  </div>

                  <v-row>
                    <v-col cols="12" md="6">
                      <v-text-field
                        v-model="pipeline.name"
                        label="Pipeline Name"
                        :rules="pipelineNameRules"
                        density="compact"
                      />
                    </v-col>
                    <v-col cols="12" md="6">
                      <v-select
                        v-model="pipeline.affinityType"
                        label="Worker Affinity"
                        :items="[
                          { title: 'Any Worker', value: 'any' },
                          { title: 'Specific Worker', value: 'worker_id' },
                          { title: 'By Label', value: 'label' },
                        ]"
                        density="compact"
                      />
                    </v-col>
                  </v-row>

                  <v-text-field
                    v-if="pipeline.affinityType !== 'any'"
                    v-model="pipeline.affinityValue"
                    :label="pipeline.affinityType === 'worker_id' ? 'Worker ID' : 'Label'"
                    density="compact"
                    class="mb-3"
                  />

                  <!-- Source Selection -->
                  <div class="text-body-2 text-medium-emphasis mb-2">VPL Source</div>

                  <v-btn-toggle
                    v-model="pipeline.sourceType"
                    mandatory
                    density="compact"
                    class="mb-3"
                    color="primary"
                  >
                    <v-btn value="saved" size="small">
                      <v-icon start>mdi-content-save</v-icon>
                      Saved
                      <v-badge
                        v-if="savedSources.length > 0"
                        :content="savedSources.length"
                        color="primary"
                        inline
                        class="ml-1"
                      />
                    </v-btn>
                    <v-btn value="file" size="small">
                      <v-icon start>mdi-file-upload</v-icon>
                      File
                    </v-btn>
                    <v-btn value="template" size="small">
                      <v-icon start>mdi-file-document</v-icon>
                      Template
                    </v-btn>
                  </v-btn-toggle>

                  <!-- Saved Sources -->
                  <v-select
                    v-if="pipeline.sourceType === 'saved'"
                    v-model="pipeline.savedSourceId"
                    label="Select Saved Source"
                    :items="savedSources.map(s => ({ title: s.name, value: s.id, subtitle: s.description }))"
                    item-title="title"
                    item-value="value"
                    density="compact"
                    :hint="savedSources.length === 0 ? 'No saved sources. Save a VPL in the Editor first.' : ''"
                    persistent-hint
                    clearable
                  >
                    <template #item="{ item, props: itemProps }">
                      <v-list-item v-bind="itemProps">
                        <v-list-item-subtitle v-if="item.raw.subtitle">
                          {{ item.raw.subtitle }}
                        </v-list-item-subtitle>
                      </v-list-item>
                    </template>
                  </v-select>

                  <!-- File Upload -->
                  <div v-if="pipeline.sourceType === 'file'">
                    <v-file-input
                      label="Upload VPL File"
                      accept=".vpl,.txt"
                      density="compact"
                      prepend-icon="mdi-file-upload"
                      :hint="pipeline.fileName ? `Selected: ${pipeline.fileName}` : ''"
                      persistent-hint
                      @change="handleFileUpload(pipeline, $event)"
                    />
                  </div>

                  <!-- Templates -->
                  <v-select
                    v-if="pipeline.sourceType === 'template'"
                    v-model="pipeline.templateIndex"
                    label="Select Template"
                    :items="vplTemplates.map((t, i) => ({ title: t.name, value: i, subtitle: t.description }))"
                    item-title="title"
                    item-value="value"
                    density="compact"
                    clearable
                  >
                    <template #item="{ item, props: itemProps }">
                      <v-list-item v-bind="itemProps">
                        <v-list-item-subtitle>{{ item.raw.subtitle }}</v-list-item-subtitle>
                      </v-list-item>
                    </template>
                  </v-select>

                  <!-- Events Preview -->
                  <div v-if="hasValidSource(pipeline)" class="mt-4">
                    <!-- Input Events -->
                    <v-card
                      v-if="pipeline.parsedEvents.inputs.length > 0"
                      variant="tonal"
                      color="success"
                      class="mb-2"
                    >
                      <v-card-title
                        class="d-flex align-center py-2 cursor-pointer"
                        @click="pipeline.showInputEvents = !pipeline.showInputEvents"
                      >
                        <v-icon start size="small">mdi-import</v-icon>
                        <span class="text-body-2">
                          Input Events ({{ pipeline.parsedEvents.inputs.length }})
                        </span>
                        <v-spacer />
                        <v-icon>
                          {{ pipeline.showInputEvents ? 'mdi-chevron-up' : 'mdi-chevron-down' }}
                        </v-icon>
                      </v-card-title>
                      <v-expand-transition>
                        <v-card-text v-show="pipeline.showInputEvents" class="pt-0">
                          <div
                            v-for="event in pipeline.parsedEvents.inputs"
                            :key="event.name"
                            class="mb-2"
                          >
                            <div class="font-weight-medium">{{ event.name }}</div>
                            <div
                              v-for="field in event.fields"
                              :key="field.name"
                              class="text-caption text-medium-emphasis ml-4"
                            >
                              {{ field.name }}: <span class="text-primary">{{ field.type }}</span>
                            </div>
                          </div>
                        </v-card-text>
                      </v-expand-transition>
                    </v-card>

                    <!-- Output Events -->
                    <v-card
                      v-if="pipeline.parsedEvents.outputs.length > 0"
                      variant="tonal"
                      color="warning"
                    >
                      <v-card-title
                        class="d-flex align-center py-2 cursor-pointer"
                        @click="pipeline.showOutputEvents = !pipeline.showOutputEvents"
                      >
                        <v-icon start size="small">mdi-export</v-icon>
                        <span class="text-body-2">
                          Output Events ({{ pipeline.parsedEvents.outputs.length }})
                        </span>
                        <v-spacer />
                        <v-icon>
                          {{ pipeline.showOutputEvents ? 'mdi-chevron-up' : 'mdi-chevron-down' }}
                        </v-icon>
                      </v-card-title>
                      <v-expand-transition>
                        <v-card-text v-show="pipeline.showOutputEvents" class="pt-0">
                          <div
                            v-for="event in pipeline.parsedEvents.outputs"
                            :key="event.name"
                            class="mb-2"
                          >
                            <div class="font-weight-medium">{{ event.name }}</div>
                            <div
                              v-for="field in event.fields"
                              :key="field.name"
                              class="text-caption text-medium-emphasis ml-4"
                            >
                              {{ field.name }}: <span class="text-primary">{{ field.type }}</span>
                            </div>
                            <div
                              v-if="event.fields.length === 0"
                              class="text-caption text-medium-emphasis ml-4 font-italic"
                            >
                              (fields inferred from emit)
                            </div>
                          </div>
                        </v-card-text>
                      </v-expand-transition>
                    </v-card>

                    <!-- No events warning -->
                    <v-alert
                      v-if="pipeline.parsedEvents.inputs.length === 0 && pipeline.parsedEvents.outputs.length === 0"
                      type="warning"
                      variant="tonal"
                      density="compact"
                    >
                      No events detected. Make sure your VPL defines event types and uses .emit()
                    </v-alert>
                  </div>

                  <!-- No source selected -->
                  <v-alert
                    v-else
                    type="info"
                    variant="tonal"
                    density="compact"
                    class="mt-3"
                  >
                    Select a VPL source to see input and output events
                  </v-alert>
                </v-card-text>
              </v-card>
            </v-card-text>
          </v-stepper-window-item>

          <!-- Step 3: Routes -->
          <v-stepper-window-item :value="3">
            <v-card-text>
              <div class="d-flex align-center mb-4">
                <span class="text-subtitle-1">Routes (Optional)</span>
                <v-spacer />
                <v-btn
                  variant="outlined"
                  size="small"
                  prepend-icon="mdi-plus"
                  @click="addRoute"
                >
                  Add Route
                </v-btn>
              </div>

              <div v-if="form.routes.length === 0" class="text-center pa-4 text-medium-emphasis">
                <v-icon size="48" class="mb-2">mdi-arrow-decision</v-icon>
                <div>No routes configured</div>
                <div class="text-caption">Routes define how events flow between pipelines</div>
              </div>

              <v-card
                v-for="(route, index) in form.routes"
                :key="index"
                variant="outlined"
                class="mb-4"
              >
                <v-card-text>
                  <div class="d-flex align-center mb-2">
                    <span class="text-subtitle-2">Route {{ index + 1 }}</span>
                    <v-spacer />
                    <v-btn
                      icon
                      size="small"
                      variant="text"
                      color="error"
                      @click="removeRoute(index)"
                    >
                      <v-icon>mdi-delete</v-icon>
                    </v-btn>
                  </div>

                  <v-row>
                    <v-col cols="12" md="5">
                      <v-select
                        v-model="route.source"
                        label="Source Pipeline"
                        :items="pipelines.map(p => p.name).filter(n => n)"
                        density="compact"
                      />
                    </v-col>
                    <v-col cols="12" md="2" class="d-flex justify-center align-center">
                      <v-icon>mdi-arrow-right</v-icon>
                    </v-col>
                    <v-col cols="12" md="5">
                      <v-select
                        v-model="route.target"
                        label="Target Pipeline"
                        :items="pipelines.map(p => p.name).filter(n => n)"
                        density="compact"
                      />
                    </v-col>
                  </v-row>

                  <v-text-field
                    v-model="route.eventTypes"
                    label="Event Types (comma-separated)"
                    placeholder="UserLogin, UserLogout"
                    density="compact"
                    hint="Leave empty to route all events"
                    persistent-hint
                  />
                </v-card-text>
              </v-card>

              <!-- Summary -->
              <v-alert type="info" variant="tonal" class="mt-4">
                <div class="text-subtitle-2">Deployment Summary</div>
                <ul class="text-body-2 mt-2">
                  <li>Group Name: <strong>{{ form.name || '(not set)' }}</strong></li>
                  <li>Pipelines: <strong>{{ pipelines.filter(p => p.name && hasValidSource(p)).length }}</strong></li>
                  <li>Routes: <strong>{{ form.routes.length }}</strong></li>
                </ul>
              </v-alert>
            </v-card-text>
          </v-stepper-window-item>
        </v-stepper-window>
      </v-stepper>

      <!-- Error -->
      <v-alert v-if="error" type="error" class="mx-4 mb-4">
        {{ error }}
      </v-alert>

      <v-card-actions>
        <v-btn
          v-if="step > 1"
          variant="text"
          @click="prevStep"
        >
          Back
        </v-btn>
        <v-spacer />
        <v-btn
          variant="text"
          @click="close"
        >
          Cancel
        </v-btn>
        <v-btn
          v-if="step < 3"
          color="primary"
          :disabled="step === 1 && !form.name"
          @click="nextStep"
        >
          Next
        </v-btn>
        <v-btn
          v-else
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

<style scoped>
.cursor-pointer {
  cursor: pointer;
}
</style>
