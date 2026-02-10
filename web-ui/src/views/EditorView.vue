<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import VplEditor from '@/components/editor/VplEditor.vue'
import EventTester from '@/components/editor/EventTester.vue'
import QuickDeployDialog from '@/components/pipelines/QuickDeployDialog.vue'
import { useVplSourcesStore, type SavedVplSource } from '@/stores/vplSources'
import { getPipelineGroup } from '@/api/cluster'

const route = useRoute()
const vplSourcesStore = useVplSourcesStore()

// Deploy dialog
const deployDialogOpen = ref(false)
const currentSourceId = ref<string | null>(null)

const activeTab = ref('editor')
const vplEditorRef = ref<InstanceType<typeof VplEditor> | null>(null)

// Save dialog state
const saveDialogOpen = ref(false)
const saveName = ref('')
const saveDescription = ref('')

// Load dialog state
const loadDialogOpen = ref(false)
const selectedSourceId = ref<string | null>(null)

// Current source tracking
const currentSourceName = ref<string | null>(null)


const vplSource = ref(`# Varpulis Pipeline Language (VPL)
# Define your event processing pipeline here

# Event type definitions
event UserLogin:
    user_id: str
    timestamp: str
    ip_address: str

event Alert:
    alert_type: str
    user_id: str
    message: str

# Input stream from UserLogin events
stream Logins = UserLogin

# Filter for suspicious logins (multiple IPs)
stream SuspiciousLogins = Logins
    .window(5, sliding: 1)
    .aggregate(
        user_id: last(user_id),
        unique_ips: count_distinct(ip_address),
        last_ip: last(ip_address)
    )
    .where(unique_ips > 1)
    .emit(
        event_type: "Alert",
        alert_type: "suspicious_login",
        user_id: user_id,
        message: "Multiple IP addresses detected"
    )
`)

const outputPanel = ref<string[]>([])
const showOutput = ref(true)
const validationStatus = ref<'idle' | 'validating' | 'valid' | 'invalid'>('idle')
const lastValidationErrors = ref<string[]>([])

const savedSources = computed(() => vplSourcesStore.sortedSources)
const parsedEvents = computed(() => vplSourcesStore.parseEvents(vplSource.value))

// Track validation state from editor
const isValidating = computed(() => vplEditorRef.value?.isValidating || false)

function handleEditorChange(source: string): void {
  vplSource.value = source
}

function addOutput(message: string): void {
  outputPanel.value.push(`[${new Date().toLocaleTimeString()}] ${message}`)
  // Keep last 100 lines
  if (outputPanel.value.length > 100) {
    outputPanel.value = outputPanel.value.slice(-100)
  }
}

function clearOutput(): void {
  outputPanel.value = []
}

function handleValidate(result: { valid: boolean; errors?: string[]; isAuto?: boolean }): void {
  if (result.valid) {
    validationStatus.value = 'valid'
    lastValidationErrors.value = []
    // Only show output for manual validation (button click)
    if (!result.isAuto) {
      addOutput('✓ Validation successful - no errors found')
    }
  } else {
    validationStatus.value = 'invalid'
    lastValidationErrors.value = result.errors || []
    // Only show output for manual validation (button click)
    if (!result.isAuto) {
      result.errors?.forEach((err) => addOutput(`✗ Error: ${err}`))
    }
  }
}

function triggerValidation(): void {
  if (vplEditorRef.value) {
    vplEditorRef.value.validate()
  }
}

// Save dialog functions
function openSaveDialog(): void {
  saveName.value = currentSourceName.value || ''
  saveDescription.value = ''
  saveDialogOpen.value = true
}

function handleSave(): void {
  if (!saveName.value.trim()) return

  const saved = vplSourcesStore.saveSource(saveName.value.trim(), vplSource.value, saveDescription.value || undefined)
  currentSourceName.value = saveName.value.trim()
  currentSourceId.value = saved.id
  addOutput(`Source saved as "${saveName.value}"`)
  saveDialogOpen.value = false

  // Also save to localStorage for quick access
  localStorage.setItem('varpulis_vpl_source', vplSource.value)
}

function quickSave(): void {
  if (currentSourceName.value) {
    const saved = vplSourcesStore.saveSource(currentSourceName.value, vplSource.value)
    currentSourceId.value = saved.id
    addOutput(`Source saved as "${currentSourceName.value}"`)
    localStorage.setItem('varpulis_vpl_source', vplSource.value)
  } else {
    openSaveDialog()
  }
}

function openDeployDialog(): void {
  // Save first if not saved
  if (!currentSourceId.value && currentSourceName.value) {
    const saved = vplSourcesStore.saveSource(currentSourceName.value, vplSource.value)
    currentSourceId.value = saved.id
  }
  deployDialogOpen.value = true
}

// Load dialog functions
function openLoadDialog(): void {
  selectedSourceId.value = null
  loadDialogOpen.value = true
}

function handleLoad(): void {
  if (!selectedSourceId.value) return

  const source = vplSourcesStore.getSource(selectedSourceId.value)
  if (source) {
    vplSource.value = source.source
    currentSourceName.value = source.name
    currentSourceId.value = source.id
    addOutput(`Loaded "${source.name}"`)
    localStorage.setItem('varpulis_vpl_source', source.source)
  }
  loadDialogOpen.value = false
}

function loadSource(source: SavedVplSource): void {
  vplSource.value = source.source
  currentSourceName.value = source.name
  currentSourceId.value = source.id
  addOutput(`Loaded "${source.name}"`)
  localStorage.setItem('varpulis_vpl_source', source.source)
  loadDialogOpen.value = false
}

function deleteSource(id: string): void {
  vplSourcesStore.deleteSource(id)
  if (selectedSourceId.value === id) {
    selectedSourceId.value = null
  }
}

function newSource(): void {
  currentSourceName.value = null
  currentSourceId.value = null
  vplSource.value = `# New VPL Pipeline
# Define your event processing pipeline here

event MyEvent:
    field1: str
    field2: int

stream Input = MyEvent

stream Output = Input
    .where(field2 > 0)
    .emit(
        event_type: "ProcessedEvent",
        value: field1
    )
`
  addOutput('Created new source')
}

function formatDate(dateStr: string): string {
  return new Date(dateStr).toLocaleString()
}

// Load pipeline source from route query (e.g., /editor?groupId=xxx&pipeline=name)
onMounted(async () => {
  const groupId = route.query.groupId as string | undefined
  const pipelineName = route.query.pipeline as string | undefined

  if (groupId && pipelineName) {
    try {
      const group = await getPipelineGroup(groupId)
      const source = group.sources?.[pipelineName]
      if (source) {
        vplSource.value = source
        currentSourceName.value = `${group.name}/${pipelineName}`
        addOutput(`Loaded pipeline "${pipelineName}" from group "${group.name}"`)
        localStorage.setItem('varpulis_vpl_source', source)
        return
      }
    } catch {
      // Fall through to localStorage
    }
  }

  // Load last saved source
  const savedSource = localStorage.getItem('varpulis_vpl_source')
  if (savedSource) {
    vplSource.value = savedSource
  }
})
</script>

<template>
  <div class="editor-view">
    <div class="d-flex align-center mb-4">
      <h1 class="text-h4">VPL Editor</h1>
      <v-chip v-if="currentSourceName" size="small" class="ml-3" color="primary" variant="outlined">
        {{ currentSourceName }}
      </v-chip>
      <v-chip v-else size="small" class="ml-3" color="grey" variant="outlined">
        Unsaved
      </v-chip>
      <v-spacer />
      <v-btn-group variant="outlined" density="compact">
        <v-btn @click="newSource">
          <v-icon start>mdi-file-plus</v-icon>
          New
        </v-btn>
        <v-btn @click="openLoadDialog">
          <v-icon start>mdi-folder-open</v-icon>
          Open
          <v-badge
            v-if="savedSources.length > 0"
            :content="savedSources.length"
            color="primary"
            floating
          />
        </v-btn>
        <v-btn @click="quickSave">
          <v-icon start>mdi-content-save</v-icon>
          Save
        </v-btn>
        <v-btn @click="openSaveDialog">
          <v-icon start>mdi-content-save-edit</v-icon>
          Save As
        </v-btn>
      </v-btn-group>
    </div>

    <v-tabs v-model="activeTab" class="mb-4">
      <v-tab value="editor">
        <v-icon start>mdi-code-braces</v-icon>
        Editor
      </v-tab>
      <v-tab value="tester">
        <v-icon start>mdi-flask</v-icon>
        Event Tester
      </v-tab>
    </v-tabs>

    <v-window v-model="activeTab">
      <!-- Editor Tab -->
      <v-window-item value="editor">
        <v-row>
          <v-col :cols="showOutput ? 8 : 12">
            <v-card class="editor-card">
              <VplEditor
                ref="vplEditorRef"
                :model-value="vplSource"
                :height="500"
                @update:model-value="handleEditorChange"
                @validate="handleValidate"
              />
            </v-card>
          </v-col>

          <v-col v-if="showOutput" cols="4">
            <v-card class="h-100">
              <v-toolbar flat density="compact">
                <v-toolbar-title class="text-body-1">Output</v-toolbar-title>
                <v-spacer />
                <v-btn icon size="small" @click="clearOutput">
                  <v-icon>mdi-delete</v-icon>
                  <v-tooltip activator="parent" location="top">Clear</v-tooltip>
                </v-btn>
                <v-btn icon size="small" @click="showOutput = false">
                  <v-icon>mdi-close</v-icon>
                </v-btn>
              </v-toolbar>

              <v-card-text class="output-panel pa-2">
                <div v-if="outputPanel.length === 0" class="text-center text-medium-emphasis pa-4">
                  <v-icon size="32">mdi-console</v-icon>
                  <div class="text-caption mt-2">Output will appear here</div>
                </div>
                <div
                  v-for="(line, index) in outputPanel"
                  :key="index"
                  class="output-line font-monospace text-caption"
                >
                  {{ line }}
                </div>
              </v-card-text>
            </v-card>
          </v-col>
        </v-row>

        <!-- Action Bar -->
        <v-card class="mt-4">
          <v-card-text class="d-flex align-center gap-2">
            <v-btn
              color="primary"
              prepend-icon="mdi-check"
              :loading="isValidating"
              @click="triggerValidation"
            >
              Validate
            </v-btn>

            <!-- Validation Status -->
            <v-chip
              v-if="validationStatus === 'valid'"
              size="small"
              color="success"
              variant="flat"
            >
              <v-icon start size="small">mdi-check-circle</v-icon>
              Valid
            </v-chip>
            <v-chip
              v-else-if="validationStatus === 'invalid'"
              size="small"
              color="error"
              variant="flat"
            >
              <v-icon start size="small">mdi-alert-circle</v-icon>
              {{ lastValidationErrors.length }} error{{ lastValidationErrors.length !== 1 ? 's' : '' }}
            </v-chip>

            <v-divider vertical class="mx-2" />

            <v-btn
              variant="outlined"
              prepend-icon="mdi-rocket-launch"
              :disabled="(!currentSourceId && !currentSourceName) || validationStatus === 'invalid'"
              @click="openDeployDialog"
            >
              Deploy
              <v-tooltip activator="parent" location="top">
                <div v-if="!currentSourceId && !currentSourceName">
                  Save your VPL source first before deploying
                </div>
                <div v-else-if="validationStatus === 'invalid'">
                  Fix validation errors before deploying
                </div>
                <div v-else>
                  Deploy this pipeline to the cluster
                </div>
              </v-tooltip>
            </v-btn>

            <!-- Events Preview -->
            <v-divider vertical class="mx-2" />
            <v-chip size="small" color="success" variant="tonal">
              <v-icon start size="small">mdi-import</v-icon>
              {{ parsedEvents.inputs.length }} input event{{ parsedEvents.inputs.length !== 1 ? 's' : '' }}
            </v-chip>
            <v-chip size="small" color="warning" variant="tonal">
              <v-icon start size="small">mdi-export</v-icon>
              {{ parsedEvents.outputs.length }} output event{{ parsedEvents.outputs.length !== 1 ? 's' : '' }}
            </v-chip>

            <v-spacer />
            <v-btn
              v-if="!showOutput"
              variant="text"
              prepend-icon="mdi-console"
              @click="showOutput = true"
            >
              Show Output
            </v-btn>
          </v-card-text>
        </v-card>
      </v-window-item>

      <!-- Event Tester Tab -->
      <v-window-item value="tester">
        <EventTester />
      </v-window-item>
    </v-window>

    <!-- Save Dialog -->
    <v-dialog v-model="saveDialogOpen" max-width="500">
      <v-card>
        <v-card-title>Save VPL Source</v-card-title>
        <v-card-text>
          <v-text-field
            v-model="saveName"
            label="Name"
            placeholder="my-pipeline"
            hint="A unique name for this VPL source"
            persistent-hint
            autofocus
            class="mb-4"
          />
          <v-textarea
            v-model="saveDescription"
            label="Description (optional)"
            placeholder="Describe what this pipeline does..."
            rows="2"
          />
        </v-card-text>
        <v-card-actions>
          <v-spacer />
          <v-btn variant="text" @click="saveDialogOpen = false">Cancel</v-btn>
          <v-btn color="primary" :disabled="!saveName.trim()" @click="handleSave">Save</v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>

    <!-- Load Dialog -->
    <v-dialog v-model="loadDialogOpen" max-width="600">
      <v-card>
        <v-card-title>Open VPL Source</v-card-title>
        <v-card-text>
          <v-alert v-if="savedSources.length === 0" type="info" variant="tonal">
            No saved sources yet. Use "Save As" to save your first VPL source.
          </v-alert>

          <v-list v-else density="compact" class="sources-list">
            <v-list-item
              v-for="source in savedSources"
              :key="source.id"
              :active="selectedSourceId === source.id"
              @click="selectedSourceId = source.id"
              @dblclick="loadSource(source)"
            >
              <template #prepend>
                <v-icon>mdi-file-code</v-icon>
              </template>

              <v-list-item-title class="font-weight-medium">
                {{ source.name }}
              </v-list-item-title>

              <v-list-item-subtitle>
                <span class="text-caption">{{ formatDate(source.savedAt) }}</span>
                <span v-if="source.description" class="text-caption ml-2">
                  - {{ source.description }}
                </span>
              </v-list-item-subtitle>

              <template #append>
                <v-btn
                  icon
                  size="small"
                  variant="text"
                  color="error"
                  @click.stop="deleteSource(source.id)"
                >
                  <v-icon>mdi-delete</v-icon>
                  <v-tooltip activator="parent" location="top">Delete</v-tooltip>
                </v-btn>
              </template>
            </v-list-item>
          </v-list>
        </v-card-text>
        <v-card-actions>
          <v-spacer />
          <v-btn variant="text" @click="loadDialogOpen = false">Cancel</v-btn>
          <v-btn
            color="primary"
            :disabled="!selectedSourceId"
            @click="handleLoad"
          >
            Open
          </v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>

    <!-- Deploy Dialog -->
    <QuickDeployDialog
      v-model="deployDialogOpen"
      :initial-source-id="currentSourceId"
    />
  </div>
</template>

<style scoped>
.editor-view {
  height: 100%;
}

.editor-card {
  overflow: hidden;
}

.output-panel {
  height: 458px;
  overflow-y: auto;
  background: rgb(var(--v-theme-surface-variant));
}

.output-line {
  padding: 2px 4px;
  border-bottom: 1px solid rgba(var(--v-border-color), 0.1);
}

.sources-list {
  max-height: 400px;
  overflow-y: auto;
}
</style>
