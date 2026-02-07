<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { usePipelinesStore } from '@/stores/pipelines'
import { useVplSourcesStore } from '@/stores/vplSources'
import PipelineList from '@/components/pipelines/PipelineList.vue'
import PipelineCard from '@/components/pipelines/PipelineCard.vue'
import QuickDeployDialog from '@/components/pipelines/QuickDeployDialog.vue'
import ConfirmDialog from '@/components/common/ConfirmDialog.vue'

const pipelinesStore = usePipelinesStore()
const vplSourcesStore = useVplSourcesStore()

// Dialog states
const quickDeployOpen = ref(false)
const confirmDialog = ref(false)
const teardownLoading = ref(false)
const groupToTeardown = ref<string | null>(null)

// Advanced mode - inline group builder
const showGroupBuilder = ref(false)
const groupName = ref('')
const selectedSourceIds = ref<string[]>([])
const groupDeploying = ref(false)
const groupError = ref<string | null>(null)

let pollInterval: ReturnType<typeof setInterval> | null = null

const groups = computed(() => pipelinesStore.groups)
const selectedGroup = computed(() => pipelinesStore.selectedGroup)
const loading = computed(() => pipelinesStore.loading)
const error = computed(() => pipelinesStore.error)
const savedSources = computed(() => vplSourcesStore.sortedSources)

// Check if group name is valid
const isGroupNameValid = computed(() => {
  return groupName.value && /^[a-z0-9-]+$/.test(groupName.value)
})

// Check if we can deploy the group
const canDeployGroup = computed(() => {
  return isGroupNameValid.value && selectedSourceIds.value.length > 0
})

function openQuickDeploy(): void {
  quickDeployOpen.value = true
}

function toggleGroupBuilder(): void {
  showGroupBuilder.value = !showGroupBuilder.value
  if (showGroupBuilder.value) {
    // Reset state
    groupName.value = ''
    selectedSourceIds.value = []
    groupError.value = null
  }
}

function selectGroup(groupId: string | null): void {
  if (groupId) {
    pipelinesStore.fetchGroupDetail(groupId)
  } else {
    pipelinesStore.selectGroup(null)
  }
}

function confirmTeardown(groupId: string): void {
  groupToTeardown.value = groupId
  confirmDialog.value = true
}

async function executeTeardown(): Promise<void> {
  if (!groupToTeardown.value) return

  teardownLoading.value = true
  try {
    await pipelinesStore.teardown(groupToTeardown.value)
    confirmDialog.value = false
    groupToTeardown.value = null
  } catch {
    // Error handled by store
  } finally {
    teardownLoading.value = false
  }
}

async function reloadGroup(groupId: string): Promise<void> {
  await pipelinesStore.reload(groupId)
}

async function deployGroup(): Promise<void> {
  if (!canDeployGroup.value) return

  groupDeploying.value = true
  groupError.value = null

  try {
    // Build pipelines from selected sources
    const pipelines = selectedSourceIds.value.map(id => {
      const source = vplSourcesStore.getSource(id)
      if (!source) throw new Error(`Source ${id} not found`)

      // Generate pipeline name from source name
      const pipelineName = source.name
        .toLowerCase()
        .replace(/[^a-z0-9-]/g, '-')
        .replace(/-+/g, '-')
        .replace(/^-|-$/g, '')

      return {
        name: pipelineName,
        vpl_source: source.source,
        affinityType: 'any' as const,
        affinityValue: '',
      }
    })

    pipelinesStore.deployForm.name = groupName.value
    pipelinesStore.deployForm.pipelines = pipelines
    pipelinesStore.deployForm.routes = []

    await pipelinesStore.deploy()

    if (!pipelinesStore.error) {
      // Success - reset and close builder
      showGroupBuilder.value = false
      groupName.value = ''
      selectedSourceIds.value = []
    } else {
      groupError.value = pipelinesStore.error
    }
  } catch (e) {
    groupError.value = e instanceof Error ? e.message : 'Deployment failed'
  } finally {
    groupDeploying.value = false
  }
}

async function fetchData(): Promise<void> {
  await pipelinesStore.fetchGroups()
}

function getSourceName(id: string): string {
  const source = vplSourcesStore.getSource(id)
  return source?.name || id
}

function getSourceEvents(id: string): { inputs: number; outputs: number } {
  const source = vplSourcesStore.getSource(id)
  if (!source) return { inputs: 0, outputs: 0 }
  const parsed = vplSourcesStore.parseEvents(source.source)
  return { inputs: parsed.inputs.length, outputs: parsed.outputs.length }
}

onMounted(() => {
  fetchData()
  pollInterval = setInterval(fetchData, 5000)
})

onUnmounted(() => {
  if (pollInterval) {
    clearInterval(pollInterval)
  }
})
</script>

<template>
  <div>
    <div class="d-flex align-center mb-4">
      <h1 class="text-h4">Pipeline Groups</h1>
      <v-spacer />
      <v-btn-group variant="outlined" density="compact">
        <v-btn color="primary" @click="openQuickDeploy">
          <v-icon start>mdi-rocket-launch</v-icon>
          Quick Deploy
        </v-btn>
        <v-btn @click="toggleGroupBuilder">
          <v-icon start>{{ showGroupBuilder ? 'mdi-close' : 'mdi-plus' }}</v-icon>
          {{ showGroupBuilder ? 'Cancel' : 'Create Group' }}
        </v-btn>
      </v-btn-group>
    </div>

    <!-- Error Alert -->
    <v-alert
      v-if="error"
      type="error"
      closable
      class="mb-4"
      @click:close="pipelinesStore.clearError"
    >
      {{ error }}
    </v-alert>

    <!-- Group Builder (inline, not popup) -->
    <v-expand-transition>
      <v-card v-if="showGroupBuilder" class="mb-4" variant="outlined">
        <v-card-title class="d-flex align-center">
          <v-icon start>mdi-layers-plus</v-icon>
          Create Pipeline Group
        </v-card-title>

        <v-card-text>
          <v-row>
            <v-col cols="12" md="4">
              <v-text-field
                v-model="groupName"
                label="Group Name"
                placeholder="my-group"
                :rules="[
                  v => !!v || 'Required',
                  v => /^[a-z0-9-]+$/.test(v) || 'Lowercase alphanumeric with hyphens',
                ]"
                density="compact"
              />
            </v-col>
            <v-col cols="12" md="8">
              <div class="text-subtitle-2 mb-2">Select VPL Sources to Include</div>
              <v-alert v-if="savedSources.length === 0" type="info" variant="tonal" density="compact">
                No saved VPL sources. Go to the Editor to create some first.
              </v-alert>
              <div v-else class="d-flex flex-wrap gap-2">
                <v-chip
                  v-for="source in savedSources"
                  :key="source.id"
                  :color="selectedSourceIds.includes(source.id) ? 'primary' : undefined"
                  :variant="selectedSourceIds.includes(source.id) ? 'flat' : 'outlined'"
                  @click="
                    selectedSourceIds.includes(source.id)
                      ? selectedSourceIds = selectedSourceIds.filter(id => id !== source.id)
                      : selectedSourceIds.push(source.id)
                  "
                >
                  <v-icon start size="small">
                    {{ selectedSourceIds.includes(source.id) ? 'mdi-check' : 'mdi-file-code' }}
                  </v-icon>
                  {{ source.name }}
                </v-chip>
              </div>
            </v-col>
          </v-row>

          <!-- Selected Sources Preview -->
          <div v-if="selectedSourceIds.length > 0" class="mt-4">
            <div class="text-subtitle-2 mb-2">
              Pipelines to Deploy ({{ selectedSourceIds.length }})
            </div>
            <v-table density="compact">
              <thead>
                <tr>
                  <th>Pipeline Name</th>
                  <th>Source</th>
                  <th>Events</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="id in selectedSourceIds" :key="id">
                  <td class="font-monospace">
                    {{ getSourceName(id).toLowerCase().replace(/[^a-z0-9-]/g, '-') }}
                  </td>
                  <td>{{ getSourceName(id) }}</td>
                  <td>
                    <v-chip size="x-small" color="success" variant="tonal" class="mr-1">
                      {{ getSourceEvents(id).inputs }} in
                    </v-chip>
                    <v-chip size="x-small" color="warning" variant="tonal">
                      {{ getSourceEvents(id).outputs }} out
                    </v-chip>
                  </td>
                  <td>
                    <v-btn
                      icon
                      size="x-small"
                      variant="text"
                      @click="selectedSourceIds = selectedSourceIds.filter(i => i !== id)"
                    >
                      <v-icon size="small">mdi-close</v-icon>
                    </v-btn>
                  </td>
                </tr>
              </tbody>
            </v-table>
          </div>

          <!-- Error -->
          <v-alert v-if="groupError" type="error" variant="tonal" class="mt-4" density="compact">
            {{ groupError }}
          </v-alert>
        </v-card-text>

        <v-card-actions>
          <v-spacer />
          <v-btn variant="text" @click="showGroupBuilder = false">Cancel</v-btn>
          <v-btn
            color="primary"
            :loading="groupDeploying"
            :disabled="!canDeployGroup"
            @click="deployGroup"
          >
            <v-icon start>mdi-rocket-launch</v-icon>
            Deploy Group
          </v-btn>
        </v-card-actions>
      </v-card>
    </v-expand-transition>

    <!-- Summary Stats -->
    <v-row class="mb-4">
      <v-col cols="12" sm="6" md="3">
        <v-card>
          <v-card-text class="d-flex align-center">
            <v-avatar color="primary" class="mr-3">
              <v-icon>mdi-pipe</v-icon>
            </v-avatar>
            <div>
              <div class="text-h5">{{ groups.length }}</div>
              <div class="text-caption text-medium-emphasis">Total Groups</div>
            </div>
          </v-card-text>
        </v-card>
      </v-col>
      <v-col cols="12" sm="6" md="3">
        <v-card>
          <v-card-text class="d-flex align-center">
            <v-avatar color="success" class="mr-3">
              <v-icon>mdi-play-circle</v-icon>
            </v-avatar>
            <div>
              <div class="text-h5">{{ pipelinesStore.runningGroups.length }}</div>
              <div class="text-caption text-medium-emphasis">Running</div>
            </div>
          </v-card-text>
        </v-card>
      </v-col>
      <v-col cols="12" sm="6" md="3">
        <v-card>
          <v-card-text class="d-flex align-center">
            <v-avatar color="error" class="mr-3">
              <v-icon>mdi-alert-circle</v-icon>
            </v-avatar>
            <div>
              <div class="text-h5">{{ pipelinesStore.failedGroups.length }}</div>
              <div class="text-caption text-medium-emphasis">Failed</div>
            </div>
          </v-card-text>
        </v-card>
      </v-col>
      <v-col cols="12" sm="6" md="3">
        <v-card>
          <v-card-text class="d-flex align-center">
            <v-avatar color="info" class="mr-3">
              <v-icon>mdi-file-code</v-icon>
            </v-avatar>
            <div>
              <div class="text-h5">{{ savedSources.length }}</div>
              <div class="text-caption text-medium-emphasis">Saved Sources</div>
            </div>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <!-- Main Content -->
    <v-row>
      <!-- Pipeline List -->
      <v-col cols="12" :md="selectedGroup ? 6 : 12">
        <PipelineList
          :groups="groups"
          :selected-id="selectedGroup?.id || null"
          :loading="loading"
          @select="selectGroup"
          @teardown="confirmTeardown"
        />
      </v-col>

      <!-- Pipeline Detail Panel -->
      <v-col v-if="selectedGroup" cols="12" md="6">
        <PipelineCard
          :group="selectedGroup"
          @close="selectGroup(null)"
          @teardown="confirmTeardown(selectedGroup.id)"
          @reload="reloadGroup(selectedGroup.id)"
        />
      </v-col>
    </v-row>

    <!-- Quick Deploy Dialog -->
    <QuickDeployDialog v-model="quickDeployOpen" />

    <!-- Confirm Teardown Dialog -->
    <ConfirmDialog
      v-model="confirmDialog"
      title="Teardown Pipeline Group"
      message="Are you sure you want to teardown this pipeline group? All pipelines will be stopped and removed from workers."
      confirm-text="Teardown"
      confirm-color="error"
      :loading="teardownLoading"
      @confirm="executeTeardown"
    />
  </div>
</template>
