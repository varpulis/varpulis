<script setup lang="ts">
import { computed } from 'vue'
import { useRouter } from 'vue-router'
import type { PipelineGroup } from '@/types/pipeline'
import StatusChip from '@/components/common/StatusChip.vue'

const props = defineProps<{
  group: PipelineGroup
}>()

const emit = defineEmits<{
  close: []
  teardown: []
}>()

const router = useRouter()

// Get the first pipeline source (most groups have a single pipeline)
const firstPipelineName = computed(() => {
  if (!props.group.sources) return null
  const names = Object.keys(props.group.sources)
  return names.length > 0 ? names[0] : null
})

function editInEditor(pipelineName: string): void {
  router.push({
    name: 'editor',
    query: { groupId: props.group.id, pipeline: pipelineName },
  })
}
</script>

<template>
  <v-card>
    <v-toolbar flat density="compact">
      <v-toolbar-title>{{ group.name }}</v-toolbar-title>
      <v-spacer />
      <v-btn icon @click="emit('close')">
        <v-icon>mdi-close</v-icon>
      </v-btn>
    </v-toolbar>

    <v-card-text>
      <!-- Status and Info -->
      <div class="d-flex align-center mb-4">
        <StatusChip :status="group.status" />
        <v-spacer />
        <span class="text-caption text-medium-emphasis">
          {{ group.pipeline_count }} pipeline(s)
        </span>
      </div>

      <!-- Placements -->
      <div class="text-subtitle-2 mb-2">
        Placements ({{ group.placements?.length || 0 }})
      </div>

      <v-card v-if="group.placements && group.placements.length > 0" variant="outlined" class="mb-4">
        <v-list density="compact" class="py-0">
          <v-list-item
            v-for="placement in group.placements"
            :key="placement.pipeline_id"
            lines="two"
          >
            <template #prepend>
              <v-icon size="small" :color="placement.status === 'Running' ? 'success' : 'grey'">
                mdi-pipe-leak
              </v-icon>
            </template>

            <v-list-item-title class="text-body-2">
              {{ placement.pipeline_name }}
            </v-list-item-title>

            <v-list-item-subtitle class="text-caption">
              Worker: {{ placement.worker_id.substring(0, 8) }}...
              <br />
              {{ placement.worker_address }}
            </v-list-item-subtitle>

            <template #append>
              <v-btn
                v-if="group.sources?.[placement.pipeline_name]"
                icon
                size="x-small"
                variant="text"
                class="mr-1"
                @click="editInEditor(placement.pipeline_name)"
              >
                <v-icon size="small">mdi-pencil</v-icon>
                <v-tooltip activator="parent" location="top">Edit in Editor</v-tooltip>
              </v-btn>
              <StatusChip :status="placement.status" size="x-small" />
            </template>
          </v-list-item>
        </v-list>
      </v-card>

      <v-alert v-else type="info" variant="tonal" density="compact" class="mb-4">
        No pipelines deployed yet
      </v-alert>

      <!-- Details -->
      <v-expansion-panels variant="accordion">
        <v-expansion-panel>
          <v-expansion-panel-title class="text-body-2">
            <v-icon size="small" class="mr-2">mdi-information-outline</v-icon>
            Details
          </v-expansion-panel-title>
          <v-expansion-panel-text>
            <v-list density="compact">
              <v-list-item>
                <v-list-item-title class="text-caption">ID</v-list-item-title>
                <v-list-item-subtitle class="font-monospace text-wrap">
                  {{ group.id }}
                </v-list-item-subtitle>
              </v-list-item>
              <v-list-item>
                <v-list-item-title class="text-caption">Status</v-list-item-title>
                <v-list-item-subtitle>{{ group.status }}</v-list-item-subtitle>
              </v-list-item>
            </v-list>
          </v-expansion-panel-text>
        </v-expansion-panel>
      </v-expansion-panels>
    </v-card-text>

    <v-card-actions>
      <v-btn
        v-if="firstPipelineName"
        variant="outlined"
        prepend-icon="mdi-pencil"
        @click="editInEditor(firstPipelineName)"
      >
        Edit in Editor
      </v-btn>
      <v-spacer />
      <v-btn
        color="error"
        variant="outlined"
        prepend-icon="mdi-delete"
        @click="emit('teardown')"
      >
        Teardown
      </v-btn>
    </v-card-actions>
  </v-card>
</template>
