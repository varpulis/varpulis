<script setup lang="ts">
import type { PipelineGroup } from '@/types/pipeline'
import StatusChip from '@/components/common/StatusChip.vue'

defineProps<{
  groups: PipelineGroup[]
  selectedId: string | null
  loading: boolean
}>()

const emit = defineEmits<{
  select: [groupId: string | null]
  teardown: [groupId: string]
}>()

function getStatusColor(status: string): string {
  switch (status) {
    case 'running':
      return 'success'
    case 'failed':
      return 'error'
    case 'partially_running':
      return 'warning'
    case 'deploying':
      return 'info'
    default:
      return 'grey'
  }
}
</script>

<template>
  <v-card>
    <v-card-title class="d-flex align-center">
      <v-icon class="mr-2">mdi-format-list-bulleted</v-icon>
      Pipeline Groups
      <v-badge
        v-if="groups.length > 0"
        :content="groups.length"
        color="primary"
        inline
        class="ml-2"
      />
      <v-spacer />
      <v-progress-circular
        v-if="loading"
        indeterminate
        size="20"
        width="2"
      />
    </v-card-title>

    <v-card-text class="pa-0">
      <!-- Empty State -->
      <div v-if="groups.length === 0 && !loading" class="text-center pa-8">
        <v-icon size="64" color="grey-lighten-1" class="mb-4">
          mdi-pipe-disconnected
        </v-icon>
        <div class="text-h6 mb-2">No Pipeline Groups</div>
        <div class="text-body-2 text-medium-emphasis">
          Deploy a new pipeline group to get started.
        </div>
      </div>

      <!-- Loading Skeleton -->
      <div v-else-if="loading && groups.length === 0" class="pa-4">
        <v-skeleton-loader type="list-item-two-line" v-for="n in 3" :key="n" />
      </div>

      <!-- Pipeline List -->
      <v-list v-else density="comfortable" class="py-0">
        <v-list-item
          v-for="group in groups"
          :key="group.id"
          :active="selectedId === group.id"
          @click="emit('select', group.id)"
        >
          <template #prepend>
            <v-avatar
              :color="getStatusColor(group.status)"
              size="40"
            >
              <v-icon>mdi-pipe</v-icon>
            </v-avatar>
          </template>

          <v-list-item-title class="font-weight-medium">
            {{ group.name }}
          </v-list-item-title>

          <v-list-item-subtitle>
            <div class="d-flex align-center gap-2 mt-1">
              <StatusChip :status="group.status" size="x-small" />
              <span class="text-caption">
                {{ group.pipeline_count }} pipeline{{ group.pipeline_count !== 1 ? 's' : '' }}
              </span>
              <span class="text-caption text-medium-emphasis">
                • {{ group.placements?.length || 0 }} deployed
              </span>
            </div>
          </v-list-item-subtitle>

          <template #append>
            <v-menu>
              <template #activator="{ props: menuProps }">
                <v-btn
                  icon
                  variant="text"
                  size="small"
                  v-bind="menuProps"
                  @click.stop
                >
                  <v-icon>mdi-dots-vertical</v-icon>
                </v-btn>
              </template>
              <v-list density="compact">
                <v-list-item @click="emit('select', group.id)">
                  <template #prepend>
                    <v-icon>mdi-eye</v-icon>
                  </template>
                  <v-list-item-title>View Details</v-list-item-title>
                </v-list-item>
                <v-list-item
                  class="text-error"
                  @click="emit('teardown', group.id)"
                >
                  <template #prepend>
                    <v-icon color="error">mdi-delete</v-icon>
                  </template>
                  <v-list-item-title>Teardown</v-list-item-title>
                </v-list-item>
              </v-list>
            </v-menu>
          </template>
        </v-list-item>
      </v-list>
    </v-card-text>
  </v-card>
</template>
