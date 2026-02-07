<script setup lang="ts">
import { computed } from 'vue'
import type { WorkerStatus } from '@/types/cluster'
import type { PipelineGroupStatus, PipelineStatus } from '@/types/pipeline'

type Status = WorkerStatus | PipelineGroupStatus | PipelineStatus | string

const props = defineProps<{
  status: Status
  size?: 'x-small' | 'small' | 'default' | 'large'
}>()

// Convert snake_case to Title Case for display
const displayStatus = computed(() => {
  if (!props.status) return ''
  return props.status
    .split('_')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join(' ')
})

const statusConfig = computed(() => {
  const status = props.status?.toLowerCase() || ''

  const configs: Record<string, { color: string; icon: string }> = {
    // Worker statuses
    ready: { color: 'success', icon: 'mdi-check-circle' },
    unhealthy: { color: 'error', icon: 'mdi-alert-circle' },
    draining: { color: 'warning', icon: 'mdi-progress-clock' },
    registering: { color: 'info', icon: 'mdi-account-plus' },

    // Pipeline group statuses (snake_case from API)
    deploying: { color: 'info', icon: 'mdi-rocket-launch' },
    running: { color: 'success', icon: 'mdi-play-circle' },
    partially_running: { color: 'warning', icon: 'mdi-alert' },
    failed: { color: 'error', icon: 'mdi-close-circle' },
    torn_down: { color: 'grey', icon: 'mdi-delete' },

    // Pipeline deployment statuses
    stopped: { color: 'grey', icon: 'mdi-stop-circle' },
  }

  return configs[status] || { color: 'grey', icon: 'mdi-help-circle' }
})
</script>

<template>
  <v-chip
    :color="statusConfig.color"
    :size="size || 'small'"
    variant="flat"
  >
    <v-icon start :size="size === 'x-small' ? 12 : 16">
      {{ statusConfig.icon }}
    </v-icon>
    {{ displayStatus }}
  </v-chip>
</template>
