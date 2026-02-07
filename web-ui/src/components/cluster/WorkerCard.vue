<script setup lang="ts">
import { computed } from 'vue'
import { formatDistanceToNow } from 'date-fns'
import type { Worker } from '@/types/cluster'
import StatusChip from '@/components/common/StatusChip.vue'

const props = defineProps<{
  worker: Worker
}>()

const emit = defineEmits<{
  select: [worker: Worker]
}>()

const lastHeartbeatText = computed(() => {
  if (!props.worker.last_heartbeat) return 'N/A'
  return formatDistanceToNow(new Date(props.worker.last_heartbeat), { addSuffix: true })
})

const statusColor = computed(() => {
  switch (props.worker.status) {
    case 'ready':
      return 'success'
    case 'unhealthy':
      return 'error'
    case 'draining':
      return 'warning'
    default:
      return 'grey'
  }
})
</script>

<template>
  <v-card
    class="worker-card h-100"
    :class="{ 'border-start-thick': true, [`border-${statusColor}`]: true }"
    hover
    @click="emit('select', worker)"
  >
    <v-card-text>
      <div class="d-flex align-center mb-3">
        <v-avatar :color="statusColor" size="40">
          <v-icon>mdi-server</v-icon>
        </v-avatar>
        <div class="ml-3">
          <div class="font-weight-medium text-truncate" style="max-width: 200px">
            {{ worker.id.length > 20 ? worker.id.substring(0, 16) + '...' : worker.id }}
          </div>
          <div class="text-caption text-medium-emphasis font-monospace">
            {{ worker.address }}
          </div>
        </div>
      </div>

      <div class="d-flex align-center justify-space-between mb-2">
        <span class="text-body-2">Status</span>
        <StatusChip :status="worker.status" size="small" />
      </div>

      <div class="d-flex align-center justify-space-between mb-2">
        <span class="text-body-2">Pipelines</span>
        <v-chip size="small" variant="outlined">
          {{ worker.pipelines_running }}
        </v-chip>
      </div>

      <div class="d-flex align-center justify-space-between">
        <span class="text-body-2">Last Heartbeat</span>
        <span class="text-caption text-medium-emphasis">
          {{ lastHeartbeatText }}
        </span>
      </div>
    </v-card-text>
  </v-card>
</template>

<style scoped>
.worker-card {
  transition: all 0.2s ease;
}

.worker-card:hover {
  transform: translateY(-2px);
}

.border-start-thick {
  border-left-width: 4px !important;
  border-left-style: solid !important;
}

.border-success {
  border-left-color: rgb(var(--v-theme-success)) !important;
}

.border-error {
  border-left-color: rgb(var(--v-theme-error)) !important;
}

.border-warning {
  border-left-color: rgb(var(--v-theme-warning)) !important;
}

.border-grey {
  border-left-color: rgb(var(--v-theme-on-surface-variant)) !important;
}
</style>
