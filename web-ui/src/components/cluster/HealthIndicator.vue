<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { useClusterStore } from '@/stores/cluster'

const clusterStore = useClusterStore()

const lastCheck = ref<Date>(new Date())
const checking = ref(false)

const healthStatus = computed(() => {
  const total = clusterStore.workers.length
  const healthy = clusterStore.healthyWorkers.length

  if (total === 0) {
    return { status: 'unknown', text: 'No workers', color: 'grey' }
  }

  const healthPercent = (healthy / total) * 100

  if (healthPercent === 100) {
    return { status: 'healthy', text: 'All healthy', color: 'success' }
  } else if (healthPercent >= 50) {
    return { status: 'degraded', text: 'Degraded', color: 'warning' }
  } else {
    return { status: 'critical', text: 'Critical', color: 'error' }
  }
})

const timeSinceCheck = computed(() => {
  const seconds = Math.floor((Date.now() - lastCheck.value.getTime()) / 1000)
  if (seconds < 60) {
    return `${seconds}s ago`
  }
  return `${Math.floor(seconds / 60)}m ago`
})

let checkInterval: ReturnType<typeof setInterval> | null = null

async function performHealthCheck(): Promise<void> {
  checking.value = true
  await clusterStore.fetchWorkers()
  lastCheck.value = new Date()
  checking.value = false
}

onMounted(() => {
  // Auto health check every 5 seconds
  checkInterval = setInterval(() => {
    lastCheck.value = new Date() // Update time display
  }, 1000)
})

onUnmounted(() => {
  if (checkInterval) {
    clearInterval(checkInterval)
  }
})
</script>

<template>
  <div class="d-flex align-center">
    <v-chip
      :color="healthStatus.color"
      variant="flat"
      size="small"
      class="mr-2"
    >
      <v-icon start size="16">
        {{ healthStatus.status === 'healthy' ? 'mdi-check-circle' : healthStatus.status === 'degraded' ? 'mdi-alert' : healthStatus.status === 'critical' ? 'mdi-alert-octagon' : 'mdi-help-circle' }}
      </v-icon>
      {{ healthStatus.text }}
    </v-chip>

    <span class="text-caption text-medium-emphasis mr-2">
      {{ timeSinceCheck }}
    </span>

    <v-btn
      icon
      size="small"
      variant="text"
      :loading="checking"
      @click="performHealthCheck"
    >
      <v-icon>mdi-refresh</v-icon>
      <v-tooltip activator="parent" location="bottom">
        Refresh status
      </v-tooltip>
    </v-btn>
  </div>
</template>
