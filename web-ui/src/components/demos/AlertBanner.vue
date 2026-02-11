<script setup lang="ts">
defineProps<{
  alerts: Array<{
    type: string
    severity: 'critical' | 'warning' | 'info'
    title: string
    fields: Record<string, unknown>
  }>
}>()

function severityColor(severity: string): string {
  switch (severity) {
    case 'critical': return '#FF5252'
    case 'warning': return '#FFC107'
    default: return '#4CAF50'
  }
}

function severityIcon(severity: string): string {
  switch (severity) {
    case 'critical': return 'mdi-alert-circle'
    case 'warning': return 'mdi-alert'
    default: return 'mdi-information'
  }
}
</script>

<template>
  <div class="alert-banner-container">
    <TransitionGroup name="slide-down">
      <v-alert
        v-for="(alert, index) in alerts"
        :key="`${alert.type}-${index}`"
        :color="severityColor(alert.severity)"
        variant="tonal"
        density="comfortable"
        class="alert-item mb-2"
        :class="{ 'alert-glow': alert.severity === 'critical' }"
      >
        <template #prepend>
          <v-icon :icon="severityIcon(alert.severity)" />
        </template>
        <div class="d-flex align-center">
          <strong class="mr-3">{{ alert.title }}</strong>
          <span
            v-for="(value, key) in alert.fields"
            :key="String(key)"
            class="text-caption mr-3"
          >
            {{ key }}: <strong>{{ value }}</strong>
          </span>
        </div>
      </v-alert>
    </TransitionGroup>
  </div>
</template>

<style scoped>
.alert-banner-container {
  position: relative;
  z-index: 10;
}

.alert-item {
  border-radius: 8px;
}

@keyframes alert-pulse {
  0%, 100% { box-shadow: 0 0 10px rgba(255, 82, 82, 0.4); }
  50% { box-shadow: 0 0 30px rgba(255, 82, 82, 0.8); }
}

.alert-glow {
  animation: alert-pulse 1.5s ease-in-out infinite;
}

.slide-down-enter-active {
  transition: all 0.5s cubic-bezier(0.4, 0, 0.2, 1);
}

.slide-down-leave-active {
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
}

.slide-down-enter-from {
  opacity: 0;
  transform: translateY(-20px);
}

.slide-down-leave-to {
  opacity: 0;
  transform: translateY(-10px);
}
</style>
