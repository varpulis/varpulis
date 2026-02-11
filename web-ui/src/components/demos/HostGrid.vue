<script setup lang="ts">
defineProps<{
  hosts: string[]
  alerts: Map<string, number>
}>()
</script>

<template>
  <div class="host-grid">
    <div
      v-for="host in hosts"
      :key="host"
      class="host-tile"
      :class="{ 'host-alert': alerts.has(host) }"
    >
      <div class="host-name">{{ host }}</div>
      <v-badge
        v-if="alerts.has(host)"
        :content="alerts.get(host)"
        color="error"
        class="host-badge"
        floating
      />
      <v-icon
        v-if="alerts.has(host)"
        size="20"
        color="error"
        class="host-icon"
      >
        mdi-alert-circle
      </v-icon>
      <v-icon
        v-else
        size="20"
        color="grey-darken-1"
        class="host-icon"
      >
        mdi-server
      </v-icon>
    </div>
  </div>
</template>

<style scoped>
.host-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(150px, 1fr));
  gap: 12px;
  padding: 8px;
}

.host-tile {
  position: relative;
  background: rgba(255, 255, 255, 0.04);
  border: 1px solid rgba(255, 255, 255, 0.08);
  border-radius: 8px;
  padding: 16px 12px;
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 8px;
  transition: all 0.3s ease;
}

.host-alert {
  background: rgba(255, 82, 82, 0.1);
  border-color: rgba(255, 82, 82, 0.4);
  animation: host-pulse 1.5s ease-in-out infinite;
}

.host-name {
  font-size: 11px;
  font-weight: 600;
  color: rgba(255, 255, 255, 0.7);
  text-align: center;
  word-break: break-all;
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
}

.host-alert .host-name {
  color: #FF5252;
}

.host-icon {
  opacity: 0.6;
}

.host-alert .host-icon {
  opacity: 1;
}

.host-badge {
  position: absolute;
  top: 4px;
  right: 4px;
}

@keyframes host-pulse {
  0%, 100% { box-shadow: 0 0 0 0 rgba(255, 82, 82, 0.3); }
  50% { box-shadow: 0 0 12px 4px rgba(255, 82, 82, 0); }
}
</style>
