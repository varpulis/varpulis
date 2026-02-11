<script setup lang="ts">
import { computed } from 'vue'

const props = defineProps<{
  activeStages: string[]
}>()

interface KillChainStage {
  id: string
  label: string
  icon: string
}

const stages: KillChainStage[] = [
  { id: 'recon', label: 'Reconnaissance', icon: 'mdi-magnify-scan' },
  { id: 'access', label: 'Initial Access', icon: 'mdi-login' },
  { id: 'lateral', label: 'Lateral Movement', icon: 'mdi-arrow-decision' },
  { id: 'exfil', label: 'Exfiltration', icon: 'mdi-database-export' },
  { id: 'escalation', label: 'Escalation', icon: 'mdi-shield-crown' },
]

function isActive(stageId: string): boolean {
  return props.activeStages.includes(stageId)
}

const latestActiveId = computed(() => {
  for (let i = stages.length - 1; i >= 0; i--) {
    if (isActive(stages[i].id)) {
      return stages[i].id
    }
  }
  return null
})
</script>

<template>
  <div class="kill-chain-bar">
    <div class="kill-chain-bar__track">
      <div
        v-for="(stage, index) in stages"
        :key="stage.id"
        class="kill-chain-bar__segment"
        :class="{
          'kill-chain-bar__segment--active': isActive(stage.id),
          'kill-chain-bar__segment--pulse': stage.id === latestActiveId,
        }"
      >
        <!-- Fill animation overlay -->
        <div
          class="kill-chain-bar__fill"
          :class="{ 'kill-chain-bar__fill--active': isActive(stage.id) }"
        />

        <!-- Content -->
        <div class="kill-chain-bar__content">
          <v-icon :icon="stage.icon" size="18" color="white" class="mr-1" />
          <span class="kill-chain-bar__label">{{ stage.label }}</span>
        </div>

        <!-- Arrow separator -->
        <div v-if="index < stages.length - 1" class="kill-chain-bar__arrow">
          <v-icon icon="mdi-chevron-right" size="20" color="rgba(255,255,255,0.3)" />
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.kill-chain-bar {
  height: 80px;
  width: 100%;
  display: flex;
  align-items: center;
  padding: 0 16px;
  flex-shrink: 0;
}

.kill-chain-bar__track {
  display: flex;
  width: 100%;
  height: 52px;
  border-radius: 8px;
  overflow: hidden;
  gap: 2px;
}

.kill-chain-bar__segment {
  flex: 1;
  position: relative;
  display: flex;
  align-items: center;
  justify-content: center;
  background: #333;
  overflow: hidden;
  transition: background 0.3s ease;
}

.kill-chain-bar__segment:first-child {
  border-radius: 8px 0 0 8px;
}

.kill-chain-bar__segment:last-child {
  border-radius: 0 8px 8px 0;
}

.kill-chain-bar__fill {
  position: absolute;
  inset: 0;
  background: #FF5252;
  transform: scaleX(0);
  transform-origin: left;
  transition: transform 0.6s cubic-bezier(0.4, 0, 0.2, 1);
}

.kill-chain-bar__fill--active {
  transform: scaleX(1);
}

.kill-chain-bar__content {
  position: relative;
  z-index: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 4px;
}

.kill-chain-bar__label {
  font-size: 12px;
  font-weight: 600;
  color: white;
  white-space: nowrap;
  letter-spacing: 0.02em;
}

.kill-chain-bar__arrow {
  position: absolute;
  right: -12px;
  z-index: 2;
}

.kill-chain-bar__segment--pulse {
  animation: stage-pulse 1.5s ease-in-out infinite;
}

@keyframes stage-pulse {
  0%, 100% {
    box-shadow: inset 0 0 0 0 rgba(255, 82, 82, 0);
  }
  50% {
    box-shadow: inset 0 0 20px rgba(255, 82, 82, 0.5), 0 0 12px rgba(255, 82, 82, 0.4);
  }
}
</style>
