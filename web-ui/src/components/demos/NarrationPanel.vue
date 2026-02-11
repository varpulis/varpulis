<script setup lang="ts">
import type { DemoStep } from '@/types/scenario'

defineProps<{
  step: DemoStep
  stepIndex: number
  totalSteps: number
  injecting: boolean
  canGoBack: boolean
  canGoForward: boolean
}>()

defineEmits<{
  inject: []
  next: []
  prev: []
}>()

function phaseColor(phase: string): string {
  switch (phase) {
    case 'normal': return 'success'
    case 'attack': return 'error'
    case 'negative': return 'grey'
    default: return 'primary'
  }
}

function phaseLabel(phase: string): string {
  switch (phase) {
    case 'normal': return 'Normal'
    case 'attack': return 'Attack'
    case 'negative': return 'Verification'
    default: return phase
  }
}
</script>

<template>
  <v-card class="narration-panel" color="rgba(0,0,0,0.7)" variant="flat">
    <v-card-text class="pa-5">
      <div class="d-flex align-center justify-space-between">
        <div class="d-flex align-center flex-grow-1">
          <v-btn
            icon="mdi-chevron-left"
            variant="text"
            :disabled="!canGoBack"
            size="small"
            class="mr-2"
            @click="$emit('prev')"
          />

          <div class="flex-grow-1">
            <div class="d-flex align-center mb-2">
              <v-chip
                :color="phaseColor(step.phase)"
                size="small"
                label
                class="mr-3"
              >
                {{ phaseLabel(step.phase) }}
              </v-chip>
              <span class="text-h6 font-weight-bold text-white">
                {{ step.title }}
              </span>
            </div>
            <div class="narration-text text-white">
              {{ step.narration }}
            </div>
          </div>

          <div class="d-flex align-center ml-4">
            <v-btn
              color="primary"
              size="x-large"
              :loading="injecting"
              :class="{ 'inject-ready': !injecting }"
              class="mr-3"
              @click="$emit('inject')"
            >
              <v-icon start>mdi-play</v-icon>
              Inject Events
            </v-btn>

            <v-btn
              icon="mdi-chevron-right"
              variant="text"
              :disabled="!canGoForward"
              size="small"
              @click="$emit('next')"
            />
          </div>
        </div>
      </div>
    </v-card-text>
  </v-card>
</template>

<style scoped>
.narration-panel {
  backdrop-filter: blur(10px);
  border-top: 1px solid rgba(255, 255, 255, 0.1);
}

.narration-text {
  font-size: 18px;
  line-height: 1.5;
  opacity: 0.9;
}

@keyframes inject-ready {
  0%, 100% { box-shadow: 0 0 0 0 rgba(124, 77, 255, 0.4); }
  50% { box-shadow: 0 0 0 8px rgba(124, 77, 255, 0); }
}

.inject-ready {
  animation: inject-ready 2s ease-in-out infinite;
}
</style>
