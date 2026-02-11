<script setup lang="ts">
import { computed } from 'vue'

const props = defineProps<{
  greedyCount: number
  varpulisCount: number
  greedyMissed: string[]
  varpulisDetected: string[]
  hasInjected?: boolean
}>()

const missedCount = computed(() => props.greedyMissed.length)
const isClean = computed(() => props.hasInjected && props.greedyCount === 0 && props.varpulisCount === 0 && missedCount.value === 0)
</script>

<template>
  <v-row class="h-100 ma-0" style="min-height: 400px">
    <!-- Left: Traditional CEP -->
    <v-col cols="6" class="pa-3">
      <v-card
        color="rgba(0, 0, 0, 0.5)"
        variant="flat"
        class="h-100"
        style="backdrop-filter: blur(8px); border: 1px solid rgba(255, 82, 82, 0.2)"
      >
        <v-card-title class="d-flex align-center text-white pb-1">
          <v-icon class="mr-2" color="orange">mdi-fire</v-icon>
          Traditional CEP
          <v-spacer />
          <v-chip
            :color="greedyCount > 0 ? 'warning' : isClean ? 'success' : 'grey-darken-1'"
            size="small"
            variant="flat"
          >
            {{ greedyCount }} detected
          </v-chip>
        </v-card-title>

        <v-card-subtitle class="text-medium-emphasis pb-3">
          Skip-till-next-match (greedy)
        </v-card-subtitle>

        <v-card-text>
          <div
            v-if="missedCount > 0"
            class="missed-alert pa-3 rounded mb-3"
          >
            <div class="d-flex align-center mb-2">
              <v-icon size="small" color="error" class="mr-2">mdi-alert-circle</v-icon>
              <span class="text-body-2 font-weight-bold" style="color: #FF5252">
                {{ missedCount }} attack{{ missedCount > 1 ? 's' : '' }} MISSED
              </span>
            </div>
            <div
              v-for="(chain, i) in greedyMissed"
              :key="i"
              class="d-flex align-center py-1"
            >
              <v-icon size="x-small" color="error" class="mr-2">mdi-close-circle</v-icon>
              <span class="text-caption" style="color: rgba(255,82,82,0.8)">{{ chain }}</span>
            </div>
          </div>

          <div
            v-for="(chain, i) in varpulisDetected.slice(0, greedyCount)"
            :key="'g-' + i"
            class="detected-chain d-flex align-center py-2"
          >
            <v-icon size="small" color="success" class="mr-2">mdi-check-circle</v-icon>
            <span class="text-body-2 text-white">{{ chain }}</span>
          </div>

          <!-- Clean traffic: no alerts, correct behavior -->
          <div
            v-if="isClean"
            class="text-center pa-8"
          >
            <v-icon size="48" class="mb-3" color="success">mdi-shield-check</v-icon>
            <div class="text-body-2 text-success">0 alerts — correct</div>
            <div class="text-caption text-medium-emphasis mt-1">No attack patterns in clean traffic</div>
          </div>

          <!-- Waiting state -->
          <div
            v-else-if="greedyCount === 0 && missedCount === 0 && !hasInjected"
            class="text-center text-medium-emphasis pa-8"
          >
            <v-icon size="48" class="mb-3" color="grey-darken-1">mdi-shield-check-outline</v-icon>
            <div class="text-body-2">Waiting for events...</div>
          </div>
        </v-card-text>
      </v-card>
    </v-col>

    <!-- Right: Varpulis -->
    <v-col cols="6" class="pa-3">
      <v-card
        color="rgba(0, 0, 0, 0.5)"
        variant="flat"
        class="h-100"
        style="backdrop-filter: blur(8px); border: 1px solid rgba(124, 77, 255, 0.3)"
      >
        <v-card-title class="d-flex align-center text-white pb-1">
          <v-icon class="mr-2" color="primary">mdi-shield-check</v-icon>
          Varpulis
          <v-spacer />
          <v-chip
            :color="varpulisCount > 0 ? 'primary' : isClean ? 'success' : 'grey-darken-1'"
            size="small"
            variant="flat"
          >
            {{ varpulisCount }} detected
          </v-chip>
        </v-card-title>

        <v-card-subtitle class="text-medium-emphasis pb-3">
          Skip-till-any-match (complete)
        </v-card-subtitle>

        <v-card-text>
          <div
            v-for="(chain, i) in varpulisDetected"
            :key="'v-' + i"
            class="detected-chain d-flex align-center py-2"
          >
            <v-icon size="small" color="success" class="mr-2">mdi-check-circle</v-icon>
            <span class="text-body-2 text-white">{{ chain }}</span>
          </div>

          <!-- Clean traffic: no alerts, correct behavior -->
          <div
            v-if="isClean"
            class="text-center pa-8"
          >
            <v-icon size="48" class="mb-3" color="success">mdi-shield-check</v-icon>
            <div class="text-body-2 text-success">0 alerts — correct</div>
            <div class="text-caption text-medium-emphasis mt-1">No false positives</div>
          </div>

          <!-- Waiting state -->
          <div
            v-else-if="varpulisCount === 0 && !hasInjected"
            class="text-center text-medium-emphasis pa-8"
          >
            <v-icon size="48" class="mb-3" color="grey-darken-1">mdi-shield-check-outline</v-icon>
            <div class="text-body-2">Waiting for events...</div>
          </div>

          <div
            v-if="varpulisCount > 0 && missedCount > 0"
            class="mt-4 pa-3 rounded"
            style="background: rgba(124, 77, 255, 0.08); border: 1px solid rgba(124, 77, 255, 0.2)"
          >
            <div class="d-flex align-center">
              <v-icon size="small" color="primary" class="mr-2">mdi-information</v-icon>
              <span class="text-caption" style="color: rgba(124, 77, 255, 0.9)">
                Found {{ varpulisCount - greedyCount }} attack{{ (varpulisCount - greedyCount) > 1 ? 's' : '' }} that traditional CEP missed
              </span>
            </div>
          </div>
        </v-card-text>
      </v-card>
    </v-col>
  </v-row>
</template>

<style scoped>
.missed-alert {
  background: rgba(255, 82, 82, 0.08);
  border: 1px solid rgba(255, 82, 82, 0.2);
}

.detected-chain {
  border-bottom: 1px solid rgba(255, 255, 255, 0.05);
}

.detected-chain:last-child {
  border-bottom: none;
}
</style>
