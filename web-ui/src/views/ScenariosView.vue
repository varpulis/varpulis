<script setup lang="ts">
import { useRouter } from 'vue-router'
import { scenarios } from '@/data/scenarios'

const router = useRouter()

function startDemo(id: string) {
  router.push(`/scenarios/${id}`)
}
</script>

<template>
  <div>
    <div class="d-flex align-center mb-6">
      <v-icon size="32" color="primary" class="mr-3">mdi-presentation-play</v-icon>
      <div>
        <h1 class="text-h4 font-weight-bold">CxO Demo Scenarios</h1>
        <p class="text-body-2 text-medium-emphasis mb-0">
          Interactive presentations showcasing real-time complex event processing
        </p>
      </div>
    </div>

    <v-row>
      <v-col
        v-for="scenario in scenarios"
        :key="scenario.id"
        cols="12"
        md="6"
      >
        <v-card
          class="scenario-card"
          :style="{ borderLeftColor: `rgb(var(--v-theme-${scenario.color}))` }"
          @click="startDemo(scenario.id)"
        >
          <div class="card-content pa-6">
            <div class="d-flex align-center mb-3">
              <v-avatar :color="scenario.color" size="48" class="mr-3">
                <v-icon size="28" color="white">{{ scenario.icon }}</v-icon>
              </v-avatar>
              <div>
                <h2 class="text-h5 font-weight-bold">{{ scenario.title }}</h2>
                <span class="text-body-2 text-medium-emphasis">{{ scenario.subtitle }}</span>
              </div>
            </div>

            <p class="text-body-1 mb-4" style="line-height: 1.6">
              {{ scenario.summary }}
            </p>

            <div class="d-flex align-center justify-space-between">
              <div class="d-flex gap-3">
                <v-chip size="small" variant="tonal" color="primary">
                  {{ scenario.patterns.length }} patterns
                </v-chip>
                <v-chip size="small" variant="tonal">
                  {{ scenario.steps.length }} steps
                </v-chip>
              </div>

              <v-btn
                :color="scenario.color"
                variant="elevated"
                @click.stop="startDemo(scenario.id)"
              >
                <v-icon start>mdi-play</v-icon>
                Start Demo
              </v-btn>
            </div>
          </div>
        </v-card>
      </v-col>
    </v-row>
  </div>
</template>

<style scoped>
.scenario-card {
  border-left: 4px solid;
  cursor: pointer;
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
}

.scenario-card:hover {
  transform: translateY(-4px);
  box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3) !important;
}

.card-content {
  min-height: 200px;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
}
</style>
