<script setup lang="ts">
import type { Worker } from '@/types/cluster'
import WorkerCard from './WorkerCard.vue'

defineProps<{
  workers: Worker[]
  loading: boolean
}>()

const emit = defineEmits<{
  select: [worker: Worker]
}>()
</script>

<template>
  <div>
    <!-- Loading State -->
    <v-row v-if="loading && workers.length === 0">
      <v-col v-for="n in 4" :key="n" cols="12" sm="6" md="4" lg="3">
        <v-skeleton-loader type="card" />
      </v-col>
    </v-row>

    <!-- Empty State -->
    <v-card v-else-if="workers.length === 0" class="text-center pa-8">
      <v-icon size="64" color="grey-lighten-1" class="mb-4">
        mdi-server-off
      </v-icon>
      <div class="text-h6 mb-2">No Workers Found</div>
      <div class="text-body-2 text-medium-emphasis">
        Workers will appear here once they connect to the coordinator.
      </div>
    </v-card>

    <!-- Worker Grid -->
    <v-row v-else>
      <v-col
        v-for="worker in workers"
        :key="worker.id"
        cols="12"
        sm="6"
        md="4"
        lg="3"
      >
        <WorkerCard
          :worker="worker"
          @select="emit('select', $event)"
        />
      </v-col>
    </v-row>
  </div>
</template>
