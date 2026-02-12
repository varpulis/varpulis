<script setup lang="ts">
import { computed } from 'vue'
import type { ClusterHealthMetrics } from '@/types/cluster'

const props = defineProps<{
  health: ClusterHealthMetrics | null
}>()

const raftRoleLabel = computed(() => {
  if (!props.health || props.health.raft_role < 0) return 'Unknown'
  switch (props.health.raft_role) {
    case 0: return 'Follower'
    case 1: return 'Candidate'
    case 2: return 'Leader'
    default: return 'Unknown'
  }
})

const raftRoleColor = computed(() => {
  if (!props.health || props.health.raft_role < 0) return 'grey'
  switch (props.health.raft_role) {
    case 2: return 'success'
    case 1: return 'warning'
    default: return 'info'
  }
})

const raftRoleIcon = computed(() => {
  if (!props.health || props.health.raft_role < 0) return 'mdi-help-circle'
  switch (props.health.raft_role) {
    case 2: return 'mdi-crown'
    case 1: return 'mdi-account-clock'
    default: return 'mdi-account'
  }
})

const totalWorkers = computed(() => {
  if (!props.health) return 0
  return props.health.workers_ready + props.health.workers_unhealthy + props.health.workers_draining
})

const totalMigrations = computed(() => {
  if (!props.health) return 0
  return props.health.migrations_success + props.health.migrations_failure
})

const totalDeploys = computed(() => {
  if (!props.health) return 0
  return props.health.deploys_success + props.health.deploys_failure
})
</script>

<template>
  <div v-if="health">
    <!-- Raft Consensus Status -->
    <v-row>
      <v-col cols="12">
        <div class="text-subtitle-1 font-weight-medium mb-3">
          <v-icon class="mr-1">mdi-shield-check</v-icon>
          Raft Consensus
        </div>
      </v-col>
    </v-row>

    <v-row>
      <v-col cols="12" sm="6" md="3">
        <v-card variant="tonal" :color="raftRoleColor">
          <v-card-text class="text-center">
            <v-icon size="32" class="mb-1">{{ raftRoleIcon }}</v-icon>
            <div class="text-h5 font-weight-bold">{{ raftRoleLabel }}</div>
            <div class="text-caption">Raft Role</div>
          </v-card-text>
        </v-card>
      </v-col>

      <v-col cols="12" sm="6" md="3">
        <v-card variant="tonal">
          <v-card-text class="text-center">
            <v-icon size="32" class="mb-1">mdi-counter</v-icon>
            <div class="text-h5 font-weight-bold">{{ health.raft_term }}</div>
            <div class="text-caption">Current Term</div>
          </v-card-text>
        </v-card>
      </v-col>

      <v-col cols="12" sm="6" md="3">
        <v-card variant="tonal">
          <v-card-text class="text-center">
            <v-icon size="32" class="mb-1">mdi-source-commit</v-icon>
            <div class="text-h5 font-weight-bold">{{ health.raft_commit_index }}</div>
            <div class="text-caption">Commit Index</div>
          </v-card-text>
        </v-card>
      </v-col>

      <v-col cols="12" sm="6" md="3">
        <v-card variant="tonal" :color="health.workers_unhealthy > 0 ? 'warning' : 'success'">
          <v-card-text class="text-center">
            <v-icon size="32" class="mb-1">mdi-server-network</v-icon>
            <div class="text-h5 font-weight-bold">{{ totalWorkers }}</div>
            <div class="text-caption">Total Workers</div>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <!-- Worker Distribution -->
    <v-row class="mt-2">
      <v-col cols="12">
        <div class="text-subtitle-1 font-weight-medium mb-3">
          <v-icon class="mr-1">mdi-server</v-icon>
          Worker Distribution
        </div>
      </v-col>
    </v-row>

    <v-row>
      <v-col cols="12" sm="4">
        <v-card variant="outlined">
          <v-card-text class="d-flex align-center">
            <v-icon color="success" size="24" class="mr-3">mdi-check-circle</v-icon>
            <div>
              <div class="text-h6 font-weight-bold">{{ health.workers_ready }}</div>
              <div class="text-caption text-medium-emphasis">Ready</div>
            </div>
          </v-card-text>
        </v-card>
      </v-col>

      <v-col cols="12" sm="4">
        <v-card variant="outlined">
          <v-card-text class="d-flex align-center">
            <v-icon color="error" size="24" class="mr-3">mdi-alert-circle</v-icon>
            <div>
              <div class="text-h6 font-weight-bold">{{ health.workers_unhealthy }}</div>
              <div class="text-caption text-medium-emphasis">Unhealthy</div>
            </div>
          </v-card-text>
        </v-card>
      </v-col>

      <v-col cols="12" sm="4">
        <v-card variant="outlined">
          <v-card-text class="d-flex align-center">
            <v-icon color="warning" size="24" class="mr-3">mdi-progress-clock</v-icon>
            <div>
              <div class="text-h6 font-weight-bold">{{ health.workers_draining }}</div>
              <div class="text-caption text-medium-emphasis">Draining</div>
            </div>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <!-- Operations -->
    <v-row class="mt-2">
      <v-col cols="12">
        <div class="text-subtitle-1 font-weight-medium mb-3">
          <v-icon class="mr-1">mdi-chart-bar</v-icon>
          Operations
        </div>
      </v-col>
    </v-row>

    <v-row>
      <v-col cols="12" md="6">
        <v-card variant="outlined">
          <v-card-title class="text-subtitle-2">
            <v-icon class="mr-1" size="18">mdi-rocket-launch</v-icon>
            Deployments
          </v-card-title>
          <v-card-text>
            <v-list density="compact" class="bg-transparent">
              <v-list-item>
                <v-list-item-title>Pipeline Groups</v-list-item-title>
                <template #append>
                  <span class="text-body-2 font-weight-bold">{{ health.pipeline_groups_total }}</span>
                </template>
              </v-list-item>
              <v-list-item>
                <v-list-item-title>Total Deployments</v-list-item-title>
                <template #append>
                  <span class="text-body-2 font-weight-bold">{{ health.deployments_total }}</span>
                </template>
              </v-list-item>
              <v-list-item>
                <v-list-item-title>Deploys (success / total)</v-list-item-title>
                <template #append>
                  <span class="text-body-2">
                    <span class="text-success font-weight-bold">{{ health.deploys_success }}</span>
                    <span class="text-medium-emphasis"> / {{ totalDeploys }}</span>
                  </span>
                </template>
              </v-list-item>
            </v-list>
          </v-card-text>
        </v-card>
      </v-col>

      <v-col cols="12" md="6">
        <v-card variant="outlined">
          <v-card-title class="text-subtitle-2">
            <v-icon class="mr-1" size="18">mdi-swap-horizontal</v-icon>
            Migrations
          </v-card-title>
          <v-card-text>
            <v-list density="compact" class="bg-transparent">
              <v-list-item>
                <v-list-item-title>Successful</v-list-item-title>
                <template #append>
                  <v-chip color="success" size="x-small" variant="flat">
                    {{ health.migrations_success }}
                  </v-chip>
                </template>
              </v-list-item>
              <v-list-item>
                <v-list-item-title>Failed</v-list-item-title>
                <template #append>
                  <v-chip :color="health.migrations_failure > 0 ? 'error' : 'default'" size="x-small" variant="flat">
                    {{ health.migrations_failure }}
                  </v-chip>
                </template>
              </v-list-item>
              <v-list-item>
                <v-list-item-title>Total</v-list-item-title>
                <template #append>
                  <span class="text-body-2 font-weight-bold">{{ totalMigrations }}</span>
                </template>
              </v-list-item>
            </v-list>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>
  </div>

  <!-- Loading state -->
  <div v-else class="d-flex justify-center align-center pa-8">
    <v-progress-circular indeterminate size="48" />
    <span class="ml-4 text-medium-emphasis">Loading cluster health metrics...</span>
  </div>
</template>
