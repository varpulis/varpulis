<script setup lang="ts">
import { ref, computed } from 'vue'
import type { RouteConfig } from '@/types/pipeline'

const props = defineProps<{
  routes: RouteConfig[]
  pipelineNames: string[]
}>()

const emit = defineEmits<{
  'update:routes': [routes: RouteConfig[]]
}>()

const localRoutes = computed({
  get: () => props.routes,
  set: (val) => emit('update:routes', val),
})

const editDialog = ref(false)
const editIndex = ref<number | null>(null)
const editRoute = ref<RouteConfig>({
  id: '',
  source: '',
  target: '',
  event_types: [],
  filter: '',
})

const eventTypesInput = ref('')

function openAddDialog(): void {
  editIndex.value = null
  editRoute.value = {
    id: `route-${Date.now()}`,
    source: '',
    target: '',
    event_types: [],
    filter: '',
  }
  eventTypesInput.value = ''
  editDialog.value = true
}

function openEditDialog(index: number): void {
  editIndex.value = index
  const route = localRoutes.value[index]
  editRoute.value = { ...route }
  eventTypesInput.value = route.event_types.join(', ')
  editDialog.value = true
}

function saveRoute(): void {
  // Parse event types
  editRoute.value.event_types = eventTypesInput.value
    .split(',')
    .map((t) => t.trim())
    .filter((t) => t)

  const newRoutes = [...localRoutes.value]

  if (editIndex.value !== null) {
    newRoutes[editIndex.value] = editRoute.value
  } else {
    newRoutes.push(editRoute.value)
  }

  localRoutes.value = newRoutes
  editDialog.value = false
}

function deleteRoute(index: number): void {
  const newRoutes = [...localRoutes.value]
  newRoutes.splice(index, 1)
  localRoutes.value = newRoutes
}

function closeDialog(): void {
  editDialog.value = false
}
</script>

<template>
  <div>
    <div class="d-flex align-center mb-3">
      <span class="text-subtitle-1">Routing Rules</span>
      <v-spacer />
      <v-btn
        variant="outlined"
        size="small"
        prepend-icon="mdi-plus"
        @click="openAddDialog"
      >
        Add Route
      </v-btn>
    </div>

    <!-- Routes Table -->
    <v-table v-if="routes.length > 0" density="compact">
      <thead>
        <tr>
          <th>Source</th>
          <th>Target</th>
          <th>Event Types</th>
          <th>Filter</th>
          <th class="text-right">Actions</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="(route, index) in routes" :key="route.id">
          <td>{{ route.source }}</td>
          <td>{{ route.target }}</td>
          <td>
            <v-chip
              v-for="et in route.event_types"
              :key="et"
              size="x-small"
              class="mr-1"
            >
              {{ et }}
            </v-chip>
            <span v-if="route.event_types.length === 0" class="text-medium-emphasis">
              All events
            </span>
          </td>
          <td>
            <code v-if="route.filter" class="text-caption">{{ route.filter }}</code>
            <span v-else class="text-medium-emphasis">-</span>
          </td>
          <td class="text-right">
            <v-btn
              icon
              size="small"
              variant="text"
              @click="openEditDialog(index)"
            >
              <v-icon>mdi-pencil</v-icon>
            </v-btn>
            <v-btn
              icon
              size="small"
              variant="text"
              color="error"
              @click="deleteRoute(index)"
            >
              <v-icon>mdi-delete</v-icon>
            </v-btn>
          </td>
        </tr>
      </tbody>
    </v-table>

    <!-- Empty State -->
    <v-card v-else variant="outlined" class="text-center pa-6">
      <v-icon size="48" color="grey-lighten-1" class="mb-2">
        mdi-arrow-decision-outline
      </v-icon>
      <div class="text-body-2 text-medium-emphasis">
        No routes configured. Events will not be forwarded between pipelines.
      </div>
    </v-card>

    <!-- Edit Dialog -->
    <v-dialog v-model="editDialog" max-width="500">
      <v-card>
        <v-card-title>
          {{ editIndex !== null ? 'Edit Route' : 'Add Route' }}
        </v-card-title>

        <v-card-text>
          <v-select
            v-model="editRoute.source"
            label="Source Pipeline"
            :items="pipelineNames"
            class="mb-3"
          />

          <v-select
            v-model="editRoute.target"
            label="Target Pipeline"
            :items="pipelineNames"
            class="mb-3"
          />

          <v-text-field
            v-model="eventTypesInput"
            label="Event Types"
            hint="Comma-separated list (leave empty for all events)"
            persistent-hint
            class="mb-3"
          />

          <v-text-field
            v-model="editRoute.filter"
            label="Filter Expression (optional)"
            hint="VPL expression to filter events"
            persistent-hint
          />
        </v-card-text>

        <v-card-actions>
          <v-spacer />
          <v-btn variant="text" @click="closeDialog">Cancel</v-btn>
          <v-btn
            color="primary"
            :disabled="!editRoute.source || !editRoute.target"
            @click="saveRoute"
          >
            Save
          </v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>
  </div>
</template>
