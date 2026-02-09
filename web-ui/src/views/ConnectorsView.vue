<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useConnectorsStore } from '@/stores/connectors'
import type { ClusterConnector } from '@/api/cluster'

const store = useConnectorsStore()

const dialogOpen = ref(false)
const editMode = ref(false)
const deleteDialogOpen = ref(false)
const deleteTarget = ref<string | null>(null)

const formName = ref('')
const formType = ref('mqtt')
const formDescription = ref('')
const formParams = ref<Record<string, string>>({})

const connectorTypes = [
  { title: 'MQTT', value: 'mqtt' },
  { title: 'Kafka', value: 'kafka' },
  { title: 'HTTP', value: 'http' },
  { title: 'Console', value: 'console' },
]

const typeParamFields: Record<string, Array<{ key: string; label: string; required: boolean; placeholder: string }>> = {
  mqtt: [
    { key: 'host', label: 'Host', required: true, placeholder: 'localhost' },
    { key: 'port', label: 'Port', required: false, placeholder: '1883' },
    { key: 'client_id', label: 'Client ID', required: false, placeholder: 'varpulis-client' },
    { key: 'qos', label: 'QoS', required: false, placeholder: '0' },
  ],
  kafka: [
    { key: 'brokers', label: 'Brokers', required: true, placeholder: 'localhost:9092' },
    { key: 'group_id', label: 'Group ID', required: false, placeholder: 'varpulis-group' },
  ],
  http: [
    { key: 'url', label: 'URL', required: true, placeholder: 'http://example.com/events' },
    { key: 'method', label: 'Method', required: false, placeholder: 'POST' },
  ],
  console: [],
}

const currentFields = computed(() => typeParamFields[formType.value] || [])

const nameValid = computed(() => /^[a-zA-Z][a-zA-Z0-9_-]*$/.test(formName.value))
const formValid = computed(() => {
  if (!nameValid.value) return false
  const required = currentFields.value.filter((f) => f.required)
  return required.every((f) => formParams.value[f.key]?.trim())
})

const headers = [
  { title: 'Name', key: 'name', sortable: true },
  { title: 'Type', key: 'connector_type', sortable: true },
  { title: 'Connection', key: 'connection', sortable: false },
  { title: 'Description', key: 'description', sortable: false },
  { title: 'Actions', key: 'actions', sortable: false, align: 'end' as const },
]

function connectionSummary(connector: ClusterConnector): string {
  switch (connector.connector_type) {
    case 'mqtt':
      return `${connector.params.host || '?'}:${connector.params.port || '1883'}`
    case 'kafka':
      return connector.params.brokers || '?'
    case 'http':
      return connector.params.url || '?'
    case 'console':
      return 'stdout'
    default:
      return Object.values(connector.params).join(', ')
  }
}

function typeIcon(type: string): string {
  switch (type) {
    case 'mqtt': return 'mdi-access-point'
    case 'kafka': return 'mdi-apache-kafka'
    case 'http': return 'mdi-web'
    case 'console': return 'mdi-console'
    default: return 'mdi-connection'
  }
}

function typeColor(type: string): string {
  switch (type) {
    case 'mqtt': return 'green'
    case 'kafka': return 'orange'
    case 'http': return 'blue'
    case 'console': return 'grey'
    default: return 'primary'
  }
}

function openCreateDialog(): void {
  editMode.value = false
  formName.value = ''
  formType.value = 'mqtt'
  formDescription.value = ''
  formParams.value = {}
  dialogOpen.value = true
}

function openEditDialog(connector: ClusterConnector): void {
  editMode.value = true
  formName.value = connector.name
  formType.value = connector.connector_type
  formDescription.value = connector.description || ''
  formParams.value = { ...connector.params }
  dialogOpen.value = true
}

function confirmDelete(name: string): void {
  deleteTarget.value = name
  deleteDialogOpen.value = true
}

async function handleSave(): Promise<void> {
  // Build params from only non-empty fields
  const params: Record<string, string> = {}
  for (const field of currentFields.value) {
    const val = formParams.value[field.key]?.trim()
    if (val) {
      params[field.key] = val
    }
  }

  const connector: ClusterConnector = {
    name: formName.value,
    connector_type: formType.value,
    params,
    description: formDescription.value.trim() || undefined,
  }

  let result
  if (editMode.value) {
    result = await store.updateConnector(formName.value, connector)
  } else {
    result = await store.createConnector(connector)
  }

  if (result) {
    dialogOpen.value = false
  }
}

async function handleDelete(): Promise<void> {
  if (deleteTarget.value) {
    await store.deleteConnector(deleteTarget.value)
    deleteDialogOpen.value = false
    deleteTarget.value = null
  }
}

onMounted(() => {
  store.fetchConnectors()
})
</script>

<template>
  <div>
    <div class="d-flex align-center mb-4">
      <h1 class="text-h4">Connectors</h1>
      <v-spacer />
      <v-btn color="primary" prepend-icon="mdi-plus" @click="openCreateDialog">
        Add Connector
      </v-btn>
    </div>

    <v-alert v-if="store.error" type="error" variant="tonal" closable class="mb-4" @click:close="store.clearError">
      {{ store.error }}
    </v-alert>

    <v-card>
      <v-data-table
        :headers="headers"
        :items="store.connectors"
        :loading="store.loading"
        item-value="name"
        no-data-text="No connectors configured. Click 'Add Connector' to create one."
        hover
      >
        <template #item.name="{ item }">
          <div class="d-flex align-center">
            <v-icon :color="typeColor(item.connector_type)" size="small" class="mr-2">
              {{ typeIcon(item.connector_type) }}
            </v-icon>
            <code class="font-weight-medium">{{ item.name }}</code>
          </div>
        </template>

        <template #item.connector_type="{ item }">
          <v-chip :color="typeColor(item.connector_type)" size="small" variant="tonal">
            {{ item.connector_type.toUpperCase() }}
          </v-chip>
        </template>

        <template #item.connection="{ item }">
          <span class="text-body-2 text-medium-emphasis">{{ connectionSummary(item) }}</span>
        </template>

        <template #item.description="{ item }">
          <span class="text-body-2 text-medium-emphasis">{{ item.description || '-' }}</span>
        </template>

        <template #item.actions="{ item }">
          <v-btn icon size="small" variant="text" @click="openEditDialog(item)">
            <v-icon>mdi-pencil</v-icon>
            <v-tooltip activator="parent" location="top">Edit</v-tooltip>
          </v-btn>
          <v-btn icon size="small" variant="text" color="error" @click="confirmDelete(item.name)">
            <v-icon>mdi-delete</v-icon>
            <v-tooltip activator="parent" location="top">Delete</v-tooltip>
          </v-btn>
        </template>
      </v-data-table>
    </v-card>

    <!-- Create/Edit Dialog -->
    <v-dialog v-model="dialogOpen" max-width="600">
      <v-card>
        <v-card-title>
          {{ editMode ? 'Edit Connector' : 'Add Connector' }}
        </v-card-title>
        <v-card-text>
          <v-text-field
            v-model="formName"
            label="Name"
            placeholder="mqtt-market"
            hint="Valid identifier: starts with letter, a-z, 0-9, _, -"
            :rules="[() => nameValid || 'Invalid name format']"
            :disabled="editMode"
            persistent-hint
            class="mb-4"
          />

          <v-select
            v-model="formType"
            :items="connectorTypes"
            label="Type"
            :disabled="editMode"
            class="mb-4"
          />

          <v-text-field
            v-model="formDescription"
            label="Description (optional)"
            placeholder="Market data MQTT broker"
            class="mb-4"
          />

          <v-divider class="mb-4" />

          <div v-if="currentFields.length > 0" class="text-subtitle-2 mb-2">Parameters</div>

          <v-text-field
            v-for="field in currentFields"
            :key="field.key"
            v-model="formParams[field.key]"
            :label="field.label + (field.required ? ' *' : '')"
            :placeholder="field.placeholder"
            density="compact"
            class="mb-2"
          />

          <div v-if="currentFields.length === 0" class="text-medium-emphasis text-body-2">
            Console connector has no configuration parameters.
          </div>
        </v-card-text>
        <v-card-actions>
          <v-spacer />
          <v-btn variant="text" @click="dialogOpen = false">Cancel</v-btn>
          <v-btn color="primary" :disabled="!formValid" @click="handleSave">
            {{ editMode ? 'Update' : 'Create' }}
          </v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>

    <!-- Delete Confirmation -->
    <v-dialog v-model="deleteDialogOpen" max-width="400">
      <v-card>
        <v-card-title>Delete Connector</v-card-title>
        <v-card-text>
          Are you sure you want to delete <strong>{{ deleteTarget }}</strong>?
          Pipelines using this connector will fail to deploy.
        </v-card-text>
        <v-card-actions>
          <v-spacer />
          <v-btn variant="text" @click="deleteDialogOpen = false">Cancel</v-btn>
          <v-btn color="error" @click="handleDelete">Delete</v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>
  </div>
</template>
