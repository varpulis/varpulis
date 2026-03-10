<script setup lang="ts">
import { ref, computed, onMounted, watch } from 'vue'
import { useConnectorsStore } from '@/stores/connectors'
import { usePipelinesStore } from '@/stores/pipelines'
import type { ClusterConnector } from '@/api/cluster'
import type { AvailableConnector } from '@/api/pipelines'

const store = useConnectorsStore()
const pipelinesStore = usePipelinesStore()

const activeTab = ref('configured')

const dialogOpen = ref(false)
const editMode = ref(false)
const deleteDialogOpen = ref(false)
const deleteTarget = ref<string | null>(null)
const showSecuritySection = ref(false)
const showPasswords = ref<Record<string, boolean>>({})

const formName = ref('')
const formType = ref('mqtt')
const formDescription = ref('')
const formParams = ref<Record<string, string>>({})

const connectorTypes = [
  { title: 'MQTT', value: 'mqtt' },
  { title: 'Kafka', value: 'kafka' },
  { title: 'NATS', value: 'nats' },
  { title: 'HTTP', value: 'http' },
  { title: 'Console', value: 'console' },
]

// ---------- Field definitions per connector type ----------

interface FieldDef {
  key: string
  label: string
  required: boolean
  placeholder: string
  sensitive?: boolean
  type?: 'text' | 'select'
  items?: string[]
}

interface SecurityFieldDef extends FieldDef {
  showWhen?: (params: Record<string, string>) => boolean
}

const typeParamFields: Record<string, FieldDef[]> = {
  mqtt: [
    { key: 'host', label: 'Host', required: true, placeholder: 'localhost' },
    { key: 'port', label: 'Port', required: false, placeholder: '1883' },
    { key: 'client_id', label: 'Client ID', required: false, placeholder: 'varpulis-client' },
    { key: 'qos', label: 'QoS', required: false, placeholder: '0' },
  ],
  kafka: [
    { key: 'brokers', label: 'Brokers', required: true, placeholder: 'broker1:9092,broker2:9092' },
    { key: 'group_id', label: 'Group ID', required: false, placeholder: 'varpulis-group' },
  ],
  nats: [
    { key: 'servers', label: 'Servers', required: true, placeholder: 'nats://localhost:4222' },
  ],
  http: [
    { key: 'url', label: 'URL', required: true, placeholder: 'http://example.com/events' },
    { key: 'method', label: 'Method', required: false, placeholder: 'POST' },
  ],
  console: [],
}

const typeSecurityFields: Record<string, SecurityFieldDef[]> = {
  mqtt: [
    { key: 'username', label: 'Username', required: false, placeholder: 'mqtt-user' },
    { key: 'password', label: 'Password', required: false, placeholder: '', sensitive: true },
    { key: 'profile', label: 'Credentials Profile', required: false, placeholder: 'production-mqtt' },
  ],
  kafka: [
    {
      key: 'security_protocol',
      label: 'Security Protocol',
      required: false,
      placeholder: 'PLAINTEXT',
      type: 'select',
      items: ['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'],
    },
    {
      key: 'sasl_mechanism',
      label: 'SASL Mechanism',
      required: false,
      placeholder: '',
      type: 'select',
      items: ['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512', 'OAUTHBEARER'],
      showWhen: (p) => (p.security_protocol || '').includes('SASL'),
    },
    {
      key: 'sasl_username',
      label: 'SASL Username',
      required: false,
      placeholder: 'kafka-user',
      showWhen: (p) => (p.security_protocol || '').includes('SASL'),
    },
    {
      key: 'sasl_password',
      label: 'SASL Password',
      required: false,
      placeholder: '',
      sensitive: true,
      showWhen: (p) => (p.security_protocol || '').includes('SASL'),
    },
    {
      key: 'ssl_ca_location',
      label: 'CA Certificate Path',
      required: false,
      placeholder: '/etc/varpulis/certs/ca.pem',
      showWhen: (p) => (p.security_protocol || '').includes('SSL'),
    },
    {
      key: 'ssl_certificate_location',
      label: 'Client Certificate Path',
      required: false,
      placeholder: '/etc/varpulis/certs/client.pem',
      showWhen: (p) => (p.security_protocol || '').includes('SSL'),
    },
    {
      key: 'ssl_key_location',
      label: 'Client Key Path',
      required: false,
      placeholder: '/etc/varpulis/certs/client.key',
      showWhen: (p) => (p.security_protocol || '').includes('SSL'),
    },
    { key: 'profile', label: 'Credentials Profile', required: false, placeholder: 'production-kafka' },
  ],
  nats: [
    { key: 'username', label: 'Username', required: false, placeholder: 'nats-user' },
    { key: 'password', label: 'Password', required: false, placeholder: '', sensitive: true },
    { key: 'token', label: 'Auth Token', required: false, placeholder: '', sensitive: true },
    { key: 'profile', label: 'Credentials Profile', required: false, placeholder: 'production-nats' },
  ],
  http: [
    { key: 'authorization', label: 'Authorization Header', required: false, placeholder: 'Bearer <token>', sensitive: true },
    { key: 'profile', label: 'Credentials Profile', required: false, placeholder: 'production-http' },
  ],
  console: [],
}

// ---------- Computed ----------

const currentFields = computed(() => typeParamFields[formType.value] || [])

const currentSecurityFields = computed(() => {
  const fields = typeSecurityFields[formType.value] || []
  return fields.filter((f) => !f.showWhen || f.showWhen(formParams.value))
})

const allSecurityFields = computed(() => typeSecurityFields[formType.value] || [])
const hasSecurityOptions = computed(() => allSecurityFields.value.length > 0)

const nameValid = computed(() => /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(formName.value))
const formValid = computed(() => {
  if (!nameValid.value) return false
  const required = currentFields.value.filter((f) => f.required)
  return required.every((f) => formParams.value[f.key]?.trim())
})

const headers = [
  { title: 'Name', key: 'name', sortable: true },
  { title: 'Type', key: 'connector_type', sortable: true },
  { title: 'Connection', key: 'connection', sortable: false },
  { title: 'Security', key: 'security', sortable: false },
  { title: 'Description', key: 'description', sortable: false },
  { title: 'Actions', key: 'actions', sortable: false, align: 'end' as const },
]

const availableHeaders = [
  { title: 'Connector', key: 'display_name', sortable: true },
  { title: 'Type ID', key: 'connector_type', sortable: true },
  { title: 'Roles', key: 'roles', sortable: false },
  { title: 'Feature Flag', key: 'feature_flag', sortable: true },
  { title: 'Description', key: 'description', sortable: false },
]

// ---------- Helpers ----------

function roleChips(c: AvailableConnector): Array<{ label: string; color: string }> {
  const chips: Array<{ label: string; color: string }> = []
  if (c.supports_source) chips.push({ label: 'Source', color: 'green' })
  if (c.supports_sink) chips.push({ label: 'Sink', color: 'blue' })
  if (c.supports_managed) chips.push({ label: 'Managed', color: 'purple' })
  return chips
}

function connectionSummary(connector: ClusterConnector): string {
  switch (connector.connector_type) {
    case 'mqtt':
      return `${connector.params.host || '?'}:${connector.params.port || '1883'}`
    case 'kafka':
      return connector.params.brokers || '?'
    case 'nats':
      return connector.params.servers || '?'
    case 'http':
      return connector.params.url || '?'
    case 'console':
      return 'stdout'
    default:
      return Object.values(connector.params).join(', ')
  }
}

interface SecurityBadge {
  icon: string
  color: string
  label: string
}

function securityBadges(connector: ClusterConnector): SecurityBadge[] {
  const badges: SecurityBadge[] = []
  const p = connector.params

  if (p.profile) {
    badges.push({ icon: 'mdi-key-chain', color: 'purple', label: `Profile: ${p.profile}` })
  }

  const proto = p.security_protocol || ''
  if (proto.includes('SSL')) {
    badges.push({ icon: 'mdi-lock', color: 'green', label: proto })
  } else if (proto.includes('SASL')) {
    badges.push({ icon: 'mdi-shield-key', color: 'amber', label: proto })
  }

  if (p.sasl_mechanism) {
    badges.push({ icon: 'mdi-account-key', color: 'blue', label: p.sasl_mechanism })
  }

  if (p.ssl_certificate_location) {
    badges.push({ icon: 'mdi-certificate', color: 'teal', label: 'mTLS' })
  }

  // MQTT/NATS username auth
  if (connector.connector_type !== 'kafka' && p.username) {
    badges.push({ icon: 'mdi-account-lock', color: 'blue', label: 'Auth' })
  }

  return badges
}

function typeIcon(type: string): string {
  switch (type) {
    case 'mqtt': return 'mdi-access-point'
    case 'kafka': return 'mdi-apache-kafka'
    case 'nats': return 'mdi-lightning-bolt'
    case 'http': return 'mdi-web'
    case 'console': return 'mdi-console'
    default: return 'mdi-connection'
  }
}

function typeColor(type: string): string {
  switch (type) {
    case 'mqtt': return 'green'
    case 'kafka': return 'orange'
    case 'nats': return 'purple'
    case 'http': return 'blue'
    case 'console': return 'grey'
    default: return 'primary'
  }
}

function togglePasswordVisibility(key: string): void {
  showPasswords.value[key] = !showPasswords.value[key]
}

// ---------- Dialog actions ----------

function openCreateDialog(): void {
  editMode.value = false
  formName.value = ''
  formType.value = 'mqtt'
  formDescription.value = ''
  formParams.value = {}
  showSecuritySection.value = false
  showPasswords.value = {}
  dialogOpen.value = true
}

function openEditDialog(connector: ClusterConnector): void {
  editMode.value = true
  formName.value = connector.name
  formType.value = connector.connector_type
  formDescription.value = connector.description || ''
  formParams.value = { ...connector.params }
  showPasswords.value = {}

  // Auto-expand security section if connector has security params
  const secFields = typeSecurityFields[connector.connector_type] || []
  showSecuritySection.value = secFields.some((f) => connector.params[f.key])

  dialogOpen.value = true
}

function confirmDelete(name: string): void {
  deleteTarget.value = name
  deleteDialogOpen.value = true
}

async function handleSave(): Promise<void> {
  const params: Record<string, string> = {}

  // Connection params
  for (const field of currentFields.value) {
    const val = formParams.value[field.key]?.trim()
    if (val) params[field.key] = val
  }

  // Security params (only if section is open)
  if (showSecuritySection.value) {
    for (const field of currentSecurityFields.value) {
      const val = formParams.value[field.key]?.trim()
      if (val) params[field.key] = val
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

// Clear security params when protocol changes to avoid stale values
watch(
  () => formParams.value.security_protocol,
  (newProto, oldProto) => {
    if (!oldProto || !newProto) return
    const wasSSL = oldProto.includes('SSL')
    const wasSASL = oldProto.includes('SASL')
    const isSSL = newProto.includes('SSL')
    const isSASL = newProto.includes('SASL')

    if (wasSASL && !isSASL) {
      delete formParams.value.sasl_mechanism
      delete formParams.value.sasl_username
      delete formParams.value.sasl_password
    }
    if (wasSSL && !isSSL) {
      delete formParams.value.ssl_ca_location
      delete formParams.value.ssl_certificate_location
      delete formParams.value.ssl_key_location
    }
  },
)

onMounted(() => {
  store.fetchConnectors()
  pipelinesStore.fetchAvailableConnectors()
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

    <v-tabs v-model="activeTab" class="mb-4">
      <v-tab value="configured">Configured</v-tab>
      <v-tab value="available">Available Types</v-tab>
    </v-tabs>

    <!-- Available Connector Types (Component Registry) -->
    <v-card v-if="activeTab === 'available'">
      <v-data-table
        :headers="availableHeaders"
        :items="pipelinesStore.availableConnectors"
        item-value="connector_type"
        no-data-text="No connector types registered. Check that connectors are compiled with the appropriate feature flags."
        hover
      >
        <template #item.display_name="{ item }">
          <div class="d-flex align-center">
            <v-icon :color="typeColor(item.connector_type)" size="small" class="mr-2">
              {{ typeIcon(item.connector_type) }}
            </v-icon>
            <span class="font-weight-medium">{{ item.display_name }}</span>
          </div>
        </template>

        <template #item.connector_type="{ item }">
          <code>{{ item.connector_type }}</code>
        </template>

        <template #item.roles="{ item }">
          <v-chip
            v-for="role in roleChips(item)"
            :key="role.label"
            :color="role.color"
            size="x-small"
            variant="tonal"
            class="mr-1"
          >
            {{ role.label }}
          </v-chip>
        </template>

        <template #item.feature_flag="{ item }">
          <code v-if="item.feature_flag" class="text-medium-emphasis">{{ item.feature_flag }}</code>
          <span v-else class="text-medium-emphasis">-</span>
        </template>

        <template #item.description="{ item }">
          <span class="text-body-2 text-medium-emphasis">{{ item.description }}</span>
        </template>
      </v-data-table>
    </v-card>

    <!-- Configured Connectors -->
    <v-card v-if="activeTab === 'configured'">
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

        <template #item.security="{ item }">
          <div v-if="securityBadges(item).length > 0" class="d-flex align-center flex-wrap ga-1">
            <v-chip
              v-for="badge in securityBadges(item)"
              :key="badge.label"
              :color="badge.color"
              :prepend-icon="badge.icon"
              size="x-small"
              variant="tonal"
            >
              {{ badge.label }}
            </v-chip>
          </div>
          <span v-else class="text-body-2 text-medium-emphasis">-</span>
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
    <v-dialog v-model="dialogOpen" max-width="700" scrollable>
      <v-card>
        <v-card-title>
          {{ editMode ? 'Edit Connector' : 'Add Connector' }}
        </v-card-title>
        <v-card-text>
          <v-text-field
            v-model="formName"
            label="Name"
            placeholder="mqtt_market"
            hint="Valid VPL identifier: starts with letter or underscore, then a-z, 0-9, _"
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

          <!-- Connection Parameters -->
          <v-divider class="mb-4" />

          <div v-if="currentFields.length > 0" class="text-subtitle-2 mb-2">Connection</div>

          <v-text-field
            v-for="field in currentFields"
            :key="field.key"
            v-model="formParams[field.key]"
            :label="field.label + (field.required ? ' *' : '')"
            :placeholder="field.placeholder"
            density="compact"
            class="mb-2"
          />

          <div v-if="currentFields.length === 0 && !hasSecurityOptions" class="text-medium-emphasis text-body-2">
            Console connector has no configuration parameters.
          </div>

          <!-- Security Section -->
          <template v-if="hasSecurityOptions">
            <v-divider class="my-4" />

            <div
              class="d-flex align-center cursor-pointer mb-2"
              @click="showSecuritySection = !showSecuritySection"
            >
              <v-icon size="small" class="mr-2" :color="showSecuritySection ? 'primary' : undefined">
                {{ showSecuritySection ? 'mdi-chevron-down' : 'mdi-chevron-right' }}
              </v-icon>
              <v-icon size="small" class="mr-2" color="amber">mdi-shield-lock</v-icon>
              <span class="text-subtitle-2">Security</span>
              <v-chip
                v-if="!showSecuritySection"
                size="x-small"
                variant="tonal"
                color="grey"
                class="ml-2"
              >
                optional
              </v-chip>
            </div>

            <v-expand-transition>
              <div v-if="showSecuritySection">
                <v-alert
                  type="info"
                  variant="tonal"
                  density="compact"
                  class="mb-3 text-body-2"
                >
                  For production, use a credentials profile instead of inline secrets.
                  See the <em>Connector Security</em> guide.
                </v-alert>

                <template v-for="field in currentSecurityFields" :key="field.key">
                  <!-- Select fields (security_protocol, sasl_mechanism) -->
                  <v-select
                    v-if="field.type === 'select'"
                    v-model="formParams[field.key]"
                    :items="field.items"
                    :label="field.label"
                    :placeholder="field.placeholder"
                    density="compact"
                    clearable
                    class="mb-2"
                  />

                  <!-- Sensitive fields (password, token, secret) -->
                  <v-text-field
                    v-else-if="field.sensitive"
                    v-model="formParams[field.key]"
                    :label="field.label"
                    :placeholder="field.placeholder"
                    :type="showPasswords[field.key] ? 'text' : 'password'"
                    :append-inner-icon="showPasswords[field.key] ? 'mdi-eye-off' : 'mdi-eye'"
                    density="compact"
                    class="mb-2"
                    autocomplete="off"
                    @click:append-inner="togglePasswordVisibility(field.key)"
                  />

                  <!-- Regular text fields -->
                  <v-text-field
                    v-else
                    v-model="formParams[field.key]"
                    :label="field.label"
                    :placeholder="field.placeholder"
                    density="compact"
                    class="mb-2"
                  />
                </template>
              </div>
            </v-expand-transition>
          </template>
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
