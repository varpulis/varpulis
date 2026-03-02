<template>
  <v-container>
    <v-row>
      <v-col cols="12">
        <h1 class="text-h4 mb-6">Admin Panel</h1>
      </v-col>
    </v-row>

    <!-- Summary Cards -->
    <v-row v-if="adminStore.usageSummary">
      <v-col cols="12" sm="6" md="3">
        <v-card>
          <v-card-text class="text-center">
            <div class="text-h4 font-weight-bold">{{ adminStore.usageSummary.total_tenants }}</div>
            <div class="text-body-2 text-medium-emphasis">Total Tenants</div>
          </v-card-text>
        </v-card>
      </v-col>
      <v-col cols="12" sm="6" md="3">
        <v-card>
          <v-card-text class="text-center">
            <div class="text-h4 font-weight-bold text-info">
              {{ adminStore.usageSummary.active_trials }}
            </div>
            <div class="text-body-2 text-medium-emphasis">Active Trials</div>
          </v-card-text>
        </v-card>
      </v-col>
      <v-col cols="12" sm="6" md="3">
        <v-card>
          <v-card-text class="text-center">
            <div class="text-h4 font-weight-bold text-success">
              {{ adminStore.usageSummary.paid_customers }}
            </div>
            <div class="text-body-2 text-medium-emphasis">Paid Customers</div>
          </v-card-text>
        </v-card>
      </v-col>
      <v-col cols="12" sm="6" md="3">
        <v-card>
          <v-card-text class="text-center">
            <div class="text-h4 font-weight-bold">
              {{ formatNumber(adminStore.usageSummary.total_events_this_month) }}
            </div>
            <div class="text-body-2 text-medium-emphasis">Events This Month</div>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <!-- Tenants Table -->
    <v-row class="mt-4">
      <v-col cols="12">
        <v-card>
          <v-card-title>
            Tenants
            <v-spacer />
            <v-text-field
              v-model="search"
              density="compact"
              variant="outlined"
              label="Search"
              prepend-inner-icon="mdi-magnify"
              hide-details
              style="max-width: 300px"
            />
          </v-card-title>
          <v-data-table
            :headers="headers"
            :items="adminStore.tenants"
            :search="search"
            :loading="adminStore.loading"
            item-value="id"
            density="comfortable"
          >
            <template #item.tier="{ item }">
              <v-chip :color="tierColor(item.tier)" size="small">
                {{ item.tier }}
              </v-chip>
            </template>

            <template #item.status="{ item }">
              <v-chip :color="statusColor(item.status)" size="small">
                {{ item.status }}
              </v-chip>
            </template>

            <template #item.usage="{ item }">
              <div style="min-width: 120px">
                <v-progress-linear
                  :model-value="usagePercent(item)"
                  :color="usagePercent(item) > 80 ? 'error' : usagePercent(item) > 60 ? 'warning' : 'primary'"
                  height="6"
                  rounded
                />
                <div class="text-caption text-medium-emphasis">
                  {{ formatNumber(item.events_this_month) }} / {{ formatNumber(item.monthly_event_limit) }}
                </div>
              </div>
            </template>

            <template #item.trial_expires_at="{ item }">
              {{ item.trial_expires_at ? formatDate(item.trial_expires_at) : '-' }}
            </template>

            <template #item.actions="{ item }">
              <v-btn size="small" variant="text" icon="mdi-eye" @click="openDetail(item)" />
              <v-menu>
                <template #activator="{ props }">
                  <v-btn size="small" variant="text" icon="mdi-dots-vertical" v-bind="props" />
                </template>
                <v-list density="compact">
                  <v-list-item @click="openTierDialog(item)">
                    <v-list-item-title>Change Tier</v-list-item-title>
                  </v-list-item>
                  <v-list-item
                    v-if="item.status === 'trial'"
                    @click="openExtendDialog(item)"
                  >
                    <v-list-item-title>Extend Trial</v-list-item-title>
                  </v-list-item>
                  <v-list-item
                    v-if="item.status !== 'suspended'"
                    @click="confirmSuspend(item)"
                  >
                    <v-list-item-title>Suspend</v-list-item-title>
                  </v-list-item>
                  <v-list-item
                    v-if="item.status === 'suspended'"
                    @click="reactivate(item)"
                  >
                    <v-list-item-title>Reactivate</v-list-item-title>
                  </v-list-item>
                  <v-list-item @click="confirmRevoke(item)">
                    <v-list-item-title class="text-error">Revoke</v-list-item-title>
                  </v-list-item>
                </v-list>
              </v-menu>
            </template>
          </v-data-table>
        </v-card>
      </v-col>
    </v-row>

    <!-- Detail Dialog -->
    <v-dialog v-model="detailOpen" max-width="700">
      <v-card v-if="adminStore.selectedTenant">
        <v-card-title>
          {{ adminStore.selectedTenant.name }}
          <v-chip :color="tierColor(adminStore.selectedTenant.tier)" size="small" class="ml-2">
            {{ adminStore.selectedTenant.tier }}
          </v-chip>
          <v-chip :color="statusColor(adminStore.selectedTenant.status)" size="small" class="ml-1">
            {{ adminStore.selectedTenant.status }}
          </v-chip>
        </v-card-title>
        <v-card-text>
          <v-row>
            <v-col cols="6">
              <div class="text-caption text-medium-emphasis">Monthly Events</div>
              <div>{{ formatNumber(adminStore.selectedTenant.events_this_month) }} / {{ formatNumber(adminStore.selectedTenant.monthly_event_limit) }}</div>
            </v-col>
            <v-col cols="6">
              <div class="text-caption text-medium-emphasis">Pipeline Limit</div>
              <div>{{ adminStore.selectedTenant.pipeline_limit }}</div>
            </v-col>
            <v-col cols="6">
              <div class="text-caption text-medium-emphasis">Events/sec Limit</div>
              <div>{{ formatNumber(adminStore.selectedTenant.events_per_second_limit) }}</div>
            </v-col>
            <v-col cols="6">
              <div class="text-caption text-medium-emphasis">Trial Expires</div>
              <div>{{ adminStore.selectedTenant.trial_expires_at ? formatDate(adminStore.selectedTenant.trial_expires_at) : 'N/A' }}</div>
            </v-col>
          </v-row>

          <h3 class="text-subtitle-1 mt-4 mb-2">Pipelines ({{ adminStore.selectedTenant.pipelines.length }})</h3>
          <v-list v-if="adminStore.selectedTenant.pipelines.length" density="compact">
            <v-list-item
              v-for="p in adminStore.selectedTenant.pipelines"
              :key="p.id"
            >
              <v-list-item-title>{{ p.name }}</v-list-item-title>
              <template #append>
                <v-chip :color="p.status === 'deployed' ? 'success' : 'default'" size="x-small">
                  {{ p.status }}
                </v-chip>
              </template>
            </v-list-item>
          </v-list>
          <div v-else class="text-body-2 text-medium-emphasis">No pipelines</div>

          <h3 class="text-subtitle-1 mt-4 mb-2">API Keys ({{ adminStore.selectedTenant.api_keys.length }})</h3>
          <v-list v-if="adminStore.selectedTenant.api_keys.length" density="compact">
            <v-list-item
              v-for="k in adminStore.selectedTenant.api_keys"
              :key="k.id"
            >
              <v-list-item-title>{{ k.name }}</v-list-item-title>
              <template #append>
                <span class="text-caption text-medium-emphasis">
                  {{ k.last_used_at ? formatDate(k.last_used_at) : 'never used' }}
                </span>
              </template>
            </v-list-item>
          </v-list>
          <div v-else class="text-body-2 text-medium-emphasis">No API keys</div>
        </v-card-text>
        <v-card-actions>
          <v-spacer />
          <v-btn @click="detailOpen = false">Close</v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>

    <!-- Tier Change Dialog -->
    <v-dialog v-model="tierDialogOpen" max-width="400">
      <v-card>
        <v-card-title>Change Tier</v-card-title>
        <v-card-text>
          <p class="mb-4">Change tier for <strong>{{ actionTarget?.name }}</strong></p>
          <v-select
            v-model="selectedTier"
            :items="['free', 'pro', 'business', 'enterprise']"
            label="Tier"
            variant="outlined"
          />
        </v-card-text>
        <v-card-actions>
          <v-spacer />
          <v-btn @click="tierDialogOpen = false">Cancel</v-btn>
          <v-btn color="primary" @click="doChangeTier">Save</v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>

    <!-- Extend Trial Dialog -->
    <v-dialog v-model="extendDialogOpen" max-width="400">
      <v-card>
        <v-card-title>Extend Trial</v-card-title>
        <v-card-text>
          <p class="mb-4">Extend trial for <strong>{{ actionTarget?.name }}</strong></p>
          <v-text-field
            v-model="extendDays"
            type="number"
            label="Additional days"
            variant="outlined"
          />
        </v-card-text>
        <v-card-actions>
          <v-spacer />
          <v-btn @click="extendDialogOpen = false">Cancel</v-btn>
          <v-btn color="primary" @click="doExtendTrial">Extend</v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>
  </v-container>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useAdminStore, type Tenant } from '@/stores/admin'

const adminStore = useAdminStore()
const search = ref('')
const detailOpen = ref(false)
const tierDialogOpen = ref(false)
const extendDialogOpen = ref(false)
const actionTarget = ref<Tenant | null>(null)
const selectedTier = ref('free')
const extendDays = ref(30)

const headers = [
  { title: 'Name', key: 'name' },
  { title: 'Tier', key: 'tier' },
  { title: 'Status', key: 'status' },
  { title: 'Usage', key: 'usage', sortable: false },
  { title: 'Trial Expires', key: 'trial_expires_at' },
  { title: 'Actions', key: 'actions', sortable: false },
]

function tierColor(tier: string) {
  switch (tier) {
    case 'enterprise': return 'purple'
    case 'business': return 'indigo'
    case 'pro': return 'primary'
    default: return 'default'
  }
}

function statusColor(status: string) {
  switch (status) {
    case 'active': return 'success'
    case 'trial': return 'info'
    case 'suspended': return 'warning'
    case 'revoked': return 'error'
    default: return 'default'
  }
}

function usagePercent(item: Tenant) {
  if (!item.monthly_event_limit) return 0
  return (item.events_this_month / item.monthly_event_limit) * 100
}

function formatNumber(n: number): string {
  if (n >= 1_000_000_000) return (n / 1_000_000_000).toFixed(1) + 'B'
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M'
  if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K'
  return n.toString()
}

function formatDate(iso: string): string {
  return new Date(iso).toLocaleDateString()
}

async function openDetail(item: Tenant) {
  await adminStore.fetchTenantDetail(item.id)
  detailOpen.value = true
}

function openTierDialog(item: Tenant) {
  actionTarget.value = item
  selectedTier.value = item.tier
  tierDialogOpen.value = true
}

function openExtendDialog(item: Tenant) {
  actionTarget.value = item
  extendDays.value = 30
  extendDialogOpen.value = true
}

async function doChangeTier() {
  if (actionTarget.value) {
    await adminStore.changeTier(actionTarget.value.id, selectedTier.value)
  }
  tierDialogOpen.value = false
}

async function doExtendTrial() {
  if (actionTarget.value) {
    const expires = new Date()
    expires.setDate(expires.getDate() + extendDays.value)
    await adminStore.extendTrial(actionTarget.value.id, expires.toISOString())
  }
  extendDialogOpen.value = false
}

async function confirmSuspend(item: Tenant) {
  await adminStore.changeStatus(item.id, 'suspended')
}

async function reactivate(item: Tenant) {
  await adminStore.changeStatus(item.id, 'active')
}

async function confirmRevoke(item: Tenant) {
  await adminStore.revokeTenant(item.id)
}

onMounted(() => {
  adminStore.fetchTenants()
  adminStore.fetchUsageSummary()
})
</script>
