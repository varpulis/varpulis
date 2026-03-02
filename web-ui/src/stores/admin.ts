import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import axios from 'axios'

export interface Tenant {
  id: string
  name: string
  tier: string
  status: string
  trial_expires_at: string | null
  pipeline_limit: number
  events_per_second_limit: number
  monthly_event_limit: number
  events_this_month: number
  notes: string
  created_at: string
  updated_at: string
}

export interface TenantDetail extends Tenant {
  stripe_customer_id: string | null
  pipelines: { id: string; name: string; status: string; created_at: string }[]
  api_keys: { id: string; name: string; created_at: string; last_used_at: string | null }[]
}

export interface UsageSummary {
  total_tenants: number
  active_trials: number
  paid_customers: number
  suspended: number
  total_events_this_month: number
}

export const useAdminStore = defineStore('admin', () => {
  const tenants = ref<Tenant[]>([])
  const selectedTenant = ref<TenantDetail | null>(null)
  const usageSummary = ref<UsageSummary | null>(null)
  const loading = ref(false)
  const error = ref<string | null>(null)

  const activeTenants = computed(() => tenants.value.filter((t) => t.status === 'active'))
  const trialTenants = computed(() => tenants.value.filter((t) => t.status === 'trial'))
  const suspendedTenants = computed(
    () => tenants.value.filter((t) => t.status === 'suspended' || t.status === 'revoked'),
  )

  async function fetchTenants() {
    loading.value = true
    error.value = null
    try {
      const res = await axios.get('/api/v1/admin/tenants')
      tenants.value = res.data.tenants ?? []
    } catch (e: unknown) {
      error.value = e instanceof Error ? e.message : 'Failed to load tenants'
      tenants.value = []
    } finally {
      loading.value = false
    }
  }

  async function fetchTenantDetail(orgId: string) {
    loading.value = true
    error.value = null
    try {
      const res = await axios.get(`/api/v1/admin/tenants/${orgId}`)
      selectedTenant.value = res.data
    } catch (e: unknown) {
      error.value = e instanceof Error ? e.message : 'Failed to load tenant'
      selectedTenant.value = null
    } finally {
      loading.value = false
    }
  }

  async function changeTier(orgId: string, tier: string) {
    await axios.put(`/api/v1/admin/tenants/${orgId}/tier`, { tier })
    await fetchTenants()
  }

  async function changeStatus(orgId: string, status: string) {
    await axios.put(`/api/v1/admin/tenants/${orgId}/status`, { status })
    await fetchTenants()
  }

  async function extendTrial(orgId: string, expiresAt: string) {
    await axios.put(`/api/v1/admin/tenants/${orgId}/trial`, { expires_at: expiresAt })
    await fetchTenants()
  }

  async function updateLimits(
    orgId: string,
    limits: {
      pipeline_limit?: number
      events_per_second_limit?: number
      monthly_event_limit?: number
    },
  ) {
    await axios.put(`/api/v1/admin/tenants/${orgId}/limits`, limits)
    await fetchTenants()
  }

  async function revokeTenant(orgId: string) {
    await axios.post(`/api/v1/admin/tenants/${orgId}/revoke`)
    await fetchTenants()
  }

  async function fetchUsageSummary() {
    try {
      const res = await axios.get('/api/v1/admin/usage')
      usageSummary.value = res.data
    } catch {
      usageSummary.value = null
    }
  }

  return {
    tenants,
    selectedTenant,
    usageSummary,
    loading,
    error,
    activeTenants,
    trialTenants,
    suspendedTenants,
    fetchTenants,
    fetchTenantDetail,
    changeTier,
    changeStatus,
    extendTrial,
    updateLimits,
    revokeTenant,
    fetchUsageSummary,
  }
})
