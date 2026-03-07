import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import api from '@/api'

export interface Tenant {
  id: string
  name: string
  tier: string
  status: string
  org_type?: string
  parent_org_id?: string | null
  slug?: string
  db_schema?: string | null
  trial_expires_at: string | null
  pipeline_limit: number
  events_per_second_limit: number
  monthly_event_limit: number
  events_this_month: number
  notes: string
  created_at: string
  updated_at: string
}

export interface SubTenantInfo {
  id: string
  name: string
  slug?: string
  status: string
  org_type: string
}

export interface TenantDetail extends Tenant {
  stripe_customer_id: string | null
  sub_tenants?: SubTenantInfo[]
  pipelines: { id: string; name: string; status: string; read_only?: boolean; scope_level?: string; created_at: string }[]
  api_keys: { id: string; name: string; created_at: string; last_used_at: string | null }[]
}

export interface UsageSummary {
  total_tenants: number
  active_trials: number
  paid_customers: number
  suspended: number
  total_events_this_month: number
}

export interface GlobalPipeline {
  id: string
  name: string
  vpl_source: string
  status: string
  tenant_count: number
  deployed_by: string | null
  created_at: string
  updated_at: string
}

export const useAdminStore = defineStore('admin', () => {
  const tenants = ref<Tenant[]>([])
  const selectedTenant = ref<TenantDetail | null>(null)
  const usageSummary = ref<UsageSummary | null>(null)
  const globalPipelines = ref<GlobalPipeline[]>([])
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
      const res = await api.get('/admin/tenants')
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
      const res = await api.get(`/admin/tenants/${orgId}`)
      selectedTenant.value = res.data
    } catch (e: unknown) {
      error.value = e instanceof Error ? e.message : 'Failed to load tenant'
      selectedTenant.value = null
    } finally {
      loading.value = false
    }
  }

  async function changeTier(orgId: string, tier: string) {
    await api.put(`/admin/tenants/${orgId}/tier`, { tier })
    await fetchTenants()
  }

  async function changeStatus(orgId: string, status: string) {
    await api.put(`/admin/tenants/${orgId}/status`, { status })
    await fetchTenants()
  }

  async function extendTrial(orgId: string, expiresAt: string) {
    await api.put(`/admin/tenants/${orgId}/trial`, { expires_at: expiresAt })
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
    await api.put(`/admin/tenants/${orgId}/limits`, limits)
    await fetchTenants()
  }

  async function revokeTenant(orgId: string) {
    await api.post(`/admin/tenants/${orgId}/revoke`)
    await fetchTenants()
  }

  async function createTenant(data: {
    name: string
    admin_username: string
    admin_password: string
    admin_email: string
    tier?: string
  }) {
    const res = await api.post('/admin/tenants', data)
    await fetchTenants()
    await fetchUsageSummary()
    return res.data as {
      tenant: { id: string; name: string; tier: string; status: string }
      admin_user: { id: string; username: string }
      api_key: string
    }
  }

  async function fetchUsageSummary() {
    try {
      const res = await api.get('/admin/usage')
      usageSummary.value = res.data
    } catch {
      usageSummary.value = null
    }
  }

  async function fetchGlobalPipelines() {
    try {
      const res = await api.get('/admin/global-pipelines')
      globalPipelines.value = res.data.global_pipelines ?? []
    } catch {
      globalPipelines.value = []
    }
  }

  async function deployGlobalPipeline(name: string, vplSource: string) {
    const res = await api.post('/admin/global-pipelines', { name, vpl_source: vplSource })
    await fetchGlobalPipelines()
    return res.data as { template_id: string; name: string; copies_created: number }
  }

  async function updateGlobalPipeline(id: string, vplSource: string) {
    await api.put(`/admin/global-pipelines/${id}`, { vpl_source: vplSource })
    await fetchGlobalPipelines()
  }

  async function undeployGlobalPipeline(id: string) {
    await api.delete(`/admin/global-pipelines/${id}`)
    await fetchGlobalPipelines()
  }

  return {
    tenants,
    selectedTenant,
    usageSummary,
    globalPipelines,
    loading,
    error,
    activeTenants,
    trialTenants,
    suspendedTenants,
    fetchTenants,
    fetchTenantDetail,
    createTenant,
    changeTier,
    changeStatus,
    extendTrial,
    updateLimits,
    revokeTenant,
    fetchUsageSummary,
    fetchGlobalPipelines,
    deployGlobalPipeline,
    updateGlobalPipeline,
    undeployGlobalPipeline,
  }
})
