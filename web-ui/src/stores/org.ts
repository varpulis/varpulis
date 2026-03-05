import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import axios from 'axios'

export interface Org {
  id: string
  name: string
  tier: string
  role?: string
  created_at?: string
}

const ACTIVE_ORG_KEY = 'varpulis_active_org'

export const useOrgStore = defineStore('org', () => {
  const organizations = ref<Org[]>([])
  const loading = ref(false)
  const activeOrgId = ref<string>(localStorage.getItem(ACTIVE_ORG_KEY) ?? '')

  const currentOrg = computed(() => {
    if (activeOrgId.value) {
      const found = organizations.value.find((o) => o.id === activeOrgId.value)
      if (found) return found
    }
    return organizations.value[0] ?? null
  })

  function switchOrg(orgId: string) {
    activeOrgId.value = orgId
    localStorage.setItem(ACTIVE_ORG_KEY, orgId)
  }

  async function loadOrgs() {
    loading.value = true
    try {
      const res = await axios.get('/api/v1/orgs')
      organizations.value = res.data.organizations ?? []
      // If no active org set yet, default to first
      if (!activeOrgId.value && organizations.value.length > 0) {
        switchOrg(organizations.value[0].id)
      }
    } catch {
      // Orgs endpoint not available (saas not enabled)
      organizations.value = []
    } finally {
      loading.value = false
    }
  }

  return {
    organizations,
    currentOrg,
    activeOrgId,
    loading,
    loadOrgs,
    switchOrg,
  }
})
