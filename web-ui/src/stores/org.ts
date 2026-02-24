import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import axios from 'axios'

export interface Org {
  id: string
  name: string
  tier: string
  created_at?: string
}

export const useOrgStore = defineStore('org', () => {
  const organizations = ref<Org[]>([])
  const loading = ref(false)

  const currentOrg = computed(() => organizations.value[0] ?? null)

  async function loadOrgs() {
    loading.value = true
    try {
      const res = await axios.get('/api/v1/orgs')
      organizations.value = res.data.organizations ?? []
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
    loading,
    loadOrgs,
  }
})
