import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import type { ClusterConnector } from '@/api/cluster'
import * as clusterApi from '@/api/cluster'

export const useConnectorsStore = defineStore('connectors', () => {
  const connectors = ref<ClusterConnector[]>([])
  const loading = ref(false)
  const error = ref<string | null>(null)

  const connectorNames = computed(() => connectors.value.map((c) => c.name))

  async function fetchConnectors(): Promise<void> {
    loading.value = true
    error.value = null
    try {
      connectors.value = await clusterApi.listConnectors()
    } catch (e) {
      error.value = e instanceof Error ? e.message : 'Failed to fetch connectors'
    } finally {
      loading.value = false
    }
  }

  async function createConnector(connector: ClusterConnector): Promise<ClusterConnector | null> {
    error.value = null
    try {
      const created = await clusterApi.createConnector(connector)
      connectors.value.push(created)
      return created
    } catch (e) {
      error.value = e instanceof Error ? e.message : 'Failed to create connector'
      return null
    }
  }

  async function updateConnector(name: string, connector: ClusterConnector): Promise<ClusterConnector | null> {
    error.value = null
    try {
      const updated = await clusterApi.updateConnector(name, connector)
      const index = connectors.value.findIndex((c) => c.name === name)
      if (index !== -1) {
        connectors.value[index] = updated
      }
      return updated
    } catch (e) {
      error.value = e instanceof Error ? e.message : 'Failed to update connector'
      return null
    }
  }

  async function deleteConnector(name: string): Promise<boolean> {
    error.value = null
    try {
      await clusterApi.deleteConnector(name)
      connectors.value = connectors.value.filter((c) => c.name !== name)
      return true
    } catch (e) {
      error.value = e instanceof Error ? e.message : 'Failed to delete connector'
      return false
    }
  }

  function clearError(): void {
    error.value = null
  }

  return {
    connectors,
    connectorNames,
    loading,
    error,
    fetchConnectors,
    createConnector,
    updateConnector,
    deleteConnector,
    clearError,
  }
})
