import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import axios from 'axios'

export interface Org {
  id: string
  name: string
  tier: string
  role?: string
  org_type?: string
  parent_org_id?: string | null
  slug?: string
  db_schema?: string | null
  created_at?: string
}

export interface OrgTreeNode {
  org: Org
  children: OrgTreeNode[]
  depth: number
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

  /** Build a tree from the flat org list based on parent_org_id */
  const orgTree = computed<OrgTreeNode[]>(() => {
    const byId = new Map<string, Org>()
    for (const o of organizations.value) byId.set(o.id, o)

    const childrenOf = new Map<string, Org[]>()
    const roots: Org[] = []

    for (const o of organizations.value) {
      if (o.parent_org_id && byId.has(o.parent_org_id)) {
        const siblings = childrenOf.get(o.parent_org_id) ?? []
        siblings.push(o)
        childrenOf.set(o.parent_org_id, siblings)
      } else {
        roots.push(o)
      }
    }

    function buildNode(org: Org, depth: number): OrgTreeNode {
      const kids = (childrenOf.get(org.id) ?? []).map((c) => buildNode(c, depth + 1))
      return { org, children: kids, depth }
    }

    return roots.map((r) => buildNode(r, 0))
  })

  /** Flatten the tree for list rendering with depth info */
  const flatOrgTree = computed<OrgTreeNode[]>(() => {
    const result: OrgTreeNode[] = []
    function walk(nodes: OrgTreeNode[]) {
      for (const n of nodes) {
        result.push(n)
        walk(n.children)
      }
    }
    walk(orgTree.value)
    return result
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
    orgTree,
    flatOrgTree,
    loading,
    loadOrgs,
    switchOrg,
  }
})
