<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { VueFlow, Position } from '@vue-flow/core'
import { Background } from '@vue-flow/background'
import type { Node, Edge } from '@vue-flow/core'

const props = defineProps<{
  compromisedHosts: string[]
  attackPaths: Array<{ from: string; to: string }>
}>()

interface HostDef {
  id: string
  label: string
  icon: string
  position: { x: number; y: number }
}

const hosts: HostDef[] = [
  { id: 'internet', label: 'Internet', icon: 'mdi-cloud', position: { x: 400, y: 0 } },
  { id: 'firewall', label: 'Firewall', icon: 'mdi-shield', position: { x: 400, y: 120 } },
  { id: 'web-server-01', label: 'web-server-01', icon: 'mdi-server', position: { x: 200, y: 250 } },
  { id: 'file-server-02', label: 'file-server-02', icon: 'mdi-database', position: { x: 600, y: 250 } },
  { id: 'workstation-15', label: 'workstation-15', icon: 'mdi-monitor', position: { x: 100, y: 400 } },
  { id: 'dev-box-03', label: 'dev-box-03', icon: 'mdi-laptop', position: { x: 500, y: 400 } },
]

interface LinkDef {
  source: string
  target: string
}

const links: LinkDef[] = [
  { source: 'internet', target: 'firewall' },
  { source: 'firewall', target: 'web-server-01' },
  { source: 'firewall', target: 'file-server-02' },
  { source: 'web-server-01', target: 'workstation-15' },
  { source: 'web-server-01', target: 'file-server-02' },
  { source: 'file-server-02', target: 'dev-box-03' },
]

function isCompromised(hostId: string): boolean {
  return props.compromisedHosts.includes(hostId)
}

function isAttackPath(source: string, target: string): boolean {
  return props.attackPaths.some(
    (p) => (p.from === source && p.to === target) || (p.from === target && p.to === source),
  )
}

const computedElements = computed(() => {
  const nodeList: Node[] = hosts.map((host) => ({
    id: host.id,
    type: 'network',
    position: host.position,
    data: {
      label: host.label,
      icon: host.icon,
      compromised: isCompromised(host.id),
    },
    sourcePosition: Position.Bottom,
    targetPosition: Position.Top,
  }))

  const edgeList: Edge[] = links.map((link, idx) => {
    const attack = isAttackPath(link.source, link.target)
    return {
      id: `link-${idx}`,
      source: link.source,
      target: link.target,
      type: 'smoothstep',
      animated: attack,
      style: {
        stroke: attack ? '#FF5252' : '#666',
        strokeWidth: attack ? 3 : 1.5,
      },
    }
  })

  return { nodes: nodeList, edges: edgeList }
})

const nodes = ref<Node[]>([])
const edges = ref<Edge[]>([])

watch(
  computedElements,
  (val) => {
    nodes.value = val.nodes
    edges.value = val.edges
  },
  { immediate: true },
)
</script>

<template>
  <div class="network-map-container">
    <VueFlow
      v-model:nodes="nodes"
      v-model:edges="edges"
      class="network-map-flow"
      :default-viewport="{ zoom: 0.9, x: 50, y: 20 }"
      :min-zoom="0.3"
      :max-zoom="2"
      :nodes-draggable="false"
      :nodes-connectable="false"
      :pan-on-drag="true"
      :zoom-on-scroll="false"
      fit-view-on-init
    >
      <Background pattern-color="#222" :gap="30" />

      <!-- Custom network node template -->
      <template #node-network="{ data }">
        <div
          class="network-node"
          :class="{ 'network-node--compromised': data.compromised }"
        >
          <div class="network-node__icon-wrap">
            <v-icon
              :icon="data.icon"
              size="28"
              :color="data.compromised ? '#FF5252' : 'white'"
            />
            <v-icon
              v-if="data.compromised"
              icon="mdi-skull-crossbones"
              size="14"
              color="#FF5252"
              class="network-node__skull-badge"
            />
          </div>
          <div class="network-node__label">{{ data.label }}</div>
        </div>
      </template>
    </VueFlow>
  </div>
</template>

<style scoped>
.network-map-container {
  height: 100%;
  width: 100%;
  min-height: 400px;
  background: transparent;
  border-radius: 8px;
}

.network-map-flow {
  height: 100%;
  width: 100%;
}

.network-node {
  display: flex;
  flex-direction: column;
  align-items: center;
  padding: 12px 16px;
  border-radius: 12px;
  border: 2px solid #444;
  background: #1e1e1e;
  min-width: 120px;
  text-align: center;
  transition: border-color 0.3s ease, box-shadow 0.3s ease;
}

.network-node--compromised {
  border-color: #FF5252;
  box-shadow: 0 0 16px rgba(255, 82, 82, 0.5), 0 0 40px rgba(255, 82, 82, 0.2);
  animation: compromised-glow 1.5s ease-in-out infinite;
}

.network-node__icon-wrap {
  position: relative;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 44px;
  height: 44px;
  margin-bottom: 6px;
}

.network-node__skull-badge {
  position: absolute;
  top: -4px;
  right: -8px;
}

.network-node__label {
  font-size: 11px;
  font-weight: 500;
  color: #ccc;
  white-space: nowrap;
  letter-spacing: 0.02em;
}

.network-node--compromised .network-node__label {
  color: #FF5252;
}

@keyframes compromised-glow {
  0%, 100% {
    box-shadow: 0 0 8px rgba(255, 82, 82, 0.4);
  }
  50% {
    box-shadow: 0 0 24px rgba(255, 82, 82, 0.9), 0 0 48px rgba(255, 82, 82, 0.3);
  }
}
</style>

<style>
/* Vue Flow overrides for network nodes */
.vue-flow__node-network {
  padding: 0;
  border-radius: 12px;
  font-size: 12px;
  background: transparent;
  border: none;
}
</style>
