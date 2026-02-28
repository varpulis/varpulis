<script setup lang="ts">
import { ref, computed, watch, onMounted } from 'vue'
import { VueFlow, useVueFlow, Position } from '@vue-flow/core'
import { Background } from '@vue-flow/background'
import { Controls } from '@vue-flow/controls'
import type { Node, Edge } from '@vue-flow/core'
import type { PipelineTopology, TopologyNode } from '@/api/pipelines'

const props = defineProps<{
  topology: PipelineTopology | null
  loading: boolean
}>()

const { fitView, onNodeClick } = useVueFlow()

const nodes = ref<Node[]>([])
const edges = ref<Edge[]>([])
const selectedNode = ref<TopologyNode | null>(null)
const dialogOpen = ref(false)

function getNodeColor(nodeType: string): string {
  switch (nodeType) {
    case 'Source': return '#4CAF50'
    case 'Stream': return '#7C4DFF'
    case 'Join': return '#FF9800'
    case 'Merge': return '#00BCD4'
    case 'Timer': return '#FFC107'
    case 'Sink': return '#F44336'
    default: return '#9E9E9E'
  }
}

function getNodeIcon(nodeType: string): string {
  switch (nodeType) {
    case 'Source': return 'mdi-import'
    case 'Stream': return 'mdi-pipe'
    case 'Join': return 'mdi-call-merge'
    case 'Merge': return 'mdi-source-merge'
    case 'Timer': return 'mdi-timer-outline'
    case 'Sink': return 'mdi-export'
    default: return 'mdi-circle'
  }
}

function getEdgeStyle(edgeType: string): Record<string, string> {
  switch (edgeType) {
    case 'EventType': return { stroke: '#7C4DFF' }
    case 'StreamOutput': return { stroke: '#4CAF50' }
    case 'MergeInput': return { stroke: '#00BCD4' }
    case 'JoinInput': return { stroke: '#FF9800' }
    default: return { stroke: '#9E9E9E' }
  }
}

const computedElements = computed(() => {
  if (!props.topology) {
    return { nodes: [], edges: [] }
  }

  const sourceIds = new Set(props.topology.sources)
  const sinkIds = new Set(props.topology.sinks)

  const flowNodes: Node[] = props.topology.nodes.map((node, index) => {
    const col = index % 4
    const row = Math.floor(index / 4)

    return {
      id: String(node.id),
      type: 'custom',
      position: { x: col * 280 + 50, y: row * 180 + 50 },
      data: {
        ...node,
        isSource: sourceIds.has(node.id),
        isSink: sinkIds.has(node.id),
      },
      sourcePosition: Position.Right,
      targetPosition: Position.Left,
    }
  })

  const flowEdges: Edge[] = props.topology.edges.map((edge, index) => ({
    id: `edge-${index}`,
    source: String(edge.from),
    target: String(edge.to),
    label: edge.label,
    type: 'smoothstep',
    animated: edge.edge_type === 'EventType',
    style: getEdgeStyle(edge.edge_type),
    labelStyle: { fill: '#9E9E9E', fontSize: 10 },
  }))

  return { nodes: flowNodes, edges: flowEdges }
})

watch(computedElements, (newVal) => {
  nodes.value = newVal.nodes
  edges.value = newVal.edges
  setTimeout(() => fitView({ padding: 0.2 }), 100)
}, { immediate: true })

onMounted(() => {
  onNodeClick(({ node }) => {
    if (props.topology) {
      const topoNode = props.topology.nodes.find((n) => n.id === Number(node.id))
      if (topoNode) {
        selectedNode.value = topoNode
        dialogOpen.value = true
      }
    }
  })
})
</script>

<template>
  <div class="topology-view">
    <!-- Loading State -->
    <div v-if="loading && !topology" class="d-flex justify-center align-center h-100">
      <v-progress-circular indeterminate size="64" />
    </div>

    <!-- Empty State -->
    <div v-else-if="!topology || topology.nodes.length === 0" class="d-flex flex-column justify-center align-center h-100">
      <v-icon size="64" color="grey-lighten-1" class="mb-4">
        mdi-graph-outline
      </v-icon>
      <div class="text-h6 mb-2">No Topology Data</div>
      <div class="text-body-2 text-medium-emphasis">
        Deploy a pipeline to view its processing topology.
      </div>
    </div>

    <!-- Vue Flow Graph -->
    <VueFlow
      v-else
      v-model:nodes="nodes"
      v-model:edges="edges"
      class="vue-flow-container"
      :default-viewport="{ zoom: 1 }"
      :min-zoom="0.2"
      :max-zoom="4"
      fit-view-on-init
    >
      <Background pattern-color="#333" :gap="20" />
      <Controls />

      <!-- Custom Node Template -->
      <template #node-custom="{ data }">
        <div
          class="topology-node"
          :style="{ borderColor: getNodeColor(data.node_type) }"
        >
          <div class="node-header" :style="{ backgroundColor: getNodeColor(data.node_type) }">
            <v-icon size="14" color="white">{{ getNodeIcon(data.node_type) }}</v-icon>
            <span class="ml-1 text-white text-caption">{{ data.name }}</span>
          </div>
          <div class="node-body">
            <div class="text-caption text-medium-emphasis">
              {{ data.node_type }} &middot; {{ data.operation_count }} ops
            </div>
            <div v-if="data.operation_summary" class="text-caption font-monospace mt-1" style="font-size: 10px; max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">
              {{ data.operation_summary }}
            </div>
          </div>
        </div>
      </template>
    </VueFlow>

    <!-- Node Detail Panel -->
    <v-dialog v-model="dialogOpen" max-width="400" v-if="selectedNode">
      <v-card>
        <v-card-title>
          <v-icon class="mr-2">{{ getNodeIcon(selectedNode.node_type) }}</v-icon>
          {{ selectedNode.name }}
        </v-card-title>
        <v-card-text>
          <v-list density="compact">
            <v-list-item>
              <template #prepend><v-icon size="small">mdi-tag</v-icon></template>
              <v-list-item-title>Type: {{ selectedNode.node_type }}</v-list-item-title>
            </v-list-item>
            <v-list-item>
              <template #prepend><v-icon size="small">mdi-counter</v-icon></template>
              <v-list-item-title>Operations: {{ selectedNode.operation_count }}</v-list-item-title>
            </v-list-item>
            <v-list-item v-if="selectedNode.operation_summary">
              <template #prepend><v-icon size="small">mdi-pipe</v-icon></template>
              <v-list-item-title class="font-monospace text-caption">{{ selectedNode.operation_summary }}</v-list-item-title>
            </v-list-item>
          </v-list>
          <v-divider class="my-2" />
          <div class="text-subtitle-2 mb-1">Metrics</div>
          <v-list density="compact">
            <v-list-item>
              <v-list-item-title>Events received: {{ selectedNode.metrics.events_received }}</v-list-item-title>
            </v-list-item>
            <v-list-item>
              <v-list-item-title>Events emitted: {{ selectedNode.metrics.events_emitted }}</v-list-item-title>
            </v-list-item>
            <v-list-item>
              <v-list-item-title>Events filtered: {{ selectedNode.metrics.events_filtered }}</v-list-item-title>
            </v-list-item>
          </v-list>
        </v-card-text>
        <v-card-actions>
          <v-spacer />
          <v-btn @click="dialogOpen = false">Close</v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>
  </div>
</template>

<style scoped>
.topology-view {
  height: 600px;
  background: rgb(var(--v-theme-surface));
  border-radius: 4px;
  position: relative;
}

.vue-flow-container {
  height: 100%;
  width: 100%;
}

.topology-node {
  background: rgb(var(--v-theme-surface));
  border: 2px solid;
  border-radius: 8px;
  min-width: 140px;
  cursor: pointer;
  transition: transform 0.2s ease, box-shadow 0.2s ease;
}

.topology-node:hover {
  transform: scale(1.05);
  box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
}

.node-header {
  display: flex;
  align-items: center;
  padding: 4px 8px;
  border-radius: 6px 6px 0 0;
}

.node-body {
  padding: 6px 8px;
}
</style>
