<script setup lang="ts">
import { ref, computed, watch, onMounted } from 'vue'
import { VueFlow, useVueFlow, Position } from '@vue-flow/core'
import { Background } from '@vue-flow/background'
import { Controls } from '@vue-flow/controls'
import type { Node, Edge } from '@vue-flow/core'
import type { TopologyInfo, Worker } from '@/types/cluster'

const props = defineProps<{
  topology: TopologyInfo | null
  loading: boolean
}>()

const emit = defineEmits<{
  selectWorker: [worker: Worker]
}>()

const { fitView, onNodeClick } = useVueFlow()

const nodes = ref<Node[]>([])
const edges = ref<Edge[]>([])

// Convert topology to Vue Flow nodes and edges
const computedElements = computed(() => {
  if (!props.topology) {
    return { nodes: [], edges: [] }
  }

  const workerNodes: Node[] = props.topology.workers.map((worker, index) => {
    const col = index % 3
    const row = Math.floor(index / 3)

    return {
      id: worker.id,
      type: 'custom',
      position: { x: col * 300 + 50, y: row * 200 + 50 },
      data: {
        label: worker.id.substring(0, 8),
        address: worker.address,
        status: worker.status,
        pipelineGroups: worker.pipeline_groups,
      },
      sourcePosition: Position.Right,
      targetPosition: Position.Left,
    }
  })

  const routeEdges: Edge[] = props.topology.routes.map((route, index) => ({
    id: `route-${index}`,
    source: route.source_worker,
    target: route.target_worker,
    label: `${route.source_pipeline} → ${route.target_pipeline}`,
    type: 'smoothstep',
    animated: true,
    style: { stroke: '#7C4DFF' },
    labelStyle: { fill: '#9E9E9E', fontSize: 10 },
  }))

  return { nodes: workerNodes, edges: routeEdges }
})

watch(computedElements, (newVal) => {
  nodes.value = newVal.nodes
  edges.value = newVal.edges
  // Fit view after layout update
  setTimeout(() => fitView({ padding: 0.2 }), 100)
}, { immediate: true })

onMounted(() => {
  onNodeClick(({ node }) => {
    if (props.topology) {
      const worker = props.topology.workers.find((w) => w.id === node.id)
      if (worker) {
        emit('selectWorker', worker as unknown as Worker)
      }
    }
  })
})

// Status color helper
function getStatusColor(status: string): string {
  switch (status) {
    case 'Ready':
      return '#4CAF50'
    case 'Unhealthy':
      return '#FF5252'
    case 'Draining':
      return '#FFC107'
    default:
      return '#9E9E9E'
  }
}
</script>

<template>
  <div class="topology-container">
    <!-- Loading State -->
    <div v-if="loading && !topology" class="d-flex justify-center align-center h-100">
      <v-progress-circular indeterminate size="64" />
    </div>

    <!-- Empty State -->
    <div v-else-if="!topology || topology.workers.length === 0" class="d-flex flex-column justify-center align-center h-100">
      <v-icon size="64" color="grey-lighten-1" class="mb-4">
        mdi-graph-outline
      </v-icon>
      <div class="text-h6 mb-2">No Topology Data</div>
      <div class="text-body-2 text-medium-emphasis">
        Topology will be displayed when workers connect.
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
          class="custom-node"
          :style="{ borderColor: getStatusColor(data.status) }"
        >
          <div class="node-header" :style="{ backgroundColor: getStatusColor(data.status) }">
            <v-icon size="16" color="white">mdi-server</v-icon>
            <span class="ml-1 text-white">{{ data.label }}</span>
          </div>
          <div class="node-body">
            <div class="text-caption font-monospace">{{ data.address }}</div>
            <div class="text-caption text-medium-emphasis mt-1">
              {{ data.pipelineGroups?.length || 0 }} groups
            </div>
          </div>
        </div>
      </template>
    </VueFlow>
  </div>
</template>

<style scoped>
.topology-container {
  height: 600px;
  background: rgb(var(--v-theme-surface));
  border-radius: 4px;
}

.vue-flow-container {
  height: 100%;
  width: 100%;
}

.custom-node {
  background: rgb(var(--v-theme-surface));
  border: 2px solid;
  border-radius: 8px;
  min-width: 160px;
  cursor: pointer;
  transition: transform 0.2s ease, box-shadow 0.2s ease;
}

.custom-node:hover {
  transform: scale(1.05);
  box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
}

.node-header {
  display: flex;
  align-items: center;
  padding: 6px 10px;
  border-radius: 6px 6px 0 0;
}

.node-body {
  padding: 8px 10px;
}
</style>

<style>
/* Vue Flow default styles */
.vue-flow__node-custom {
  padding: 0;
  border-radius: 8px;
  font-size: 12px;
}

.vue-flow__edge-path {
  stroke-width: 2;
}

.vue-flow__controls {
  background: rgb(var(--v-theme-surface));
  border-radius: 4px;
}

.vue-flow__controls button {
  background: rgb(var(--v-theme-surface));
  color: rgb(var(--v-theme-on-surface));
  border-bottom: 1px solid rgb(var(--v-theme-surface-variant));
}

.vue-flow__controls button:hover {
  background: rgb(var(--v-theme-surface-light));
}
</style>
