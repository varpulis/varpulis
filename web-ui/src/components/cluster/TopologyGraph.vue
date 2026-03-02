<script setup lang="ts">
import { ref, computed, watch, onMounted, nextTick } from 'vue'
import { VueFlow, useVueFlow, Position } from '@vue-flow/core'
import { Background } from '@vue-flow/background'
import { Controls } from '@vue-flow/controls'
import type { Node, Edge } from '@vue-flow/core'
import type { TopologyInfo, Worker } from '@/types/cluster'
import '@vue-flow/core/dist/style.css'
import '@vue-flow/core/dist/theme-default.css'
import '@vue-flow/controls/dist/style.css'

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

const computedElements = computed(() => {
  if (!props.topology) {
    return { nodes: [], edges: [] }
  }

  const count = props.topology.workers.length
  // Single row, evenly spaced horizontally, all at the same Y
  const nodeWidth = 130
  const gap = 40
  const totalWidth = count * nodeWidth + (count - 1) * gap
  const startX = Math.max(0, (800 - totalWidth) / 2)
  const y = 60

  const workerNodes: Node[] = props.topology.workers.map((worker, index) => ({
    id: worker.id,
    type: 'custom',
    position: { x: startX + index * (nodeWidth + gap), y },
    data: {
      label: worker.id,
      address: worker.address,
      status: worker.status,
      pipelineGroups: worker.pipeline_groups,
    },
    sourcePosition: Position.Bottom,
    targetPosition: Position.Top,
  }))

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

watch(computedElements, async (newVal) => {
  nodes.value = newVal.nodes
  edges.value = newVal.edges
  await nextTick()
  setTimeout(() => fitView({ padding: 0.3 }), 300)
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

function getStatusColor(status: string): string {
  switch (status.toLowerCase()) {
    case 'ready':
      return '#4CAF50'
    case 'unhealthy':
      return '#FF5252'
    case 'draining':
      return '#FFC107'
    default:
      return '#9E9E9E'
  }
}
</script>

<template>
  <div class="topology-container">
    <div v-if="loading && !topology" class="d-flex justify-center align-center h-100">
      <v-progress-circular indeterminate size="64" />
    </div>

    <div v-else-if="!topology || topology.workers.length === 0" class="d-flex flex-column justify-center align-center h-100">
      <v-icon size="64" color="grey-lighten-1" class="mb-4">
        mdi-graph-outline
      </v-icon>
      <div class="text-h6 mb-2">No Topology Data</div>
      <div class="text-body-2 text-medium-emphasis">
        Topology will be displayed when workers connect.
      </div>
    </div>

    <VueFlow
      v-else
      v-model:nodes="nodes"
      v-model:edges="edges"
      class="vue-flow-container"
      :min-zoom="0.5"
      :max-zoom="2"
      fit-view-on-init
    >
      <Background pattern-color="#333" :gap="20" />
      <Controls />

      <template #node-custom="{ data }">
        <div
          class="worker-node"
          :style="{ borderColor: getStatusColor(data.status) }"
        >
          <div class="worker-header" :style="{ backgroundColor: getStatusColor(data.status) }">
            <v-icon size="12" color="white">mdi-server</v-icon>
            <span class="ml-1 text-white text-caption font-weight-medium">{{ data.label }}</span>
          </div>
          <div class="worker-body">
            <div class="text-caption text-medium-emphasis" style="font-size: 10px;">
              {{ data.pipelineGroups?.length || 0 }} group(s)
            </div>
          </div>
        </div>
      </template>
    </VueFlow>
  </div>
</template>

<style scoped>
.topology-container {
  height: 400px;
  background: rgb(var(--v-theme-surface));
  border-radius: 4px;
}

.vue-flow-container {
  height: 100%;
  width: 100%;
}

.worker-node {
  background: rgb(var(--v-theme-surface));
  border: 2px solid;
  border-radius: 6px;
  width: 130px;
  cursor: pointer;
  transition: transform 0.15s ease, box-shadow 0.15s ease;
}

.worker-node:hover {
  transform: scale(1.05);
  box-shadow: 0 2px 12px rgba(0, 0, 0, 0.3);
}

.worker-header {
  display: flex;
  align-items: center;
  padding: 3px 8px;
  border-radius: 4px 4px 0 0;
}

.worker-body {
  padding: 4px 8px;
}
</style>

<style>
.vue-flow__node-custom {
  padding: 0;
  border-radius: 6px;
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
