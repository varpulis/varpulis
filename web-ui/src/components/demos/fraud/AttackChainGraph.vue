<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { VueFlow, Position } from '@vue-flow/core'
import { Background } from '@vue-flow/background'
import type { Node, Edge } from '@vue-flow/core'

const props = defineProps<{
  activeEvents: string[]
  alertTypes: string[]
}>()

function isActive(eventType: string): boolean {
  return props.activeEvents.includes(eventType)
}

function isAlerted(alertType: string): boolean {
  return props.alertTypes.includes(alertType)
}

interface ChainRow {
  label: string
  alertType: string
  y: number
  events: Array<{ id: string; eventType: string; displayLabel: string }>
}

const chains: ChainRow[] = [
  {
    label: 'Account Takeover',
    alertType: 'account_takeover',
    y: 0,
    events: [
      { id: 'at-login', eventType: 'Login', displayLabel: 'Login' },
      { id: 'at-pwdchange', eventType: 'PasswordChange', displayLabel: 'PasswordChange' },
      { id: 'at-purchase', eventType: 'Purchase', displayLabel: 'Purchase' },
    ],
  },
  {
    label: 'Card Testing',
    alertType: 'card_testing',
    y: 200,
    events: [
      { id: 'ct-small1', eventType: 'SmallPurchase', displayLabel: 'SmallPurchase' },
      { id: 'ct-small2', eventType: 'SmallPurchase', displayLabel: 'SmallPurchase' },
      { id: 'ct-small3', eventType: 'SmallPurchase', displayLabel: 'SmallPurchase' },
      { id: 'ct-large', eventType: 'LargePurchase', displayLabel: 'LargePurchase' },
    ],
  },
  {
    label: 'Impossible Travel',
    alertType: 'impossible_travel',
    y: 400,
    events: [
      { id: 'it-login-us', eventType: 'Login', displayLabel: 'Login (US)' },
      { id: 'it-login-ng', eventType: 'Login', displayLabel: 'Login (NG)' },
    ],
  },
]

const computedElements = computed(() => {
  const nodeList: Node[] = []
  const edgeList: Edge[] = []

  for (const chain of chains) {
    // Row label node
    nodeList.push({
      id: `label-${chain.alertType}`,
      type: 'label',
      position: { x: 0, y: chain.y + 25 },
      data: { label: chain.label },
      selectable: false,
      draggable: false,
    })

    // Event nodes
    chain.events.forEach((evt, idx) => {
      const alerted = isAlerted(chain.alertType)
      const active = isActive(evt.eventType)

      nodeList.push({
        id: evt.id,
        type: 'chain',
        position: { x: 180 + idx * 250, y: chain.y },
        data: {
          label: evt.displayLabel,
          active,
          alerted,
        },
        sourcePosition: Position.Right,
        targetPosition: Position.Left,
      })

      // Edge to next node in the chain
      if (idx < chain.events.length - 1) {
        const nextEvt = chain.events[idx + 1]
        const bothActive = isActive(evt.eventType) && isActive(nextEvt.eventType)

        edgeList.push({
          id: `edge-${evt.id}-${nextEvt.id}`,
          source: evt.id,
          target: nextEvt.id,
          type: 'smoothstep',
          animated: bothActive || alerted,
          style: {
            stroke: alerted ? '#FF5252' : bothActive ? '#7C4DFF' : '#555',
            strokeWidth: 2,
            strokeDasharray: '6 4',
          },
        })
      }
    })
  }

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
  <div class="attack-chain-container">
    <VueFlow
      v-model:nodes="nodes"
      v-model:edges="edges"
      class="attack-chain-flow"
      :default-viewport="{ zoom: 0.85, x: 20, y: 30 }"
      :min-zoom="0.3"
      :max-zoom="2"
      :nodes-draggable="false"
      :nodes-connectable="false"
      :pan-on-drag="true"
      :zoom-on-scroll="false"
    >
      <Background pattern-color="#222" :gap="30" />

      <!-- Label node template -->
      <template #node-label="{ data }">
        <div class="chain-label">
          {{ data.label }}
        </div>
      </template>

      <!-- Chain node template -->
      <template #node-chain="{ data }">
        <div
          class="chain-node"
          :class="{
            'chain-node--active': data.active && !data.alerted,
            'chain-node--alert': data.alerted,
            'chain-node--default': !data.active && !data.alerted,
          }"
        >
          <span class="chain-node__label">{{ data.label }}</span>
        </div>
      </template>
    </VueFlow>
  </div>
</template>

<style scoped>
.attack-chain-container {
  height: 100%;
  width: 100%;
  min-height: 500px;
  background: transparent;
  border-radius: 8px;
}

.attack-chain-flow {
  height: 100%;
  width: 100%;
}

.chain-label {
  font-size: 14px;
  font-weight: 600;
  color: #9E9E9E;
  white-space: nowrap;
  padding: 4px 8px;
  user-select: none;
}

.chain-node {
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 12px 20px;
  border-radius: 10px;
  border: 2px solid;
  background: rgba(20, 20, 30, 0.9);
  min-width: 140px;
  text-align: center;
  transition: border-color 0.3s ease, box-shadow 0.3s ease, color 0.3s ease;
}

.chain-node--default {
  border-color: #333;
  color: #777;
}

.chain-node--active {
  border-color: #7C4DFF;
  color: #fff;
  box-shadow: 0 0 16px rgba(124, 77, 255, 0.5), 0 0 4px rgba(124, 77, 255, 0.3);
}

.chain-node--alert {
  border-color: #FF5252;
  color: #fff;
  animation: node-pulse 1.2s ease-in-out infinite;
}

.chain-node__label {
  font-size: 13px;
  font-weight: 500;
  letter-spacing: 0.02em;
}

@keyframes node-pulse {
  0%, 100% {
    box-shadow: 0 0 8px rgba(255, 82, 82, 0.4);
  }
  50% {
    box-shadow: 0 0 24px rgba(255, 82, 82, 0.9), 0 0 48px rgba(255, 82, 82, 0.3);
  }
}
</style>

<style>
/* Vue Flow overrides for chain nodes */
.vue-flow__node-chain {
  padding: 0;
  border-radius: 10px;
  font-size: 13px;
  background: transparent;
  border: none;
}

.vue-flow__node-label {
  padding: 0;
  background: transparent;
  border: none;
}
</style>
