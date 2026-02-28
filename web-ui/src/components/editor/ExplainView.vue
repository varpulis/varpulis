<script setup lang="ts">
import { ref, computed } from 'vue'
import { explainVpl } from '@/api/pipelines'

const props = defineProps<{
  vplSource: string
}>()

const loading = ref(false)
const error = ref<string | null>(null)
const logicalPlan = ref<string | null>(null)
const optimizedPlan = ref<string | null>(null)
const showOptimized = ref(true)

const displayPlan = computed(() => {
  if (showOptimized.value && optimizedPlan.value) {
    return optimizedPlan.value
  }
  return logicalPlan.value
})

async function runExplain(): Promise<void> {
  if (!props.vplSource.trim()) return

  loading.value = true
  error.value = null
  logicalPlan.value = null
  optimizedPlan.value = null

  try {
    const result = await explainVpl(props.vplSource)
    logicalPlan.value = result.logical_plan
    optimizedPlan.value = result.optimized_plan ?? null
  } catch (e) {
    error.value = e instanceof Error ? e.message : 'Failed to generate explain plan'
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <div class="explain-view">
    <div class="d-flex align-center mb-3">
      <v-btn
        color="primary"
        variant="tonal"
        size="small"
        prepend-icon="mdi-file-tree-outline"
        :loading="loading"
        :disabled="!vplSource.trim()"
        @click="runExplain"
      >
        EXPLAIN
      </v-btn>

      <v-btn-toggle
        v-if="logicalPlan && optimizedPlan"
        v-model="showOptimized"
        mandatory
        density="compact"
        class="ml-4"
      >
        <v-btn :value="false" size="small" variant="outlined">Logical</v-btn>
        <v-btn :value="true" size="small" variant="outlined">Optimized</v-btn>
      </v-btn-toggle>
    </div>

    <v-alert v-if="error" type="error" variant="tonal" density="compact" class="mb-3">
      {{ error }}
    </v-alert>

    <!-- Empty State -->
    <div v-if="!displayPlan && !loading" class="text-medium-emphasis text-body-2 pa-4 text-center">
      Click EXPLAIN to see the query execution plan.
    </div>

    <!-- Plan Output -->
    <pre
      v-if="displayPlan"
      class="plan-output pa-4"
    >{{ displayPlan }}</pre>
  </div>
</template>

<style scoped>
.explain-view {
  background: rgb(var(--v-theme-surface));
  border-radius: 4px;
}

.plan-output {
  background: rgba(0, 0, 0, 0.2);
  border-radius: 4px;
  font-family: 'JetBrains Mono', 'Fira Code', Consolas, monospace;
  font-size: 12px;
  line-height: 1.5;
  overflow-x: auto;
  white-space: pre;
  color: rgb(var(--v-theme-on-surface));
}
</style>
