<script lang="ts">
export default { name: 'EventPanel' }
</script>

<script setup lang="ts">
import { computed } from 'vue'

const model = defineModel<string>({ required: true })

const lineCount = computed(() => {
  if (!model.value) return 0
  return model.value
    .split('\n')
    .filter(l => {
      const t = l.trim()
      return t.length > 0 && !t.startsWith('#') && !t.startsWith('//')
    })
    .length
})
</script>

<template>
  <div class="event-panel">
    <div class="d-flex align-center justify-space-between mb-2">
      <span class="text-subtitle-2">
        <v-icon size="small" class="mr-1">mdi-file-document-outline</v-icon>
        Input Events (.evt)
        <v-chip size="x-small" class="ml-1">{{ lineCount }}</v-chip>
      </span>
    </div>

    <v-textarea
      v-model="model"
      variant="outlined"
      density="compact"
      rows="10"
      auto-grow
      hide-details
      class="mono-text"
      placeholder='# .evt format — one event per line
@0s TempReading { sensor_id: "S1", temperature: 35 }
@1s TempReading { sensor_id: "S2", temperature: 22 }'
    />

    <div class="text-caption text-medium-emphasis mt-1">
      Format: <code>@&lt;time&gt; EventType { field: value, ... }</code>
    </div>
  </div>
</template>

<style scoped>
.event-panel {
  height: 100%;
  overflow-y: auto;
}
.mono-text :deep(textarea) {
  font-family: 'JetBrains Mono', 'Fira Code', 'Consolas', monospace !important;
  font-size: 0.8rem !important;
  line-height: 1.5 !important;
}
</style>
