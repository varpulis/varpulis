<script lang="ts">
export default { name: 'ExampleSelector' }
</script>

<script setup lang="ts">
import { ref, onMounted, computed } from 'vue'
import { listExamples, getExample, type PlaygroundExample, type PlaygroundExampleDetail } from '@/api/playground'

const emit = defineEmits<{
  (e: 'select', example: PlaygroundExampleDetail): void
}>()

const examples = ref<PlaygroundExample[]>([])
const loading = ref(false)
const selectedId = ref<string | null>(null)

const categories = computed(() => {
  const cats = new Set(examples.value.map(e => e.category))
  return Array.from(cats)
})

const groupedExamples = computed(() => {
  const groups: Record<string, PlaygroundExample[]> = {}
  for (const ex of examples.value) {
    if (!groups[ex.category]) groups[ex.category] = []
    groups[ex.category].push(ex)
  }
  return groups
})

onMounted(async () => {
  try {
    examples.value = await listExamples()
  } catch (err) {
    console.error('Failed to load examples:', err)
  }
})

async function selectExample(id: string) {
  selectedId.value = id
  loading.value = true
  try {
    const detail = await getExample(id)
    emit('select', detail)
  } catch (err) {
    console.error('Failed to load example:', err)
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <v-menu>
    <template v-slot:activator="{ props }">
      <v-btn
        v-bind="props"
        variant="outlined"
        size="small"
        :loading="loading"
        prepend-icon="mdi-book-open-variant"
      >
        {{ selectedId ? examples.find(e => e.id === selectedId)?.name || 'Examples' : 'Examples' }}
        <v-icon end>mdi-chevron-down</v-icon>
      </v-btn>
    </template>
    <v-list density="compact" max-width="400">
      <template v-for="cat in categories" :key="cat">
        <v-list-subheader>{{ cat }}</v-list-subheader>
        <v-list-item
          v-for="ex in groupedExamples[cat]"
          :key="ex.id"
          :active="ex.id === selectedId"
          @click="selectExample(ex.id)"
        >
          <v-list-item-title>{{ ex.name }}</v-list-item-title>
          <v-list-item-subtitle>{{ ex.description }}</v-list-item-subtitle>
        </v-list-item>
      </template>
    </v-list>
  </v-menu>
</template>
