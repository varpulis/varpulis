<script setup lang="ts">
interface NavItem {
  title: string
  icon: string
  to: string
}

const props = defineProps<{
  modelValue: boolean
  rail: boolean
  items: NavItem[]
  currentRoute: string
}>()

const emit = defineEmits<{
  'update:modelValue': [value: boolean]
  navigate: [path: string]
}>()

function isActive(path: string): boolean {
  if (path === '/') {
    return props.currentRoute === '/'
  }
  return props.currentRoute.startsWith(path)
}
</script>

<template>
  <v-navigation-drawer
    :model-value="modelValue"
    :rail="rail"
    permanent
    @update:model-value="emit('update:modelValue', $event)"
  >
    <v-list density="compact" nav>
      <v-list-item
        v-for="item in items"
        :key="item.to"
        :active="isActive(item.to)"
        :prepend-icon="item.icon"
        :title="item.title"
        color="primary"
        @click="emit('navigate', item.to)"
      />
    </v-list>

    <template #append>
      <v-divider />
      <v-list density="compact" nav>
        <v-list-item
          prepend-icon="mdi-help-circle-outline"
          title="Help"
          href="https://github.com/your-org/varpulis/wiki"
          target="_blank"
        />
      </v-list>
      <div v-if="!rail" class="pa-3 text-caption text-medium-emphasis">
        Varpulis v0.2.0
      </div>
    </template>
  </v-navigation-drawer>
</template>
