<script setup lang="ts">
import { useRoute } from 'vue-router'
import { computed } from 'vue'

const route = useRoute()

// Demo views use DemoShell which has its own layout,
// so we only need to distinguish "never show chrome" pages
const wrapInContainer = computed(() => {
  const name = route.name as string
  return name === 'scenarios'
})
</script>

<template>
  <v-app>
    <v-main>
      <template v-if="wrapInContainer">
        <v-container fluid class="pa-4">
          <router-view />
        </v-container>
      </template>
      <template v-else>
        <router-view />
      </template>
    </v-main>
  </v-app>
</template>

<style>
/* Scrollbar styling for dark theme */
::-webkit-scrollbar {
  width: 8px;
  height: 8px;
}

::-webkit-scrollbar-track {
  background: transparent;
}

::-webkit-scrollbar-thumb {
  background: rgba(128, 128, 128, 0.5);
  border-radius: 4px;
}

::-webkit-scrollbar-thumb:hover {
  background: rgba(128, 128, 128, 0.7);
}

.v-table {
  overflow-x: auto;
}
</style>
