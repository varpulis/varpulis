<script setup lang="ts">
import PublicLayout from '@/components/common/PublicLayout.vue'
import { changelogEntries } from '@/data/changelog'
import { useSeoMeta } from '@/composables/useSeoMeta'

useSeoMeta({
  title: 'Changelog',
  description: 'Release history and version notes for Varpulis CEP engine.',
})

function changeColor(type: string) {
  switch (type) {
    case 'added': return 'success'
    case 'fixed': return 'warning'
    case 'improved': return 'info'
    case 'changed': return 'secondary'
    default: return 'grey'
  }
}

function changeIcon(type: string) {
  switch (type) {
    case 'added': return 'mdi-plus-circle'
    case 'fixed': return 'mdi-wrench'
    case 'improved': return 'mdi-arrow-up-circle'
    case 'changed': return 'mdi-swap-horizontal'
    default: return 'mdi-circle'
  }
}
</script>

<template>
  <PublicLayout>
    <v-container class="py-12" style="max-width: 900px">
      <h1 class="text-h3 font-weight-bold mb-2">Changelog</h1>
      <p class="text-body-1 text-medium-emphasis mb-8">Release history and version notes.</p>

      <v-timeline side="end" density="compact">
        <v-timeline-item
          v-for="entry in changelogEntries"
          :key="entry.version"
          dot-color="primary"
          size="small"
        >
          <template #opposite>
            <div class="text-caption text-medium-emphasis">
              {{ new Date(entry.date).toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' }) }}
            </div>
          </template>

          <v-card variant="outlined" class="mb-4">
            <v-card-title class="d-flex align-center ga-2">
              <v-chip color="primary" size="small" variant="flat">v{{ entry.version }}</v-chip>
              <span class="text-h6">{{ entry.title }}</span>
            </v-card-title>
            <v-card-subtitle v-if="entry.highlights" class="pb-0">
              {{ entry.highlights }}
            </v-card-subtitle>
            <v-card-text>
              <v-list density="compact" class="pa-0">
                <v-list-item
                  v-for="(change, i) in entry.changes"
                  :key="i"
                  class="px-0"
                >
                  <template #prepend>
                    <v-icon :color="changeColor(change.type)" size="small" class="mr-2">
                      {{ changeIcon(change.type) }}
                    </v-icon>
                  </template>
                  <v-list-item-title class="text-body-2" style="white-space: normal">
                    {{ change.text }}
                  </v-list-item-title>
                </v-list-item>
              </v-list>
            </v-card-text>
          </v-card>
        </v-timeline-item>
      </v-timeline>
    </v-container>
  </PublicLayout>
</template>
