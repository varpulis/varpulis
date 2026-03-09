<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import PublicLayout from '@/components/common/PublicLayout.vue'
import { docCategories } from '@/data/docs'
import { useSeoMeta } from '@/composables/useSeoMeta'

const route = useRoute()
const router = useRouter()

const slug = computed(() => {
  const p = route.params.slug
  if (Array.isArray(p)) return p.join('/')
  return p || ''
})
const loading = ref(false)
const rawContent = ref('')
const error = ref('')

const currentPage = computed(() => {
  for (const cat of docCategories) {
    const page = cat.pages.find(p => p.slug === slug.value)
    if (page) return page
  }
  return null
})

useSeoMeta({
  title: currentPage.value ? `${currentPage.value.title} - Docs` : 'Documentation',
  description: 'Varpulis CEP engine documentation — tutorials, language reference, architecture guides, and API reference.',
})

function simpleMarkdownToHtml(md: string): string {
  if (!md) return ''
  if (md.trim().startsWith('<')) return md
  return md
    .replace(/```(\w*)\n([\s\S]*?)```/g, '<pre><code>$2</code></pre>')
    .replace(/^### (.+)$/gm, '<h3>$1</h3>')
    .replace(/^## (.+)$/gm, '<h2>$1</h2>')
    .replace(/^# (.+)$/gm, '<h1>$1</h1>')
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/\*(.+?)\*/g, '<em>$1</em>')
    .replace(/`([^`]+)`/g, '<code>$1</code>')
    .replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2">$1</a>')
    .replace(/^\| (.+) \|$/gm, (_, row: string) => {
      const cells = row.split(' | ').map(c => c.trim())
      return '<tr>' + cells.map(c => `<td>${c}</td>`).join('') + '</tr>'
    })
    .replace(/^- (.+)$/gm, '<li>$1</li>')
    .replace(/((?:<li>.*<\/li>\n?)+)/g, '<ul>$1</ul>')
    .replace(/\n\n/g, '\n</p>\n<p>\n')
}

const renderedContent = computed(() => simpleMarkdownToHtml(rawContent.value))

async function loadDoc(docSlug: string) {
  if (!docSlug) {
    rawContent.value = ''
    return
  }
  loading.value = true
  error.value = ''
  try {
    const res = await fetch(`/docs/${docSlug}.md`)
    if (res.ok) {
      rawContent.value = await res.text()
    } else {
      error.value = 'Documentation page not found.'
      rawContent.value = ''
    }
  } catch {
    error.value = 'Failed to load documentation.'
    rawContent.value = ''
  } finally {
    loading.value = false
  }
}

function navigateTo(docSlug: string) {
  router.push(`/docs/${docSlug}`)
}

watch(() => slug.value, (newSlug) => {
  loadDoc(newSlug)
}, { immediate: true })
</script>

<template>
  <PublicLayout>
    <v-container fluid class="pa-0" style="max-width: 100%">
      <v-row no-gutters>
        <!-- Sidebar -->
        <v-col cols="12" md="3" lg="2">
          <div class="docs-sidebar" style="position: sticky; top: 64px; height: calc(100vh - 64px); overflow-y: auto">
            <v-list density="compact" nav class="pa-2">
              <template v-for="cat in docCategories" :key="cat.name">
                <v-list-subheader class="text-uppercase font-weight-bold mt-2">
                  <v-icon size="small" class="mr-1">{{ cat.icon }}</v-icon>
                  {{ cat.name }}
                </v-list-subheader>
                <v-list-item
                  v-for="page in cat.pages"
                  :key="page.slug"
                  :active="slug === page.slug"
                  color="primary"
                  density="compact"
                  @click="navigateTo(page.slug)"
                >
                  <v-list-item-title class="text-body-2">{{ page.title }}</v-list-item-title>
                </v-list-item>
              </template>
            </v-list>
          </div>
        </v-col>

        <!-- Content -->
        <v-col>
          <div class="pa-6 pa-md-8" style="max-width: 900px">
            <template v-if="!slug">
              <h1 class="text-h3 font-weight-bold mb-4">Documentation</h1>
              <p class="text-body-1 text-medium-emphasis mb-8">
                Everything you need to build real-time pattern detection pipelines with Varpulis.
              </p>

              <v-row>
                <v-col v-for="cat in docCategories" :key="cat.name" cols="12" sm="6" md="4">
                  <v-card
                    variant="outlined"
                    class="pa-4 h-100 doc-cat-card"
                    style="cursor: pointer"
                    @click="navigateTo(cat.pages[0].slug)"
                  >
                    <v-icon size="28" color="primary" class="mb-2">{{ cat.icon }}</v-icon>
                    <h3 class="text-subtitle-1 font-weight-bold mb-1">{{ cat.name }}</h3>
                    <p class="text-body-2 text-medium-emphasis">{{ cat.pages.length }} pages</p>
                  </v-card>
                </v-col>
              </v-row>
            </template>

            <template v-else>
              <v-progress-linear v-if="loading" indeterminate class="mb-4" />

              <v-alert v-if="error" type="warning" variant="tonal" class="mb-4">
                {{ error }}
                <template #append>
                  <v-btn
                    variant="text"
                    size="small"
                    :href="`https://github.com/varpulis/varpulis/blob/main/docs/${slug}.md`"
                    target="_blank"
                  >
                    View on GitHub
                  </v-btn>
                </template>
              </v-alert>

              <div v-if="currentPage" class="mb-4">
                <v-breadcrumbs density="compact" class="px-0">
                  <v-breadcrumbs-item to="/docs">Docs</v-breadcrumbs-item>
                  <v-breadcrumbs-divider>/</v-breadcrumbs-divider>
                  <v-breadcrumbs-item>{{ currentPage.category }}</v-breadcrumbs-item>
                  <v-breadcrumbs-divider>/</v-breadcrumbs-divider>
                  <v-breadcrumbs-item>{{ currentPage.title }}</v-breadcrumbs-item>
                </v-breadcrumbs>
              </div>

              <div v-if="renderedContent" class="article-body" v-html="renderedContent" />
            </template>
          </div>
        </v-col>
      </v-row>
    </v-container>
  </PublicLayout>
</template>

<style>
.doc-cat-card:hover {
  border-color: rgb(var(--v-theme-primary));
  transition: border-color 0.2s ease;
}
</style>
