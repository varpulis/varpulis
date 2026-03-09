<script setup lang="ts">
import { ref, computed } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import PublicLayout from '@/components/common/PublicLayout.vue'
import { blogPosts } from '@/data/blog'
import { useSeoMeta } from '@/composables/useSeoMeta'

useSeoMeta({
  title: 'Blog',
  description: 'Engineering insights, release announcements, and deep dives into Complex Event Processing with Varpulis.',
})

const router = useRouter()
const route = useRoute()
const activeTag = ref((route.query.tag as string) || '')

const allTags = computed(() => {
  const tags = new Set<string>()
  blogPosts.forEach(p => p.tags.forEach(t => tags.add(t)))
  return Array.from(tags).sort()
})

const filtered = computed(() => {
  if (!activeTag.value) return blogPosts
  return blogPosts.filter(p => p.tags.includes(activeTag.value))
})

function toggleTag(tag: string) {
  activeTag.value = activeTag.value === tag ? '' : tag
  router.replace({ query: activeTag.value ? { tag: activeTag.value } : {} })
}
</script>

<template>
  <PublicLayout>
    <v-container class="py-12" style="max-width: 900px">
      <h1 class="text-h3 font-weight-bold mb-2">Blog</h1>
      <p class="text-body-1 text-medium-emphasis mb-6">Engineering insights and release announcements.</p>

      <div class="d-flex ga-2 flex-wrap mb-8">
        <v-chip
          v-for="tag in allTags"
          :key="tag"
          :color="activeTag === tag ? 'primary' : undefined"
          variant="outlined"
          size="small"
          @click="toggleTag(tag)"
        >
          {{ tag }}
        </v-chip>
      </div>

      <div v-for="post in filtered" :key="post.slug" class="mb-8">
        <v-card
          variant="outlined"
          class="blog-card"
          @click="router.push(`/blog/${post.slug}`)"
          style="cursor: pointer"
        >
          <v-card-text>
            <div class="text-caption text-medium-emphasis mb-1">
              {{ new Date(post.date).toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' }) }}
              &middot; {{ post.author }}
            </div>
            <h2 class="text-h5 font-weight-bold mb-2">{{ post.title }}</h2>
            <p class="text-body-1 text-medium-emphasis mb-3">{{ post.summary }}</p>
            <div class="d-flex ga-2">
              <v-chip v-for="tag in post.tags" :key="tag" size="x-small" variant="tonal">{{ tag }}</v-chip>
            </div>
          </v-card-text>
        </v-card>
      </div>

      <div v-if="filtered.length === 0" class="text-center text-medium-emphasis py-12">
        No posts found for tag "{{ activeTag }}".
      </div>
    </v-container>
  </PublicLayout>
</template>

<style scoped>
.blog-card:hover {
  border-color: rgb(var(--v-theme-primary));
  transition: border-color 0.2s ease;
}
</style>
