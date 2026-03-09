<script setup lang="ts">
import { computed } from 'vue'
import { useRoute } from 'vue-router'
import PublicLayout from '@/components/common/PublicLayout.vue'
import { blogPosts } from '@/data/blog'
import { useSeoMeta } from '@/composables/useSeoMeta'

const route = useRoute()
const post = computed(() => blogPosts.find(p => p.slug === route.params.slug))

if (post.value) {
  useSeoMeta({
    title: post.value.title,
    description: post.value.summary,
    ogType: 'article',
  })
} else {
  useSeoMeta({
    title: 'Post Not Found',
    description: 'The requested blog post could not be found.',
  })
}
</script>

<template>
  <PublicLayout>
    <v-container class="py-12" style="max-width: 800px">
      <template v-if="post">
        <v-btn variant="text" size="small" prepend-icon="mdi-arrow-left" to="/blog" class="mb-4">
          Back to Blog
        </v-btn>

        <div class="text-caption text-medium-emphasis mb-2">
          {{ new Date(post.date).toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' }) }}
          &middot; {{ post.author }}
        </div>
        <h1 class="text-h3 font-weight-bold mb-4">{{ post.title }}</h1>
        <div class="d-flex ga-2 mb-6">
          <v-chip v-for="tag in post.tags" :key="tag" size="small" variant="tonal">{{ tag }}</v-chip>
        </div>

        <v-divider class="mb-6" />

        <div class="article-body" v-html="post.body" />
      </template>

      <template v-else>
        <div class="text-center py-16">
          <v-icon size="64" color="grey" class="mb-4">mdi-file-question</v-icon>
          <h2 class="text-h5 mb-4">Post not found</h2>
          <v-btn to="/blog" variant="outlined">Back to Blog</v-btn>
        </div>
      </template>
    </v-container>
  </PublicLayout>
</template>

<style>
.article-body h2 {
  font-size: 1.5rem;
  font-weight: 700;
  margin: 2rem 0 1rem;
}

.article-body h3 {
  font-size: 1.25rem;
  font-weight: 600;
  margin: 1.5rem 0 0.75rem;
}

.article-body p {
  line-height: 1.7;
  margin-bottom: 1rem;
}

.article-body ul,
.article-body ol {
  margin-bottom: 1rem;
  padding-left: 1.5rem;
}

.article-body li {
  line-height: 1.7;
  margin-bottom: 0.25rem;
}

.article-body pre {
  background: rgba(var(--v-theme-surface-variant), 0.3);
  border-radius: 8px;
  padding: 1rem;
  overflow-x: auto;
  margin-bottom: 1rem;
  font-family: 'JetBrains Mono', 'Fira Code', 'Consolas', monospace;
  font-size: 0.85rem;
  line-height: 1.5;
}

.article-body code {
  font-family: 'JetBrains Mono', 'Fira Code', 'Consolas', monospace;
  font-size: 0.9em;
}

.article-body p code {
  background: rgba(var(--v-theme-surface-variant), 0.3);
  padding: 0.15em 0.4em;
  border-radius: 4px;
}

.article-body a {
  color: rgb(var(--v-theme-primary));
}

.article-body table {
  width: 100%;
  border-collapse: collapse;
  margin-bottom: 1rem;
}

.article-body th,
.article-body td {
  border: 1px solid rgba(var(--v-theme-on-surface), 0.12);
  padding: 0.5rem 0.75rem;
  text-align: left;
}

.article-body th {
  font-weight: 600;
  background: rgba(var(--v-theme-surface-variant), 0.2);
}

.article-body small {
  color: rgba(var(--v-theme-on-surface), 0.6);
}
</style>
