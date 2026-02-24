<script lang="ts">
export default { name: 'ShareButton' }
</script>

<script setup lang="ts">
import { ref } from 'vue'

const props = defineProps<{
  vpl: string
}>()

const copied = ref(false)

function shareLink() {
  const encoded = encodeURIComponent(props.vpl)
  const url = `${window.location.origin}/playground?code=${encoded}`
  navigator.clipboard.writeText(url).then(() => {
    copied.value = true
    setTimeout(() => { copied.value = false }, 2000)
  })
}
</script>

<template>
  <v-btn
    variant="outlined"
    size="small"
    :color="copied ? 'success' : undefined"
    @click="shareLink"
    :prepend-icon="copied ? 'mdi-check' : 'mdi-share-variant'"
  >
    {{ copied ? 'Copied!' : 'Share' }}
  </v-btn>
</template>
