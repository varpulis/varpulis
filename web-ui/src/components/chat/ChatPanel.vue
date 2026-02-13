<script setup lang="ts">
import { ref, nextTick, onMounted, computed } from 'vue'
import { sendChatMessage, getChatConfig } from '@/api/cluster'
import type { ChatMessage, ToolCallInfo, LlmConfigResponse } from '@/types/cluster'

interface DisplayMessage {
  role: string
  content: string
  toolCalls?: ToolCallInfo[]
}

const messages = ref<DisplayMessage[]>([])
const input = ref('')
const loading = ref(false)
const chatConfig = ref<LlmConfigResponse | null>(null)
const messagesContainer = ref<HTMLElement | null>(null)

const isConfigured = computed(() => chatConfig.value?.configured ?? false)
const modelLabel = computed(() => {
  if (!chatConfig.value?.configured) return ''
  return `${chatConfig.value.model} (${chatConfig.value.provider})`
})

async function fetchConfig() {
  try {
    chatConfig.value = await getChatConfig()
  } catch {
    chatConfig.value = null
  }
}

async function send() {
  const text = input.value.trim()
  if (!text || loading.value) return

  messages.value.push({ role: 'user', content: text })
  input.value = ''
  loading.value = true
  await scrollToBottom()

  try {
    const conversation: ChatMessage[] = messages.value
      .filter(m => m.role === 'user' || m.role === 'assistant')
      .map(m => ({ role: m.role, content: m.content }))

    const response = await sendChatMessage(conversation)
    messages.value.push({
      role: 'assistant',
      content: response.message.content,
      toolCalls: response.tool_calls_executed,
    })
  } catch (e) {
    messages.value.push({
      role: 'assistant',
      content: `Error: ${e instanceof Error ? e.message : 'Failed to get response'}`,
    })
  } finally {
    loading.value = false
    await scrollToBottom()
  }
}

async function scrollToBottom() {
  await nextTick()
  if (messagesContainer.value) {
    messagesContainer.value.scrollTop = messagesContainer.value.scrollHeight
  }
}

function handleKeydown(e: KeyboardEvent) {
  if (e.key === 'Enter' && !e.shiftKey) {
    e.preventDefault()
    send()
  }
}

onMounted(fetchConfig)
</script>

<template>
  <div class="chat-panel d-flex flex-column" style="height: 100%;">
    <!-- Header -->
    <div class="pa-3 d-flex align-center" style="border-bottom: 1px solid rgba(var(--v-border-color), var(--v-border-opacity));">
      <v-icon class="mr-2">mdi-robot</v-icon>
      <span class="text-subtitle-1 font-weight-medium">AI Assistant</span>
      <v-spacer />
      <v-chip v-if="isConfigured" size="small" color="primary" variant="tonal">
        {{ modelLabel }}
      </v-chip>
    </div>

    <!-- Messages -->
    <div
      ref="messagesContainer"
      class="flex-grow-1 overflow-y-auto pa-3"
      style="min-height: 0;"
    >
      <!-- Empty state -->
      <div v-if="messages.length === 0 && isConfigured" class="text-center pa-8">
        <v-icon size="48" color="grey-lighten-1" class="mb-4">mdi-chat-processing-outline</v-icon>
        <div class="text-body-1 text-medium-emphasis">
          Ask about your cluster, pipelines, or VPL queries.
        </div>
      </div>

      <div v-if="!isConfigured" class="text-center pa-8">
        <v-icon size="48" color="grey-lighten-1" class="mb-4">mdi-cog-outline</v-icon>
        <div class="text-body-1 text-medium-emphasis mb-2">
          No LLM provider configured.
        </div>
        <div class="text-body-2 text-medium-emphasis">
          Configure an AI provider in Settings to enable the assistant.
        </div>
      </div>

      <!-- Message bubbles -->
      <div
        v-for="(msg, i) in messages"
        :key="i"
        class="mb-3"
        :class="msg.role === 'user' ? 'd-flex justify-end' : ''"
      >
        <div
          :class="[
            'pa-3 rounded-lg',
            msg.role === 'user'
              ? 'bg-primary text-white'
              : 'bg-surface-light',
          ]"
          style="max-width: 85%; white-space: pre-wrap; word-break: break-word;"
        >
          {{ msg.content }}
        </div>

        <!-- Tool calls -->
        <div v-if="msg.toolCalls && msg.toolCalls.length > 0" class="mt-2" style="max-width: 85%;">
          <v-expansion-panels variant="accordion" density="compact">
            <v-expansion-panel
              v-for="(tc, j) in msg.toolCalls"
              :key="j"
            >
              <v-expansion-panel-title class="text-caption">
                <v-icon size="14" class="mr-1">mdi-wrench</v-icon>
                {{ tc.tool_name }}
              </v-expansion-panel-title>
              <v-expansion-panel-text>
                <div class="text-caption font-weight-medium mb-1">Input:</div>
                <pre class="text-caption mb-2" style="overflow-x: auto;">{{ JSON.stringify(tc.input, null, 2) }}</pre>
                <div class="text-caption font-weight-medium mb-1">Output:</div>
                <pre class="text-caption" style="overflow-x: auto; max-height: 200px;">{{ JSON.stringify(tc.output, null, 2) }}</pre>
              </v-expansion-panel-text>
            </v-expansion-panel>
          </v-expansion-panels>
        </div>
      </div>

      <!-- Loading indicator -->
      <div v-if="loading" class="mb-3">
        <v-progress-linear indeterminate color="primary" class="mb-2" />
        <span class="text-caption text-medium-emphasis">Thinking...</span>
      </div>
    </div>

    <!-- Input -->
    <div class="pa-3" style="border-top: 1px solid rgba(var(--v-border-color), var(--v-border-opacity));">
      <v-textarea
        v-model="input"
        :disabled="!isConfigured || loading"
        placeholder="Ask about your cluster..."
        rows="2"
        auto-grow
        max-rows="4"
        hide-details
        variant="outlined"
        density="compact"
        @keydown="handleKeydown"
      >
        <template #append-inner>
          <v-btn
            icon="mdi-send"
            size="small"
            variant="text"
            :disabled="!input.trim() || loading || !isConfigured"
            @click="send"
          />
        </template>
      </v-textarea>
    </div>
  </div>
</template>

<style scoped>
.bg-surface-light {
  background: rgba(var(--v-theme-surface-variant), 0.3);
}
</style>
