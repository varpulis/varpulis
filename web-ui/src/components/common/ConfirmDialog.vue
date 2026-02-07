<script setup lang="ts">
withDefaults(
  defineProps<{
    modelValue: boolean
    title?: string
    message?: string
    confirmText?: string
    cancelText?: string
    confirmColor?: string
    loading?: boolean
    persistent?: boolean
  }>(),
  {
    title: 'Confirm Action',
    message: 'Are you sure you want to proceed?',
    confirmText: 'Confirm',
    cancelText: 'Cancel',
    confirmColor: 'primary',
    loading: false,
    persistent: false,
  }
)

const emit = defineEmits<{
  'update:modelValue': [value: boolean]
  confirm: []
  cancel: []
}>()

function close(): void {
  emit('update:modelValue', false)
}

function handleConfirm(): void {
  emit('confirm')
}

function handleCancel(): void {
  emit('cancel')
  close()
}
</script>

<template>
  <v-dialog
    :model-value="modelValue"
    :persistent="persistent || loading"
    max-width="450"
    @update:model-value="emit('update:modelValue', $event)"
  >
    <v-card>
      <v-card-title class="d-flex align-center">
        <v-icon class="mr-2" color="warning">mdi-alert-circle-outline</v-icon>
        {{ title }}
      </v-card-title>

      <v-card-text>
        <slot>
          {{ message }}
        </slot>
      </v-card-text>

      <v-card-actions>
        <v-spacer />
        <v-btn
          variant="text"
          :disabled="loading"
          @click="handleCancel"
        >
          {{ cancelText }}
        </v-btn>
        <v-btn
          :color="confirmColor"
          :loading="loading"
          @click="handleConfirm"
        >
          {{ confirmText }}
        </v-btn>
      </v-card-actions>
    </v-card>
  </v-dialog>
</template>
