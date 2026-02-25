<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { listModels, uploadModel, deleteModel } from '@/api/cluster'
import type { ModelRegistryEntry } from '@/types/cluster'

const models = ref<ModelRegistryEntry[]>([])
const loading = ref(false)
const showUpload = ref(false)
const uploading = ref(false)
const deleteTarget = ref<string | null>(null)
const showDeleteConfirm = ref(false)

const form = ref({
  name: '',
  inputs: '',
  outputs: '',
  description: '',
})
const modelFile = ref<File | null>(null)

const headers = [
  { title: 'Name', key: 'name' },
  { title: 'Format', key: 'format' },
  { title: 'Inputs', key: 'inputs' },
  { title: 'Outputs', key: 'outputs' },
  { title: 'Size', key: 'size_bytes' },
  { title: 'Uploaded', key: 'uploaded_at' },
  { title: 'Actions', key: 'actions', sortable: false },
]

async function fetchModels() {
  loading.value = true
  try {
    models.value = await listModels()
  } catch {
    // API might not be available
    models.value = []
  } finally {
    loading.value = false
  }
}

function readFileAsBase64(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader()
    reader.onload = () => {
      const result = reader.result as string
      // Strip data URL prefix (e.g., "data:application/octet-stream;base64,")
      const base64 = result.includes(',') ? result.split(',')[1] : result
      resolve(base64)
    }
    reader.onerror = reject
    reader.readAsDataURL(file)
  })
}

async function handleUpload() {
  uploading.value = true
  try {
    let data_base64: string | undefined
    if (modelFile.value) {
      data_base64 = await readFileAsBase64(modelFile.value)
    }
    await uploadModel({
      name: form.value.name,
      inputs: form.value.inputs.split(',').map(s => s.trim()).filter(Boolean),
      outputs: form.value.outputs.split(',').map(s => s.trim()).filter(Boolean),
      description: form.value.description,
      data_base64,
    })
    showUpload.value = false
    form.value = { name: '', inputs: '', outputs: '', description: '' }
    modelFile.value = null
    await fetchModels()
  } finally {
    uploading.value = false
  }
}

function confirmDelete(name: string) {
  deleteTarget.value = name
  showDeleteConfirm.value = true
}

async function handleDelete() {
  if (!deleteTarget.value) return
  try {
    await deleteModel(deleteTarget.value)
    showDeleteConfirm.value = false
    deleteTarget.value = null
    await fetchModels()
  } catch {
    // handle error
  }
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i]
}

function formatDate(iso: string): string {
  return new Date(iso).toLocaleDateString(undefined, {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

onMounted(fetchModels)
</script>

<template>
  <div>
    <div class="d-flex align-center mb-4">
      <h1 class="text-h4">Model Registry</h1>
      <v-spacer />
      <v-btn color="primary" prepend-icon="mdi-plus" @click="showUpload = true">
        Register Model
      </v-btn>
    </div>

    <v-card>
      <v-data-table
        :headers="headers"
        :items="models"
        :loading="loading"
        item-value="name"
        class="elevation-0"
      >
        <template #item.inputs="{ item }">
          <v-chip v-for="inp in item.inputs" :key="inp" size="small" class="mr-1">
            {{ inp }}
          </v-chip>
        </template>

        <template #item.outputs="{ item }">
          <v-chip v-for="out in item.outputs" :key="out" size="small" color="primary" class="mr-1">
            {{ out }}
          </v-chip>
        </template>

        <template #item.size_bytes="{ item }">
          {{ formatBytes(item.size_bytes) }}
        </template>

        <template #item.uploaded_at="{ item }">
          {{ formatDate(item.uploaded_at) }}
        </template>

        <template #item.actions="{ item }">
          <v-btn icon="mdi-delete" size="small" variant="text" color="error" @click="confirmDelete(item.name)" />
        </template>

        <template #no-data>
          <div class="text-center pa-8">
            <v-icon size="48" color="grey-lighten-1" class="mb-4">mdi-brain</v-icon>
            <div class="text-h6 mb-2">No Models Registered</div>
            <div class="text-body-2 text-medium-emphasis mb-4">
              Register an ONNX model to use with the .score() operator in VPL pipelines.
            </div>
            <v-btn color="primary" @click="showUpload = true">Register First Model</v-btn>
          </div>
        </template>
      </v-data-table>
    </v-card>

    <!-- Upload Dialog -->
    <v-dialog v-model="showUpload" max-width="500">
      <v-card>
        <v-card-title>Register Model</v-card-title>
        <v-card-text>
          <v-file-input
            v-model="modelFile"
            label="ONNX Model File"
            accept=".onnx"
            prepend-icon="mdi-brain"
            hint="Select an .onnx model file to upload"
            persistent-hint
            class="mb-3"
            show-size
          />
          <v-text-field v-model="form.name" label="Model Name" hint="e.g., fraud_scorer" persistent-hint class="mb-3" />
          <v-text-field v-model="form.inputs" label="Input Fields" hint="Comma-separated, e.g., amount,velocity,distance" persistent-hint class="mb-3" />
          <v-text-field v-model="form.outputs" label="Output Fields" hint="Comma-separated, e.g., fraud_prob" persistent-hint class="mb-3" />
          <v-textarea v-model="form.description" label="Description" rows="2" />
        </v-card-text>
        <v-card-actions>
          <v-spacer />
          <v-btn @click="showUpload = false">Cancel</v-btn>
          <v-btn color="primary" :loading="uploading" :disabled="!form.name || !form.inputs || !form.outputs" @click="handleUpload">
            Register
          </v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>

    <!-- Delete Confirmation -->
    <v-dialog v-model="showDeleteConfirm" max-width="400">
      <v-card>
        <v-card-title>Delete Model</v-card-title>
        <v-card-text>
          Are you sure you want to delete model "{{ deleteTarget }}"? This cannot be undone.
        </v-card-text>
        <v-card-actions>
          <v-spacer />
          <v-btn @click="showDeleteConfirm = false">Cancel</v-btn>
          <v-btn color="error" @click="handleDelete()">Delete</v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>
  </div>
</template>
