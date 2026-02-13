import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import vuetify from 'vite-plugin-vuetify'
import { fileURLToPath, URL } from 'node:url'

const coordinatorPort = process.env.VITE_COORDINATOR_PORT || '9100'

export default defineConfig({
  plugins: [
    vue(),
    vuetify({ autoImport: true }),
  ],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url))
    }
  },
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: `http://localhost:${coordinatorPort}`,
        changeOrigin: true,
      },
      '/ws': {
        target: `ws://localhost:${coordinatorPort}`,
        ws: true,
      },
    },
  },
  build: {
    sourcemap: false,
    rollupOptions: {
      output: {
        manualChunks: {
          'vuetify': ['vuetify'],
          'echarts': ['echarts', 'vue-echarts'],
          'monaco': ['@guolao/vue-monaco-editor'],
          'vue-flow': ['@vue-flow/core', '@vue-flow/background', '@vue-flow/controls'],
        },
      },
    },
  },
})
