import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/ws': {
        target: 'ws://localhost:3002',
        ws: true,
      },
      '/api': {
        target: 'http://localhost:3002',
      },
    },
  },
})
