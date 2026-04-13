import { defineConfig } from '@playwright/test'

const PORT = process.env.CI ? 5173 : 5174

export default defineConfig({
  testDir: './e2e',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: 'list',
  use: {
    baseURL: `http://localhost:${PORT}/stream-builder/`,
    trace: 'on-first-retry',
  },
  webServer: {
    command: `npx vite --port ${PORT}`,
    url: `http://localhost:${PORT}/stream-builder/`,
    reuseExistingServer: !process.env.CI,
    timeout: 15000,
  },
  projects: [
    {
      name: 'chromium',
      use: { browserName: 'chromium' },
    },
  ],
})
