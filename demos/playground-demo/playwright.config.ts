import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: '.',
  testMatch: 'record-demo.ts',
  fullyParallel: false,
  workers: 1,
  reporter: 'list',
  timeout: 120000,
  use: {
    // Record video of the demo
    video: {
      mode: 'on',
      size: { width: 1400, height: 900 },
    },
    // No screenshots on failure — we want the video
    screenshot: 'off',
    trace: 'off',
    // Use a clean browser context
    colorScheme: 'dark',
    viewport: { width: 1400, height: 900 },
    // Slow down actions for visual effect
    actionTimeout: 10000,
  },
  projects: [
    {
      name: 'demo',
      use: {
        ...devices['Desktop Chrome'],
        viewport: { width: 1400, height: 900 },
      },
    },
  ],
});
