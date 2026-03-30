import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './tests',
  timeout: 60000,
  use: {
    baseURL: 'http://localhost:5678',
    video: 'on',
    screenshot: 'on',
    trace: 'on',
  },
  reporter: [['html', { open: 'never' }]],
  outputDir: 'test-results',
});
