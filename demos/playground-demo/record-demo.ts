/**
 * Varpulis Playground Demo Recorder
 *
 * Records a screencast: paste VPL → paste events → Run → see result.
 * The "wow" is in the simplicity of the code and the result.
 *
 * Run:
 *   cd demos/playground-demo
 *   npx playwright test                   # headless (video saved)
 *   npx playwright test --headed          # watch it live
 *
 * Convert to GIF:
 *   ffmpeg -i test-results/.../video.webm \
 *     -vf "fps=12,scale=1280:-1:flags=lanczos,split[s0][s1];[s0]palettegen=max_colors=128[p];[s1][p]paletteuse=dither=bayer" \
 *     -loop 0 demo.gif
 */

import { test, expect } from '@playwright/test';

const PLAYGROUND_URL = process.env.PLAYGROUND_URL || 'https://www.varpulis-cep.com/playground';

const VPL_CODE = `event TempReading:
    sensor_id: str
    temperature: float

stream HighTemp = TempReading -> all TempReading.increasing(temperature) as rising
    .partition_by(sensor_id)
    .emit(alert: "Rising temperature", sensor: rising.sensor_id, peak: rising.temperature, count: count(rising))`;

const EVENTS = `@0s TempReading { sensor_id: "HVAC-01", temperature: 22.0 }
@1s TempReading { sensor_id: "HVAC-02", temperature: 35.0 }
@2s TempReading { sensor_id: "HVAC-03", temperature: 28.0 }
@3s TempReading { sensor_id: "HVAC-01", temperature: 41.0 }
@4s TempReading { sensor_id: "HVAC-01", temperature: 50.0 }
@5s TempReading { sensor_id: "HVAC-01", temperature: 52.0 }
@6s TempReading { sensor_id: "HVAC-01", temperature: 55.0 }
@7s TempReading { sensor_id: "HVAC-01", temperature: 30.0 }`;

test('Record rising temperature demo', async ({ page, context }) => {
  await context.grantPermissions(['clipboard-read', 'clipboard-write']);

  // --- Navigate ---
  await page.goto(PLAYGROUND_URL, { waitUntil: 'networkidle' });
  await page.waitForTimeout(2500);

  // --- Step 1: Set VPL code in Monaco editor ---
  // Use Monaco's API directly for reliable code insertion
  await page.evaluate((code) => {
    const editors = (window as any).monaco?.editor?.getEditors?.();
    if (editors && editors.length > 0) {
      editors[0].setValue(code);
      // Scroll to top so the full code is visible
      editors[0].revealLine(1);
    }
  }, VPL_CODE);
  await page.waitForTimeout(1500); // Let viewer read the code

  // --- Step 2: Fill events textarea ---
  const eventsTextarea = page.locator('textarea').nth(1);
  await expect(eventsTextarea).toBeVisible();
  await eventsTextarea.click();
  await eventsTextarea.fill('');
  await page.waitForTimeout(200);
  await eventsTextarea.fill(EVENTS);
  await page.waitForTimeout(1200);

  // --- Step 3: Click Run ---
  const runButton = page.locator('.v-btn', { hasText: 'Run' });
  await expect(runButton).toBeVisible();
  await page.waitForTimeout(600);
  await runButton.click();

  // --- Step 4: Wait for results ---
  await page.waitForTimeout(5000);

  // Take a screenshot
  await page.screenshot({ path: 'demo-final.png' });

  // Hold for viewer
  await page.waitForTimeout(4000);
});
