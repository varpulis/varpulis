import { test } from '@playwright/test';

test.use({
  video: 'on',
  viewport: { width: 1400, height: 900 },
});

test('Varpulis n8n workflow demo', async ({ page }) => {
  // 1. Login
  await page.goto('http://localhost:5678/signin');
  await page.waitForTimeout(1500);
  await page.locator('input[autocomplete="email"]').first().fill('demo@varpulis.com');
  await page.locator('input[type="password"]').first().fill('VarpulisDemo123!');
  await page.locator('button:has-text("Sign in")').click();
  await page.waitForTimeout(3000);

  // Handle onboarding survey if present
  for (let i = 0; i < 5; i++) {
    const getStarted = page.locator('button:has-text("Get started"), button:has-text("Skip")');
    if (await getStarted.first().isVisible({ timeout: 2000 }).catch(() => false)) {
      await getStarted.first().click();
      await page.waitForTimeout(1500);
    } else {
      break;
    }
  }
  await page.waitForTimeout(1000);

  // 2. Create new workflow — click "Start from scratch" or "+"
  const startScratch = page.locator('text=Start from scratch');
  if (await startScratch.isVisible({ timeout: 3000 }).catch(() => false)) {
    await startScratch.click();
    await page.waitForTimeout(2000);
  }

  await page.screenshot({ path: 'test-results/01-empty-canvas.png' });

  // 3. Click "Add first step..." to open trigger panel
  const addStep = page.locator('text=Add first step');
  if (await addStep.isVisible({ timeout: 3000 }).catch(() => false)) {
    await addStep.click();
    await page.waitForTimeout(1000);
  }

  // 4. Search for Manual Trigger
  await page.keyboard.type('Manual', { delay: 60 });
  await page.waitForTimeout(1500);
  const manualTrigger = page.locator('text=Manual Trigger').first();
  if (await manualTrigger.isVisible({ timeout: 3000 }).catch(() => false)) {
    await manualTrigger.click();
    await page.waitForTimeout(2000);
  }

  await page.screenshot({ path: 'test-results/02-manual-trigger-added.png' });

  // Close any open panel
  await page.keyboard.press('Escape');
  await page.waitForTimeout(500);

  // 5. Add Varpulis node — click the + on the right side of the trigger
  // Find the + button that appears after the trigger node
  const plusOnNode = page.locator('[data-test-id="canvas-node-creator-button"], .plus-endpoint, [data-test-id="canvas-add-button"]');
  if (await plusOnNode.first().isVisible({ timeout: 3000 }).catch(() => false)) {
    await plusOnNode.first().click();
    await page.waitForTimeout(1000);
  } else {
    // Fallback: press Tab to open node creator
    await page.keyboard.press('Tab');
    await page.waitForTimeout(1000);
  }

  // 6. Search for Varpulis
  await page.keyboard.type('Varpulis', { delay: 80 });
  await page.waitForTimeout(2000);

  await page.screenshot({ path: 'test-results/03-search-varpulis.png' });

  // Click on Varpulis CEP
  const varpulisResult = page.locator('text=Varpulis CEP');
  if (await varpulisResult.first().isVisible({ timeout: 3000 }).catch(() => false)) {
    await varpulisResult.first().click();
    await page.waitForTimeout(2000);
  }

  await page.screenshot({ path: 'test-results/04-varpulis-added.png' });

  // 7. The node config panel should open — check for VPL Pattern field
  const vplField = page.locator('text=VPL Pattern, label:has-text("VPL Pattern")');
  if (await vplField.first().isVisible({ timeout: 3000 }).catch(() => false)) {
    await page.screenshot({ path: 'test-results/05-varpulis-config.png' });
  }

  // Close config
  await page.keyboard.press('Escape');
  await page.waitForTimeout(500);

  await page.screenshot({ path: 'test-results/06-workflow-complete.png' });
  await page.waitForTimeout(2000);
});
