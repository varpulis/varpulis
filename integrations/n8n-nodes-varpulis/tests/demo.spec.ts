import { test } from '@playwright/test';

test.use({
  video: 'on',
  viewport: { width: 1400, height: 900 },
});

test('Varpulis n8n node demo', async ({ page }) => {
  await page.goto('http://localhost:5678');
  await page.waitForTimeout(2000);

  // Handle login if needed
  const signIn = page.locator('button:has-text("Sign in")');
  if (await signIn.isVisible({ timeout: 3000 }).catch(() => false)) {
    await page.locator('input[name="email"], input[autocomplete="email"]').first().fill('demo@varpulis.com');
    await page.locator('input[name="password"], input[type="password"]').first().fill('VarpulisDemo123!');
    await signIn.click();
    await page.waitForTimeout(3000);
  }

  // Handle setup if needed (first run)
  const setupEmail = page.locator('text=Set up owner account');
  if (await setupEmail.isVisible({ timeout: 2000 }).catch(() => false)) {
    await page.locator('input[name="email"]').fill('demo@varpulis.com');
    await page.locator('input[name="firstName"]').fill('Varpulis');
    await page.locator('input[name="lastName"]').fill('Demo');
    await page.locator('input[type="password"]').fill('VarpulisDemo123!');
    await page.locator('button:has-text("Next")').click();
    await page.waitForTimeout(3000);
    // Skip additional steps
    for (let i = 0; i < 3; i++) {
      const skip = page.locator('button:has-text("Skip"), button:has-text("Get started")');
      if (await skip.first().isVisible({ timeout: 2000 }).catch(() => false)) {
        await skip.first().click();
        await page.waitForTimeout(1000);
      }
    }
  }

  await page.waitForTimeout(1000);
  await page.screenshot({ path: 'test-results/01-logged-in.png' });

  // Click "Start from scratch" or "+" to create a workflow
  const startScratch = page.locator('text=Start from scratch');
  if (await startScratch.isVisible({ timeout: 3000 }).catch(() => false)) {
    await startScratch.click();
    await page.waitForTimeout(2000);
  }

  // Or click the + button in sidebar
  const plusNav = page.locator('[data-test-id="side-menu-item-new-workflow"], a[href*="workflow/new"]');
  if (await plusNav.first().isVisible({ timeout: 2000 }).catch(() => false)) {
    await plusNav.first().click();
    await page.waitForTimeout(2000);
  }

  await page.screenshot({ path: 'test-results/02-canvas.png' });

  // Open node panel - click the + on canvas
  const canvasPlus = page.locator('[data-test-id="canvas-plus-button"]');
  if (await canvasPlus.isVisible({ timeout: 3000 }).catch(() => false)) {
    await canvasPlus.click();
    await page.waitForTimeout(1000);
  } else {
    // Try Tab key to open node selector
    await page.keyboard.press('Tab');
    await page.waitForTimeout(1000);
  }

  await page.screenshot({ path: 'test-results/03-node-panel.png' });

  // Type "Varpulis" to search
  await page.keyboard.type('Varpulis', { delay: 80 });
  await page.waitForTimeout(2000);

  await page.screenshot({ path: 'test-results/04-search.png' });

  // Click on result if found
  const vNode = page.locator('[data-test-id*="varpulis"], text=Varpulis Pattern');
  if (await vNode.first().isVisible({ timeout: 3000 }).catch(() => false)) {
    await vNode.first().click();
    await page.waitForTimeout(2000);
  }

  await page.screenshot({ path: 'test-results/05-final.png' });
  await page.waitForTimeout(2000);
});
