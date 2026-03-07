/**
 * Core UI E2E Tests
 *
 * Tests core UI flows: navigation, dashboard, editor, pipelines view,
 * theme toggle, responsive layout, and accessibility basics.
 *
 * Runs against real backend (coordinator + workers) started by e2e.spec.ts
 * or independently with a running dev server.
 */

import { test, expect } from '@playwright/test'

const BASE_URL = 'http://localhost:5173'
const API_KEY = 'e2e-test-key'

test.describe('Core UI Flows', () => {
  test.beforeEach(async ({ page }) => {
    // Set API key in session storage before navigating
    await page.goto(BASE_URL)
    await page.evaluate((key) => {
      sessionStorage.setItem('varpulis_api_key', key)
      localStorage.setItem('varpulis_authenticated', 'true')
    }, API_KEY)
    await page.goto(BASE_URL)
  })

  test('dashboard renders with navigation sidebar', async ({ page }) => {
    // AppBar should be visible
    await expect(page.locator('.v-app-bar')).toBeVisible()
    await expect(page.getByText('Varpulis')).toBeVisible()
    await expect(page.getByText('Control Plane')).toBeVisible()

    // Navigation drawer should have key items
    await expect(page.getByTestId('nav-dashboard')).toBeVisible()
    await expect(page.getByTestId('nav-cluster')).toBeVisible()
    await expect(page.getByTestId('nav-pipelines')).toBeVisible()
    await expect(page.getByTestId('nav-editor')).toBeVisible()
  })

  test('navigation between views works', async ({ page }) => {
    // Navigate to Pipelines
    await page.getByTestId('nav-pipelines').click()
    await expect(page).toHaveURL(/\/pipelines/)
    await expect(page.getByText('Pipeline Groups')).toBeVisible()

    // Navigate to Editor
    await page.getByTestId('nav-editor').click()
    await expect(page).toHaveURL(/\/editor/)

    // Navigate to Cluster
    await page.getByTestId('nav-cluster').click()
    await expect(page).toHaveURL(/\/cluster/)

    // Navigate back to Dashboard
    await page.getByTestId('nav-dashboard').click()
    await expect(page).toHaveURL(/\/$/)
  })

  test('theme toggle switches between dark and light', async ({ page }) => {
    // Default is dark theme
    const appDiv = page.locator('.v-application')

    // Click theme toggle
    await page.getByRole('button', { name: /switch to light mode/i }).click()

    // Verify theme changed
    await expect(appDiv).toHaveClass(/v-theme--light/)

    // Toggle back
    await page.getByRole('button', { name: /switch to dark mode/i }).click()
    await expect(appDiv).toHaveClass(/v-theme--dark/)
  })

  test('pipelines view shows summary stats and quick deploy button', async ({ page }) => {
    await page.getByTestId('nav-pipelines').click()
    await expect(page).toHaveURL(/\/pipelines/)

    // Summary stats cards should be visible
    await expect(page.getByText('Total Groups')).toBeVisible()
    await expect(page.getByText('Running')).toBeVisible()
    await expect(page.getByText('Failed')).toBeVisible()
    await expect(page.getByText('Saved Sources')).toBeVisible()

    // Quick Deploy button should exist
    await expect(page.getByTestId('quick-deploy-btn')).toBeVisible()
  })

  test('editor view loads with Monaco editor', async ({ page }) => {
    await page.getByTestId('nav-editor').click()
    await expect(page).toHaveURL(/\/editor/)

    // Monaco editor container should be present
    await expect(page.locator('.monaco-editor')).toBeVisible({ timeout: 15000 })
  })

  test('connection status indicator is visible', async ({ page }) => {
    // Connection chip should show Live or Offline
    const connectionChip = page.locator('.v-chip').filter({ hasText: /Live|Offline/ })
    await expect(connectionChip).toBeVisible()
  })

  test('user menu opens and shows profile info', async ({ page }) => {
    await page.getByTestId('user-menu-btn').click()

    // Menu card should appear with settings option
    await expect(page.getByText('Settings')).toBeVisible()
    await expect(page.getByTestId('logout-btn')).toBeVisible()
  })

  test('drawer toggle cycles through states', async ({ page }) => {
    const drawer = page.locator('.v-navigation-drawer')
    await expect(drawer).toBeVisible()

    // Click nav toggle to cycle to rail mode
    await page.getByRole('button', { name: /toggle navigation drawer/i }).click()
    await expect(drawer).toHaveClass(/v-navigation-drawer--rail/)

    // Click again to hide
    await page.getByRole('button', { name: /toggle navigation drawer/i }).click()

    // Click again to restore
    await page.getByRole('button', { name: /toggle navigation drawer/i }).click()
    await expect(drawer).toBeVisible()
  })
})

test.describe('Accessibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(BASE_URL)
    await page.evaluate((key) => {
      sessionStorage.setItem('varpulis_api_key', key)
      localStorage.setItem('varpulis_authenticated', 'true')
    }, API_KEY)
    await page.goto(BASE_URL)
  })

  test('icon buttons have aria-labels', async ({ page }) => {
    // Theme toggle
    const themeBtn = page.getByRole('button', { name: /switch to (light|dark) mode/i })
    await expect(themeBtn).toBeVisible()

    // GitHub link
    const githubBtn = page.getByRole('link', { name: /view documentation on github/i })
    await expect(githubBtn).toBeVisible()

    // Nav drawer toggle
    const navToggle = page.getByRole('button', { name: /toggle navigation drawer/i })
    await expect(navToggle).toBeVisible()
  })

  test('snackbar has aria-live attribute', async ({ page }) => {
    // The snackbar container should have role=status for screen readers
    // It may not be visible yet, but the attribute should be in the DOM
    const snackbar = page.locator('[aria-live="polite"]')
    // Check it exists in DOM (may be hidden)
    await expect(snackbar.first()).toBeAttached()
  })
})

test.describe('Responsive Layout', () => {
  test('mobile viewport hides user name, shows icons', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 812 })
    await page.goto(BASE_URL)
    await page.evaluate((key) => {
      sessionStorage.setItem('varpulis_api_key', key)
      localStorage.setItem('varpulis_authenticated', 'true')
    }, API_KEY)
    await page.goto(BASE_URL)

    // AppBar should still be visible
    await expect(page.locator('.v-app-bar')).toBeVisible()

    // User name should be hidden on mobile (d-none d-sm-inline)
    const userName = page.locator('.d-none.d-sm-inline')
    if (await userName.count() > 0) {
      await expect(userName.first()).not.toBeVisible()
    }
  })

  test('tablet viewport shows pipeline stats in grid', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 })
    await page.goto(BASE_URL)
    await page.evaluate((key) => {
      sessionStorage.setItem('varpulis_api_key', key)
      localStorage.setItem('varpulis_authenticated', 'true')
    }, API_KEY)
    await page.goto(BASE_URL + '/pipelines')

    await expect(page.getByText('Pipeline Groups').first()).toBeVisible()
    await expect(page.getByText('Total Groups')).toBeVisible()
  })
})
