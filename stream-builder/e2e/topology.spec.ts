import { test, expect } from '@playwright/test'
import { toolbar } from './helpers'

test.describe('Topology panel', () => {
  test('toggle collapse/expand', async ({ page }) => {
    await page.goto('/')

    // Topology panel header should be visible
    const topoHeader = page.locator('text=Topology').first()
    await expect(topoHeader).toBeVisible()

    // Click to collapse
    await topoHeader.click()

    // Click again to expand
    await topoHeader.click()
  })

  test('shows stream count in topology footer', async ({ page }) => {
    await page.goto('/')

    // The topology bar shows combined stats
    const topoBar = page.locator('text=Topology').first().locator('..')
    await expect(topoBar.locator('.tabular-nums')).toContainText('0 streams')

    // Load demo
    await toolbar(page).getByRole('button', { name: /Demo/ }).click()
    await expect(topoBar.locator('.tabular-nums')).toContainText('5 streams')
    await expect(topoBar.locator('.tabular-nums')).toContainText('1 connectors')
  })
})
