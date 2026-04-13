import { test, expect } from '@playwright/test'
import { toolbar } from './helpers'

test.describe('Keyboard shortcuts', () => {
  test('Ctrl+S opens deploy dialog when streams exist', async ({ page }) => {
    await page.goto('/')
    await page.evaluate(() => {
      localStorage.setItem('varpulis_server_url', 'http://localhost:19000')
    })

    // Add a stream first
    await toolbar(page).getByRole('button', { name: /Stream/ }).click()
    await expect(toolbar(page).getByText('1 stream', { exact: false })).toBeVisible()

    // Press Ctrl+S
    await page.keyboard.press('Control+s')
    await expect(page.getByText('Deploy Pipeline')).toBeVisible()

    // Close
    await page.getByRole('button', { name: 'Cancel' }).click()
  })

  test('Ctrl+S shows info toast when no streams', async ({ page }) => {
    await page.goto('/')
    await page.evaluate(() => {
      localStorage.setItem('varpulis_server_url', 'http://localhost:19000')
    })

    await page.keyboard.press('Control+s')
    await expect(page.getByText('Nothing to deploy')).toBeVisible()
  })

  test('Delete key removes selected stream', async ({ page }) => {
    await page.goto('/')
    // Add a stream
    await toolbar(page).getByRole('button', { name: /Stream/ }).click()
    await expect(toolbar(page).getByText('1 stream', { exact: false })).toBeVisible()

    // Click the stream card to select it
    const card = page.locator('[class*="w-72"]').first()
    await card.click()
    // Verify card is selected (has ring-2 class)
    await expect(card).toHaveClass(/ring-2/)

    // Click a non-interactive area to take focus off Monaco/inputs, then press Delete
    // The topology header is a safe target (just a <span>)
    await page.locator('text=Topology').first().click()
    await page.waitForTimeout(50)
    await page.keyboard.press('Delete')

    await expect(toolbar(page).getByText('0 streams')).toBeVisible()
  })
})
