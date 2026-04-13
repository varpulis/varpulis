import { test, expect } from '@playwright/test'
import { toolbar } from './helpers'

test.describe('Stream management', () => {
  test('add a stream via toolbar button', async ({ page }) => {
    await page.goto('/')
    await expect(page.getByText('No streams yet')).toBeVisible()

    // Click "+ Stream" button in toolbar
    await toolbar(page).getByRole('button', { name: /Stream/ }).click()

    // Empty state should be gone, a stream card should appear
    await expect(page.getByText('No streams yet')).not.toBeVisible()
    await expect(toolbar(page).getByText('1 stream', { exact: false })).toBeVisible()
  })

  test('add multiple streams', async ({ page }) => {
    await page.goto('/')
    const addBtn = toolbar(page).getByRole('button', { name: /Stream/ })

    await addBtn.click()
    await addBtn.click()
    await addBtn.click()

    await expect(toolbar(page).getByText('3 streams')).toBeVisible()
  })

  test('remove a stream via card X button', async ({ page }) => {
    await page.goto('/')
    await toolbar(page).getByRole('button', { name: /Stream/ }).click()
    await expect(toolbar(page).getByText('1 stream', { exact: false })).toBeVisible()

    // Hover on the stream card to reveal the X button and click it
    const card = page.locator('[class*="w-72"]').first()
    await card.hover()
    // The X button is inside the card header
    const removeBtn = card.getByRole('button').filter({ has: page.locator('svg.lucide-x') }).first()
    await removeBtn.click()

    await expect(page.getByText('No streams yet')).toBeVisible()
    await expect(toolbar(page).getByText('0 streams')).toBeVisible()
  })

  test('clear all streams', async ({ page }) => {
    await page.goto('/')
    const addBtn = toolbar(page).getByRole('button', { name: /Stream/ })
    await addBtn.click()
    await addBtn.click()
    await expect(toolbar(page).getByText('2 streams')).toBeVisible()

    // Click trash icon (Clear pipeline)
    await page.getByTitle('Clear pipeline').click()

    await expect(page.getByText('No streams yet')).toBeVisible()
    await expect(toolbar(page).getByText('0 streams')).toBeVisible()
  })
})
