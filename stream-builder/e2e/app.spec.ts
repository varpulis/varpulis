import { test, expect } from '@playwright/test'
import { toolbar, sidebar } from './helpers'

test.describe('App shell', () => {
  test('loads with toolbar, sidebar, canvas, and editor', async ({ page }) => {
    await page.goto('/')
    // Toolbar branding
    await expect(toolbar(page).getByText('Stream Builder')).toBeVisible()
    // Toolbar buttons
    await expect(toolbar(page).getByRole('button', { name: /Stream/ })).toBeVisible()
    await expect(toolbar(page).getByRole('button', { name: /Validate/ })).toBeVisible()
    await expect(toolbar(page).getByRole('button', { name: /Deploy/ })).toBeVisible()
    // Sidebar sections
    await expect(sidebar(page).getByText('Connectors', { exact: true })).toBeVisible()
    await expect(sidebar(page).getByText('Event Types', { exact: true })).toBeVisible()
    // Empty canvas message
    await expect(page.getByText('No streams yet')).toBeVisible()
    // VPL Source header
    await expect(page.getByText('VPL Source')).toBeVisible()
  })

  test('canvas shows empty state with helper text', async ({ page }) => {
    await page.goto('/')
    await expect(page.getByText('No streams yet')).toBeVisible()
    await expect(page.getByText('+ Stream')).toBeVisible()
  })

  test('stream count shows 0 initially', async ({ page }) => {
    await page.goto('/')
    await expect(toolbar(page).getByText('0 streams')).toBeVisible()
  })
})
