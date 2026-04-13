import { test, expect } from '@playwright/test'

test.describe('Toolbar actions', () => {
  test('settings dialog opens and closes', async ({ page }) => {
    await page.goto('/')
    await page.getByTitle('Server settings').click()
    await expect(page.locator('text=Varpulis Server')).toBeVisible()
    await expect(page.getByPlaceholder('http://localhost:19000')).toBeVisible()
    await expect(page.getByPlaceholder('your-api-key')).toBeVisible()

    // Close via Cancel
    await page.getByRole('button', { name: 'Cancel' }).click()
    await expect(page.locator('text=Varpulis Server')).not.toBeVisible()
  })

  test('deploy dialog opens with auto-generated name', async ({ page }) => {
    await page.goto('/')
    // Need at least one stream to deploy
    await page.getByRole('button', { name: /Stream/ }).first().click()

    // Configure a dummy server URL first via localStorage
    await page.evaluate(() => {
      localStorage.setItem('varpulis_server_url', 'http://localhost:19000')
    })

    await page.getByRole('button', { name: /Deploy/ }).click()
    await expect(page.locator('text=Deploy Pipeline')).toBeVisible()

    // Pipeline name input should be pre-filled with timestamp-based name
    const nameInput = page.getByPlaceholder('my-pipeline')
    const value = await nameInput.inputValue()
    expect(value).toMatch(/^pipeline-\d+$/)

    // Close
    await page.getByRole('button', { name: 'Cancel' }).click()
  })

  test('validate shows info toast when no streams', async ({ page }) => {
    await page.goto('/')
    await page.evaluate(() => {
      localStorage.setItem('varpulis_server_url', 'http://localhost:19000')
    })
    await page.getByRole('button', { name: /Validate/ }).click()
    // Should show info toast about adding streams first
    await expect(page.locator('text=Nothing to validate')).toBeVisible()
  })

  test('deploy shows info toast when no streams', async ({ page }) => {
    await page.goto('/')
    await page.evaluate(() => {
      localStorage.setItem('varpulis_server_url', 'http://localhost:19000')
    })
    await page.getByRole('button', { name: /Deploy/ }).click()
    await expect(page.locator('text=Nothing to deploy')).toBeVisible()
  })

  test('load dialog opens', async ({ page }) => {
    await page.goto('/')
    await page.evaluate(() => {
      localStorage.setItem('varpulis_server_url', 'http://localhost:19000')
    })
    await page.getByRole('button', { name: /Load/ }).click()
    await expect(page.locator('text=Load Pipeline')).toBeVisible()

    // Since server is not running, should show empty or error state
    // Wait a bit for the API call to fail
    await page.waitForTimeout(1000)

    // Close
    await page.getByRole('button', { name: 'Cancel' }).click()
    await expect(page.locator('text=Load Pipeline')).not.toBeVisible()
  })

  test('settings prompts when no server URL configured', async ({ page }) => {
    await page.goto('/')
    // Clear any stored config
    await page.evaluate(() => {
      localStorage.removeItem('varpulis_server_url')
      localStorage.removeItem('varpulis_api_key')
    })
    await page.reload()

    await page.getByRole('button', { name: /Validate/ }).click()
    // Should open settings dialog
    await expect(page.getByRole('heading', { name: 'Varpulis Server' })).toBeVisible()
  })
})
