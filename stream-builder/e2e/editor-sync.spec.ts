import { test, expect } from '@playwright/test'
import { toolbar } from './helpers'

test.describe('Editor sync', () => {
  test('sync indicator shows "synced" initially', async ({ page }) => {
    await page.goto('/')
    await expect(page.getByText('synced')).toBeVisible()
  })

  test('adding a stream generates VPL in editor', async ({ page }) => {
    await page.goto('/')
    await toolbar(page).getByRole('button', { name: /Stream/ }).click()

    // Wait for editor to render
    await page.waitForTimeout(500)

    // Editor should contain "stream" keyword
    const editorContent = page.locator('.monaco-editor .view-lines')
    await expect(editorContent).toBeVisible()
    const text = await editorContent.textContent()
    expect(text).toContain('stream')
  })

  test('demo pipeline generates full VPL with connectors and events', async ({ page }) => {
    await page.goto('/')
    await toolbar(page).getByRole('button', { name: /Demo/ }).click()
    await page.waitForTimeout(800)

    // Monaco virtualizes rendering; get full text from the model via JS evaluation
    const text = await page.evaluate(() => {
      const editors = (window as unknown as { monaco?: { editor: { getEditors(): Array<{ getValue(): string }> } } }).monaco?.editor.getEditors()
      return editors?.[0]?.getValue() ?? ''
    })

    // If Monaco global isn't exposed, fall back to checking individual rendered lines
    if (text) {
      expect(text).toContain('connector')
      expect(text).toContain('mqtt')
      expect(text).toContain('event')
      expect(text).toContain('SensorReading')
      expect(text).toContain('Telemetry')
      expect(text).toContain('HighTemp')
    } else {
      // Fallback: check rendered line spans individually
      const lines = page.locator('.monaco-editor .view-line')
      const count = await lines.count()
      expect(count).toBeGreaterThan(5)
      // At least some lines should contain key VPL tokens
      const allText = await Promise.all(
        Array.from({ length: count }, (_, i) => lines.nth(i).textContent())
      )
      const combined = allText.join('\n')
      expect(combined).toContain('connector')
      expect(combined).toContain('stream')
    }
  })

  test('clicking a stream card highlights it', async ({ page }) => {
    await page.goto('/')
    await toolbar(page).getByRole('button', { name: /Demo/ }).click()
    await page.waitForTimeout(500)

    // Click the ZoneStats card
    const canvas = page.locator('.flex-1.bg-muted\\/20').first()
    const card = canvas.locator('[class*="w-72"]').filter({ hasText: 'ZoneStats' }).first()
    await card.click()

    // The card should now have ring-2 ring-primary (selected state)
    await expect(card).toHaveClass(/ring-2/)
  })
})
