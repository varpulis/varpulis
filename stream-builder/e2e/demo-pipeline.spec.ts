import { test, expect } from '@playwright/test'
import { toolbar, sidebar } from './helpers'

test.describe('Demo pipeline', () => {
  test('loads demo with 5 streams and populates editor', async ({ page }) => {
    await page.goto('/')
    await toolbar(page).getByRole('button', { name: /Demo/ }).click()

    // Should show 5 streams in toolbar count
    await expect(toolbar(page).getByText('5 streams')).toBeVisible()

    // Stream cards should be visible in the canvas
    const canvas = page.locator('.flex-1.bg-muted\\/20').first()
    await expect(canvas.getByText('Telemetry').first()).toBeVisible()
    await expect(canvas.getByText('HighTemp').first()).toBeVisible()
    await expect(canvas.getByText('ZoneStats').first()).toBeVisible()
    await expect(canvas.getByText('ZoneAlert').first()).toBeVisible()
    await expect(canvas.getByText('TempSpike').first()).toBeVisible()
  })

  test('demo populates sidebar with connector and event', async ({ page }) => {
    await page.goto('/')
    await toolbar(page).getByRole('button', { name: /Demo/ }).click()

    // Connector in sidebar
    await expect(sidebar(page).getByText('MqttBroker')).toBeVisible()
    // Event type in sidebar
    await expect(sidebar(page).getByText('SensorReading')).toBeVisible()
  })

  test('demo generates VPL code in editor', async ({ page }) => {
    await page.goto('/')
    await toolbar(page).getByRole('button', { name: /Demo/ }).click()

    // Wait for Monaco editor to render content
    const editorContent = page.locator('.monaco-editor .view-lines')
    await expect(editorContent).toBeVisible()

    // Editor should contain VPL keywords from the demo
    const text = await editorContent.textContent()
    expect(text).toContain('connector')
    expect(text).toContain('MqttBroker')
    expect(text).toContain('stream')
  })

  test('topology panel shows stats after loading demo', async ({ page }) => {
    await page.goto('/')
    await toolbar(page).getByRole('button', { name: /Demo/ }).click()

    // Topology footer shows combined counts (last element with tabular-nums in topology bar)
    const topoBar = page.locator('text=Topology').first().locator('..')
    await expect(topoBar.locator('.tabular-nums')).toContainText('5 streams')
    await expect(topoBar.locator('.tabular-nums')).toContainText('1 connectors')
    await expect(topoBar.locator('.tabular-nums')).toContainText('1 events')
  })
})
