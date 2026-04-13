import { test, expect } from '@playwright/test'
import { toolbar, sidebar } from './helpers'

test.describe('Sidebar', () => {
  test('sidebar sections are collapsible', async ({ page }) => {
    await page.goto('/')

    // Connectors section is open by default
    const connectorsHeader = sidebar(page).getByText('Connectors', { exact: true })
    await expect(connectorsHeader).toBeVisible()

    // Click to collapse
    await connectorsHeader.click()
    // Click again to expand
    await connectorsHeader.click()
  })

  test('demo pipeline populates all sidebar sections', async ({ page }) => {
    await page.goto('/')
    await toolbar(page).getByRole('button', { name: /Demo/ }).click()

    // Connector appears in sidebar
    await expect(sidebar(page).getByText('MqttBroker')).toBeVisible()
    // Event type appears in sidebar
    await expect(sidebar(page).getByText('SensorReading')).toBeVisible()
    // Streams appear in sidebar
    await expect(sidebar(page).getByText('Telemetry')).toBeVisible()
    await expect(sidebar(page).getByText('HighTemp')).toBeVisible()
  })

  test('clearing pipeline removes sidebar items', async ({ page }) => {
    await page.goto('/')
    await toolbar(page).getByRole('button', { name: /Demo/ }).click()
    await expect(sidebar(page).getByText('MqttBroker')).toBeVisible()

    // Clear pipeline
    await page.getByTitle('Clear pipeline').click()

    // Sidebar should be empty of demo items
    await expect(sidebar(page).getByText('MqttBroker')).not.toBeVisible()
    await expect(sidebar(page).getByText('SensorReading')).not.toBeVisible()
  })
})
