/**
 * Connectors Security Configuration E2E Tests
 *
 * Tests the connector management UI with security features:
 * - Creating connectors with SASL_SSL, SCRAM, mTLS configuration
 * - Password masking and visibility toggle
 * - Security badges in connector list
 * - Credential profile references
 * - Conditional field visibility (SASL fields appear when protocol includes SASL)
 *
 * Run with:
 *   npx playwright test tests/connectors.spec.ts
 *   npx playwright test tests/connectors.spec.ts --headed   # Watch in browser
 */

import { test, expect, type Page } from '@playwright/test'
import { spawn, type ChildProcess } from 'child_process'
import * as path from 'path'
import * as fs from 'fs'
import { fileURLToPath } from 'url'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

const SCREENSHOT_DIR = path.join(__dirname, '..', 'docs', 'screenshots')
const VARPULIS_BIN = path.join(__dirname, '..', '..', 'target', 'release', 'varpulis')
const COORDINATOR_PORT = 19100
const API_KEY = 'e2e-connector-test-key'
const BASE_URL = 'http://localhost:5173'

interface ProcessInfo {
  process: ChildProcess
  name: string
}

const processes: ProcessInfo[] = []

if (!fs.existsSync(SCREENSHOT_DIR)) {
  fs.mkdirSync(SCREENSHOT_DIR, { recursive: true })
}

async function waitForPort(port: number, maxAttempts = 30): Promise<boolean> {
  for (let i = 0; i < maxAttempts; i++) {
    try {
      const response = await fetch(`http://localhost:${port}/health`)
      if (response.ok) return true
    } catch {
      // Not ready yet
    }
    await new Promise((resolve) => setTimeout(resolve, 1000))
  }
  return false
}

async function startCoordinator(): Promise<ChildProcess> {
  console.log('Starting coordinator...')
  const proc = spawn(VARPULIS_BIN, [
    'coordinator',
    '--port', String(COORDINATOR_PORT),
    '--bind', '127.0.0.1',
    '--api-key', API_KEY,
  ], {
    stdio: ['ignore', 'pipe', 'pipe'],
  })

  proc.stdout?.on('data', (data) => {
    console.log(`[coordinator] ${data.toString().trim()}`)
  })
  proc.stderr?.on('data', (data) => {
    console.error(`[coordinator] ${data.toString().trim()}`)
  })

  processes.push({ process: proc, name: 'coordinator' })

  const ready = await waitForPort(COORDINATOR_PORT)
  if (!ready) {
    throw new Error('Coordinator failed to start')
  }
  console.log('Coordinator ready')
  return proc
}

function killAll(): void {
  for (const p of processes) {
    try {
      p.process.kill('SIGTERM')
    } catch {
      // Already dead
    }
  }
  processes.length = 0
}

// Helper to create a connector via API
async function createConnectorViaApi(connector: {
  name: string
  connector_type: string
  params: Record<string, string>
  description?: string
}): Promise<void> {
  const response = await fetch(`http://localhost:${COORDINATOR_PORT}/api/v1/cluster/connectors`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-API-Key': API_KEY,
    },
    body: JSON.stringify(connector),
  })
  if (!response.ok) {
    const error = await response.text()
    throw new Error(`Failed to create connector: ${error}`)
  }
}

// Helper to delete a connector via API
async function deleteConnectorViaApi(name: string): Promise<void> {
  await fetch(`http://localhost:${COORDINATOR_PORT}/api/v1/cluster/connectors/${name}`, {
    method: 'DELETE',
    headers: { 'X-API-Key': API_KEY },
  })
}

// Helper to list connectors via API
async function listConnectorsViaApi(): Promise<string[]> {
  const response = await fetch(`http://localhost:${COORDINATOR_PORT}/api/v1/cluster/connectors`, {
    headers: { 'X-API-Key': API_KEY },
  })
  const data = await response.json()
  return (data.connectors || []).map((c: { name: string }) => c.name)
}

// Clean up all test connectors
async function cleanupConnectors(): Promise<void> {
  const names = await listConnectorsViaApi()
  for (const name of names) {
    if (name.startsWith('e2e_')) {
      await deleteConnectorViaApi(name)
    }
  }
}

// ---------- Test Suite ----------

test.describe('Connectors Security Configuration', () => {
  test.beforeAll(async () => {
    await startCoordinator()
  })

  test.afterAll(async () => {
    await cleanupConnectors()
    killAll()
  })

  test.beforeEach(async ({ page }) => {
    await cleanupConnectors()
    await page.setViewportSize({ width: 1440, height: 900 })
    await page.emulateMedia({ colorScheme: 'dark' })

    // Auth setup: set API key in sessionStorage (for axios X-API-Key header + router guard)
    // Do NOT set localStorage.varpulis_authenticated — that triggers fetchUser→clearToken cascade
    await page.addInitScript((apiKey) => {
      sessionStorage.setItem('varpulis_api_key', apiKey)
      localStorage.removeItem('varpulis_coordinator_url')
    }, API_KEY)

    // Mock /me endpoint — coordinator without OAuth returns 503/401 which triggers
    // the response interceptor redirect-to-login cascade. Return a fake user instead.
    await page.route('**/api/v1/me', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ id: 'e2e', login: 'e2e-test', name: 'E2E Test User' }),
      })
    })

    // Mock /auth/renew — prevent any renewal attempts from redirecting
    await page.route('**/auth/renew', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ ok: true }),
      })
    })
  })

  test('connectors page loads with empty state', async ({ page }) => {
    await page.goto(`${BASE_URL}/connectors`)
    await page.waitForLoadState('networkidle')

    // Verify page title
    await expect(page.getByRole('heading', { name: 'Connectors' })).toBeVisible()

    // Verify Add Connector button
    const addBtn = page.locator('button').filter({ hasText: 'Add Connector' })
    await expect(addBtn).toBeVisible()

    // Verify tabs
    await expect(page.getByRole('tab', { name: 'Configured' })).toBeVisible()
    await expect(page.getByRole('tab', { name: 'Available Types' })).toBeVisible()

    // Take screenshot of empty connectors page
    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, 'connectors-empty.png'),
      fullPage: true,
    })
  })

  test('create Kafka connector with SASL_SSL + SCRAM-SHA-512', async ({ page }) => {
    await page.goto(`${BASE_URL}/connectors`)
    await page.waitForLoadState('networkidle')

    // Open create dialog
    await page.locator('button').filter({ hasText: 'Add Connector' }).click()
    const dialog = page.locator('.v-dialog').filter({ hasText: 'Add Connector' })
    await expect(dialog).toBeVisible()

    // Fill basic fields
    await dialog.locator('input').first().fill('e2e_kafka_prod')
    // Select Kafka type
    await dialog.locator('.v-select').first().click()
    await page.locator('.v-list-item').filter({ hasText: 'Kafka' }).click()

    // Fill description
    await dialog.locator('input[type="text"]').nth(1).fill('Production Kafka with SCRAM-SHA-512')

    // Fill broker address
    const brokersField = dialog.locator('input').filter({ has: page.locator('[aria-label]') }).or(
      dialog.locator('.v-text-field').filter({ hasText: 'Brokers' }).locator('input')
    )
    // Find input fields by their labels
    const connectionSection = dialog.locator('.v-card-text')
    const brokersInput = connectionSection.locator('.v-text-field').filter({ hasText: /Brokers/ }).locator('input')
    await brokersInput.fill('kafka-1:9093,kafka-2:9093')

    // Take screenshot before security section
    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, 'connectors-kafka-basic.png'),
    })

    // Expand security section
    const securityToggle = dialog.locator('text=Security')
    await securityToggle.click()
    await page.waitForTimeout(300) // Wait for expand animation

    // Select security protocol
    const protocolSelect = dialog.locator('.v-select').filter({ hasText: /Security Protocol|PLAINTEXT/ })
    await protocolSelect.click()
    await page.locator('.v-list-item').filter({ hasText: 'SASL_SSL' }).click()
    await page.waitForTimeout(200)

    // Select SASL mechanism
    const mechanismSelect = dialog.locator('.v-select').filter({ hasText: /SASL Mechanism/ })
    await mechanismSelect.click()
    await page.locator('.v-list-item').filter({ hasText: 'SCRAM-SHA-512' }).click()

    // Fill SASL credentials
    const saslUsername = dialog.locator('.v-text-field').filter({ hasText: /SASL Username/ }).locator('input')
    await saslUsername.fill('varpulis-app')

    const saslPassword = dialog.locator('.v-text-field').filter({ hasText: /SASL Password/ }).locator('input')
    await saslPassword.fill('s3cureP@ss')

    // Fill SSL certificate paths
    const caPath = dialog.locator('.v-text-field').filter({ hasText: /CA Certificate/ }).locator('input')
    await caPath.fill('/etc/varpulis/certs/ca.pem')

    const clientCert = dialog.locator('.v-text-field').filter({ hasText: /Client Certificate Path/ }).locator('input')
    await clientCert.fill('/etc/varpulis/certs/client.pem')

    const clientKey = dialog.locator('.v-text-field').filter({ hasText: /Client Key/ }).locator('input')
    await clientKey.fill('/etc/varpulis/certs/client-key.pem')

    // Fill credentials profile
    const profileField = dialog.locator('.v-text-field').filter({ hasText: /Credentials Profile/ }).locator('input')
    await profileField.fill('production')

    // Take screenshot of fully configured security form
    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, 'connectors-kafka-security.png'),
    })

    // Submit
    await dialog.locator('button').filter({ hasText: 'Create' }).click()
    await page.waitForTimeout(500)

    // Verify connector appears in list with security badges
    await expect(page.locator('code').filter({ hasText: 'e2e_kafka_prod' })).toBeVisible()

    // Verify security badges are shown
    const row = page.locator('tr').filter({ hasText: 'e2e_kafka_prod' })
    await expect(row.locator('.v-chip').filter({ hasText: 'SASL_SSL' })).toBeVisible()
    await expect(row.locator('.v-chip').filter({ hasText: 'SCRAM-SHA-512' })).toBeVisible()
    await expect(row.locator('.v-chip').filter({ hasText: /Profile/ })).toBeVisible()
    await expect(row.locator('.v-chip').filter({ hasText: 'mTLS' })).toBeVisible()

    // Take screenshot of connector list with security badges
    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, 'connectors-list-security.png'),
      fullPage: true,
    })
  })

  test('create MQTT connector with username/password auth', async ({ page }) => {
    await page.goto(`${BASE_URL}/connectors`)
    await page.waitForLoadState('networkidle')

    // Open create dialog
    await page.locator('button').filter({ hasText: 'Add Connector' }).click()
    const dialog = page.locator('.v-dialog').filter({ hasText: 'Add Connector' })
    await expect(dialog).toBeVisible()

    // Fill name — MQTT is the default type
    await dialog.locator('input').first().fill('e2e_mqtt_sensors')

    // Fill connection params
    const hostInput = dialog.locator('.v-text-field').filter({ hasText: /Host/ }).locator('input')
    await hostInput.fill('mqtt.example.com')

    const portInput = dialog.locator('.v-text-field').filter({ hasText: /Port/ }).locator('input')
    await portInput.fill('8883')

    // Expand security section
    await dialog.locator('text=Security').click()
    await page.waitForTimeout(300)

    // Fill username and password
    const usernameField = dialog.locator('.v-text-field').filter({ hasText: /Username/ }).locator('input')
    await usernameField.fill('sensor-gateway')

    const passwordField = dialog.locator('.v-text-field').filter({ hasText: /Password/ }).locator('input')
    await passwordField.fill('mqtt-secret-pass')

    // Verify password is masked
    await expect(passwordField).toHaveAttribute('type', 'password')

    // Toggle password visibility
    const eyeIcon = dialog.locator('.v-text-field').filter({ hasText: /Password/ }).locator('.mdi-eye')
    await eyeIcon.click()
    await expect(passwordField).toHaveAttribute('type', 'text')

    // Take screenshot showing password toggle
    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, 'connectors-mqtt-security.png'),
    })

    // Toggle back to hidden
    const eyeOffIcon = dialog.locator('.v-text-field').filter({ hasText: /Password/ }).locator('.mdi-eye-off')
    await eyeOffIcon.click()
    await expect(passwordField).toHaveAttribute('type', 'password')

    // Submit
    await dialog.locator('button').filter({ hasText: 'Create' }).click()
    await page.waitForTimeout(500)

    // Verify connector appears with auth badge
    const row = page.locator('tr').filter({ hasText: 'e2e_mqtt_sensors' })
    await expect(row).toBeVisible()
    await expect(row.locator('.v-chip').filter({ hasText: 'Auth' })).toBeVisible()
  })

  test('Kafka security fields show/hide based on protocol', async ({ page }) => {
    await page.goto(`${BASE_URL}/connectors`)
    await page.waitForLoadState('networkidle')

    await page.locator('button').filter({ hasText: 'Add Connector' }).click()
    const dialog = page.locator('.v-dialog').filter({ hasText: 'Add Connector' })

    // Switch to Kafka
    await dialog.locator('.v-select').first().click()
    await page.locator('.v-list-item').filter({ hasText: 'Kafka' }).click()

    // Expand security
    await dialog.locator('text=Security').click()
    await page.waitForTimeout(300)

    // Initially no SASL fields visible (no protocol selected)
    await expect(dialog.locator('.v-text-field').filter({ hasText: /SASL Username/ })).not.toBeVisible()
    await expect(dialog.locator('.v-text-field').filter({ hasText: /CA Certificate/ })).not.toBeVisible()

    // Select SASL_SSL — both SASL and SSL fields should appear
    const protocolSelect = dialog.locator('.v-select').filter({ hasText: /Security Protocol/ })
    await protocolSelect.click()
    await page.locator('.v-list-item').filter({ hasText: 'SASL_SSL' }).click()
    await page.waitForTimeout(200)

    await expect(dialog.locator('.v-select').filter({ hasText: /SASL Mechanism/ })).toBeVisible()
    await expect(dialog.locator('.v-text-field').filter({ hasText: /SASL Username/ })).toBeVisible()
    await expect(dialog.locator('.v-text-field').filter({ hasText: /SASL Password/ })).toBeVisible()
    await expect(dialog.locator('.v-text-field').filter({ hasText: /CA Certificate/ })).toBeVisible()
    await expect(dialog.locator('.v-text-field').filter({ hasText: /Client Certificate/ })).toBeVisible()
    await expect(dialog.locator('.v-text-field').filter({ hasText: /Client Key/ })).toBeVisible()

    // Switch to SSL only — SASL fields should hide, SSL fields stay
    await protocolSelect.click()
    await page.locator('.v-list-item').filter({ hasText: /^SSL$/ }).click()
    await page.waitForTimeout(200)

    await expect(dialog.locator('.v-select').filter({ hasText: /SASL Mechanism/ })).not.toBeVisible()
    await expect(dialog.locator('.v-text-field').filter({ hasText: /SASL Username/ })).not.toBeVisible()
    await expect(dialog.locator('.v-text-field').filter({ hasText: /CA Certificate/ })).toBeVisible()

    // Switch to PLAINTEXT — all security-specific fields should hide
    await protocolSelect.click()
    await page.getByRole('option', { name: 'PLAINTEXT', exact: true }).click()
    await page.waitForTimeout(200)

    await expect(dialog.locator('.v-select').filter({ hasText: /SASL Mechanism/ })).not.toBeVisible()
    await expect(dialog.locator('.v-text-field').filter({ hasText: /CA Certificate/ })).not.toBeVisible()

    // Credentials Profile should always be visible regardless of protocol
    await expect(dialog.locator('.v-text-field').filter({ hasText: /Credentials Profile/ })).toBeVisible()
  })

  test('edit existing connector preserves security settings', async ({ page }) => {
    // Create a connector via API with security params
    await createConnectorViaApi({
      name: 'e2e_kafka_edit',
      connector_type: 'kafka',
      params: {
        brokers: 'kafka:9093',
        security_protocol: 'SASL_SSL',
        sasl_mechanism: 'SCRAM-SHA-512',
        sasl_username: 'admin',
        sasl_password: 'secret123',
        ssl_ca_location: '/certs/ca.pem',
        profile: 'staging',
      },
      description: 'Edit test connector',
    })

    await page.goto(`${BASE_URL}/connectors`)
    await page.waitForLoadState('networkidle')

    // Click edit on the connector
    const row = page.locator('tr').filter({ hasText: 'e2e_kafka_edit' })
    await expect(row).toBeVisible()
    await row.locator('button').filter({ has: page.locator('.mdi-pencil') }).click()

    const dialog = page.locator('.v-dialog').filter({ hasText: 'Edit Connector' })
    await expect(dialog).toBeVisible()

    // Security section should be auto-expanded since connector has security params
    await expect(dialog.locator('.v-text-field').filter({ hasText: /SASL Username/ })).toBeVisible()

    // Verify values are preserved
    const usernameField = dialog.locator('.v-text-field').filter({ hasText: /SASL Username/ }).locator('input')
    await expect(usernameField).toHaveValue('admin')

    const profileField = dialog.locator('.v-text-field').filter({ hasText: /Credentials Profile/ }).locator('input')
    await expect(profileField).toHaveValue('staging')

    const caField = dialog.locator('.v-text-field').filter({ hasText: /CA Certificate/ }).locator('input')
    await expect(caField).toHaveValue('/certs/ca.pem')

    // Take screenshot of edit dialog with security
    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, 'connectors-edit-security.png'),
    })
  })

  test('connector list shows multiple connectors with mixed security', async ({ page }) => {
    // Create several connectors with different security profiles
    await createConnectorViaApi({
      name: 'e2e_kafka_sasl',
      connector_type: 'kafka',
      params: {
        brokers: 'kafka-1:9093,kafka-2:9093',
        security_protocol: 'SASL_SSL',
        sasl_mechanism: 'SCRAM-SHA-512',
        sasl_username: 'app',
        ssl_ca_location: '/certs/ca.pem',
        ssl_certificate_location: '/certs/client.pem',
        ssl_key_location: '/certs/client-key.pem',
        profile: 'production',
      },
      description: 'Production Kafka (SCRAM + mTLS)',
    })

    await createConnectorViaApi({
      name: 'e2e_kafka_plain',
      connector_type: 'kafka',
      params: {
        brokers: 'localhost:9092',
      },
      description: 'Development Kafka (no auth)',
    })

    await createConnectorViaApi({
      name: 'e2e_mqtt_auth',
      connector_type: 'mqtt',
      params: {
        host: 'mqtt.example.com',
        port: '8883',
        username: 'sensor-gw',
        password: 'secret',
      },
      description: 'MQTT with authentication',
    })

    await createConnectorViaApi({
      name: 'e2e_http_bearer',
      connector_type: 'http',
      params: {
        url: 'https://api.example.com/events',
        authorization: 'Bearer tok_xxx',
      },
      description: 'HTTP with Bearer token',
    })

    await page.goto(`${BASE_URL}/connectors`)
    await page.waitForLoadState('networkidle')

    // Verify all connectors are visible
    await expect(page.locator('code').filter({ hasText: 'e2e_kafka_sasl' })).toBeVisible()
    await expect(page.locator('code').filter({ hasText: 'e2e_kafka_plain' })).toBeVisible()
    await expect(page.locator('code').filter({ hasText: 'e2e_mqtt_auth' })).toBeVisible()
    await expect(page.locator('code').filter({ hasText: 'e2e_http_bearer' })).toBeVisible()

    // Verify security badges for kafka_sasl
    const saslRow = page.locator('tr').filter({ hasText: 'e2e_kafka_sasl' })
    await expect(saslRow.locator('.v-chip').filter({ hasText: 'SASL_SSL' })).toBeVisible()
    await expect(saslRow.locator('.v-chip').filter({ hasText: 'SCRAM-SHA-512' })).toBeVisible()
    await expect(saslRow.locator('.v-chip').filter({ hasText: 'mTLS' })).toBeVisible()
    await expect(saslRow.locator('.v-chip').filter({ hasText: /Profile/ })).toBeVisible()

    // Verify no security badges for kafka_plain
    const plainRow = page.locator('tr').filter({ hasText: 'e2e_kafka_plain' })
    await expect(plainRow.locator('.v-chip').filter({ hasText: /SASL|SSL|Auth|Profile/ })).not.toBeVisible()

    // Verify Auth badge for mqtt_auth
    const mqttRow = page.locator('tr').filter({ hasText: 'e2e_mqtt_auth' })
    await expect(mqttRow.locator('.v-chip').filter({ hasText: 'Auth' })).toBeVisible()

    // Take screenshot of mixed security connector list
    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, 'connectors-list-mixed.png'),
      fullPage: true,
    })
  })

  test('available connector types tab shows capabilities', async ({ page }) => {
    await page.goto(`${BASE_URL}/connectors`)
    await page.waitForLoadState('networkidle')

    // Switch to Available Types tab
    await page.getByRole('tab', { name: 'Available Types' }).click()
    await page.waitForTimeout(300)

    // Take screenshot
    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, 'connectors-available-types.png'),
      fullPage: true,
    })
  })

  test('delete connector with confirmation', async ({ page }) => {
    await createConnectorViaApi({
      name: 'e2e_to_delete',
      connector_type: 'mqtt',
      params: { host: 'localhost' },
      description: 'Will be deleted',
    })

    await page.goto(`${BASE_URL}/connectors`)
    await page.waitForLoadState('networkidle')

    // Click delete
    const row = page.locator('tr').filter({ hasText: 'e2e_to_delete' })
    await expect(row).toBeVisible()
    await row.locator('button').filter({ has: page.locator('.mdi-delete') }).click()

    // Confirmation dialog
    const confirmDialog = page.locator('.v-dialog').filter({ hasText: 'Delete Connector' })
    await expect(confirmDialog).toBeVisible()
    await expect(confirmDialog.locator('strong')).toHaveText('e2e_to_delete')

    // Confirm deletion
    await confirmDialog.locator('button').filter({ hasText: 'Delete' }).click()
    await page.waitForTimeout(500)

    // Verify connector is gone
    await expect(page.locator('code').filter({ hasText: 'e2e_to_delete' })).not.toBeVisible()
  })
})
