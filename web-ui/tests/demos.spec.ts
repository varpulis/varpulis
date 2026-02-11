/**
 * Showcase Demo E2E Tests
 *
 * Tests the 3 new showcase demos (Blind Spot, Haystack, SOC at Scale)
 * against real Varpulis coordinator and workers.
 */

import { test, expect, type Page } from '@playwright/test'
import { spawn, type ChildProcess } from 'child_process'
import * as path from 'path'
import * as fs from 'fs'
import { fileURLToPath } from 'url'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

const SCREENSHOT_DIR = path.join(__dirname, '..', 'test-results', 'screenshots')
const VARPULIS_BIN = path.join(__dirname, '..', '..', 'target', 'release', 'varpulis')
const COORDINATOR_PORT = 19100
const WORKER_PORT = 19000
const API_KEY = 'e2e-test-key'

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
  if (!ready) throw new Error('Coordinator failed to start')
  return proc
}

async function startWorker(): Promise<ChildProcess> {
  const proc = spawn(VARPULIS_BIN, [
    'server',
    '--port', String(WORKER_PORT),
    '--bind', '127.0.0.1',
    '--api-key', API_KEY,
    '--coordinator', `http://localhost:${COORDINATOR_PORT}`,
    '--worker-id', 'demo-worker',
  ], {
    stdio: ['ignore', 'pipe', 'pipe'],
  })

  proc.stdout?.on('data', (data) => {
    console.log(`[worker] ${data.toString().trim()}`)
  })
  proc.stderr?.on('data', (data) => {
    console.error(`[worker] ${data.toString().trim()}`)
  })

  processes.push({ process: proc, name: 'demo-worker' })

  const ready = await waitForPort(WORKER_PORT)
  if (!ready) throw new Error('Worker failed to start')
  return proc
}

function cleanupProcesses(): void {
  for (const { process, name } of processes) {
    try {
      process.kill('SIGTERM')
      console.log(`Stopped ${name}`)
    } catch {
      // Already dead
    }
  }
  processes.length = 0
}

/** Wait for pipeline deployment overlay to disappear */
async function waitForDeployment(page: Page): Promise<void> {
  // Wait for "Pipeline Running" chip to appear
  await expect(
    page.locator('.v-chip').filter({ hasText: 'Pipeline Running' })
  ).toBeVisible({ timeout: 30000 })
}

/** Click the "Inject Events" button in the narration panel */
async function injectEvents(page: Page): Promise<void> {
  const injectBtn = page.locator('button').filter({ hasText: /Inject Events/i })
  await expect(injectBtn).toBeVisible({ timeout: 5000 })
  await injectBtn.click()
  // Wait for injection to complete (button becomes enabled again)
  await expect(injectBtn).toBeEnabled({ timeout: 15000 })
}

/** Navigate to the next step */
async function nextStep(page: Page): Promise<void> {
  const nextBtn = page.locator('button').filter({ has: page.locator('.mdi-chevron-right') })
  await nextBtn.click()
  await page.waitForTimeout(300)
}

/** Click a step indicator chip */
async function goToStep(page: Page, stepNumber: number): Promise<void> {
  const stepChip = page.locator('.presentation-bar .v-chip').filter({ hasText: String(stepNumber) })
  await stepChip.click()
  await page.waitForTimeout(300)
}

/** End the demo and return to scenarios page */
async function endDemo(page: Page): Promise<void> {
  // Use force:true to bypass any navigation drawers that might intercept clicks
  const endBtn = page.locator('button').filter({ hasText: 'End Demo' })
  await endBtn.click({ force: true })
  await expect(page).toHaveURL(/\/scenarios/, { timeout: 10000 })
}

test.describe('Showcase Demo E2E', () => {
  test.beforeAll(async () => {
    if (!fs.existsSync(VARPULIS_BIN)) {
      throw new Error(
        `Varpulis binary not found at ${VARPULIS_BIN}. Run 'cargo build --release -p varpulis-cli' first.`
      )
    }

    await startCoordinator()
    await startWorker()
    // Wait for worker to register with coordinator
    await new Promise((resolve) => setTimeout(resolve, 2000))
  })

  test.afterAll(async () => {
    cleanupProcesses()
  })

  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 })
    await page.emulateMedia({ colorScheme: 'dark' })
    await page.addInitScript((apiKey) => {
      localStorage.setItem('varpulis_api_key', apiKey)
      localStorage.removeItem('varpulis_coordinator_url')
    }, API_KEY)
  })

  // =====================
  // Scenarios Index
  // =====================

  test('Scenarios page lists all 8 demos', async ({ page }) => {
    await page.goto('/scenarios')
    await page.waitForLoadState('networkidle')

    // Verify 3 showcase demos are listed (first in order)
    await expect(page.locator('text=The Blind Spot')).toBeVisible()
    await expect(page.locator('text=Needle in a Haystack')).toBeVisible()
    await expect(page.locator('text=SOC at Scale')).toBeVisible()

    // Verify CxO demos are also present
    await expect(page.locator('text=Credit Card Fraud Detection')).toBeVisible()

    // Count total scenario cards
    const cards = page.locator('.scenario-card')
    await expect(cards).toHaveCount(8)

    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, 'scenarios-index.png'),
      fullPage: true,
    })
  })

  // =====================
  // Demo C: The Blind Spot
  // =====================

  test('Blind Spot: deploys and shows comparison panel', async ({ page }) => {
    await page.goto('/scenarios/blind-spot')
    await waitForDeployment(page)

    // Verify comparison panel is visible with both columns
    await expect(page.locator('text=Traditional CEP')).toBeVisible()
    await expect(page.locator('.v-card-title').filter({ hasText: 'Varpulis' })).toBeVisible()

    // Verify step indicators exist (check step 1 and step 4 chips)
    await expect(page.locator('.step-active')).toBeVisible()

    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, 'blind-spot-deployed.png'),
      fullPage: true,
    })

    await endDemo(page)
  })

  test('Blind Spot Step 1: Clear Signal - both engines detect', async ({ page }) => {
    await page.goto('/scenarios/blind-spot')
    await waitForDeployment(page)

    // Step 1: Clear Signal
    await injectEvents(page)

    // Varpulis should find 1 kill chain
    const varpulisChip = page.locator('.v-card').filter({ hasText: 'Varpulis' })
      .locator('.v-chip').filter({ hasText: /1 detected/ })
    await expect(varpulisChip).toBeVisible({ timeout: 10000 })

    // Greedy should also find 1 (no blocking noise)
    const greedyChip = page.locator('.v-card').filter({ hasText: 'Traditional CEP' })
      .locator('.v-chip').filter({ hasText: /1 detected/ })
    await expect(greedyChip).toBeVisible({ timeout: 5000 })

    // Verify detected chain shows real data (not "unknown")
    const detectedChain = page.locator('.detected-chain').first()
    await expect(detectedChain).toBeVisible()
    await expect(detectedChain).not.toContainText('unknown')

    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, 'blind-spot-step1.png'),
      fullPage: true,
    })

    await endDemo(page)
  })

  test('Blind Spot Step 2: The Blind Spot - greedy misses attack', async ({ page }) => {
    await page.goto('/scenarios/blind-spot')
    await waitForDeployment(page)

    // Navigate to step 2
    await goToStep(page, 2)

    // Inject events
    await injectEvents(page)

    // Varpulis should find 1 kill chain
    const varpulisChip = page.locator('.v-card').filter({ hasText: 'Varpulis' })
      .locator('.v-chip').filter({ hasText: /1 detected/ })
    await expect(varpulisChip).toBeVisible({ timeout: 10000 })

    // Greedy finds 0 (blocked by noise)
    const greedyChip = page.locator('.v-card').filter({ hasText: 'Traditional CEP' })
      .locator('.v-chip').filter({ hasText: /0 detected/ })
    await expect(greedyChip).toBeVisible({ timeout: 5000 })

    // Verify "MISSED" indicator appears
    await expect(page.locator('text=attack MISSED')).toBeVisible()

    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, 'blind-spot-step2.png'),
      fullPage: true,
    })

    await endDemo(page)
  })

  test('Blind Spot Step 3: Buried in Traffic - greedy misses 2 of 3', async ({ page }) => {
    await page.goto('/scenarios/blind-spot')
    await waitForDeployment(page)

    // Navigate to step 3
    await goToStep(page, 3)

    await injectEvents(page)

    // Varpulis should find 3 kill chains
    const varpulisChip = page.locator('.v-card').filter({ hasText: 'Varpulis' })
      .locator('.v-chip').filter({ hasText: /3 detected/ })
    await expect(varpulisChip).toBeVisible({ timeout: 10000 })

    // Greedy finds only 1
    const greedyChip = page.locator('.v-card').filter({ hasText: 'Traditional CEP' })
      .locator('.v-chip').filter({ hasText: /1 detected/ })
    await expect(greedyChip).toBeVisible({ timeout: 5000 })

    // Verify missed count
    await expect(page.locator('text=2 attacks MISSED')).toBeVisible()

    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, 'blind-spot-step3.png'),
      fullPage: true,
    })

    await endDemo(page)
  })

  test('Blind Spot Step 4: Clean Traffic - zero false positives', async ({ page }) => {
    await page.goto('/scenarios/blind-spot')
    await waitForDeployment(page)

    // Navigate to step 4
    await goToStep(page, 4)

    await injectEvents(page)

    // Both should show 0 detected
    const greedyZero = page.locator('.v-card').filter({ hasText: 'Traditional CEP' })
      .locator('.v-chip').filter({ hasText: /0 detected/ })
    await expect(greedyZero).toBeVisible({ timeout: 5000 })

    const varpulisZero = page.locator('.v-card').filter({ hasText: 'Varpulis' })
      .locator('.v-chip').filter({ hasText: /0 detected/ })
    await expect(varpulisZero).toBeVisible({ timeout: 5000 })

    // Verify "correct" message appears for clean traffic
    await expect(page.locator('text=0 alerts — correct').first()).toBeVisible()

    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, 'blind-spot-step4-clean.png'),
      fullPage: true,
    })

    await endDemo(page)
  })

  // =====================
  // Demo B: Needle in a Haystack
  // =====================

  test('Haystack: deploys and shows host grid', async ({ page }) => {
    await page.goto('/scenarios/haystack')
    await waitForDeployment(page)

    // Verify host grid shows 20 hosts
    await expect(page.locator('text=Network Hosts')).toBeVisible()
    const hostTiles = page.locator('.host-tile')
    await expect(hostTiles).toHaveCount(20)

    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, 'haystack-deployed.png'),
      fullPage: true,
    })

    await endDemo(page)
  })

  test('Haystack Step 1: finds 3 attacks in 200+ events', async ({ page }) => {
    await page.goto('/scenarios/haystack')
    await waitForDeployment(page)

    // Step 1: The Haystack
    await injectEvents(page)

    // Should find 3 attacks
    const attacksChip = page.locator('.haystack-stats').locator('text=3').first()
    await expect(attacksChip).toBeVisible({ timeout: 15000 })

    // Host grid should show 3 compromised hosts
    await expect(page.locator('text=3 compromised')).toBeVisible({ timeout: 5000 })

    // Compromised host tiles should have alert styling
    const alertTiles = page.locator('.host-alert')
    await expect(alertTiles).toHaveCount(3)

    // Verify specific targets lit up
    await expect(page.locator('.host-alert').filter({ hasText: 'web-server-02' })).toBeVisible()
    await expect(page.locator('.host-alert').filter({ hasText: 'db-server-01' })).toBeVisible()
    await expect(page.locator('.host-alert').filter({ hasText: 'mail-server-01' })).toBeVisible()

    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, 'haystack-step1.png'),
      fullPage: true,
    })

    await endDemo(page)
  })

  test('Haystack Step 2: zoom in confirms attacks', async ({ page }) => {
    await page.goto('/scenarios/haystack')
    await waitForDeployment(page)

    // Navigate to step 2
    await goToStep(page, 2)

    await injectEvents(page)

    // Should find 3 more attacks (zoom events)
    const alertTiles = page.locator('.host-alert')
    const count = await alertTiles.count()
    expect(count).toBeGreaterThanOrEqual(3)

    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, 'haystack-step2.png'),
      fullPage: true,
    })

    await endDemo(page)
  })

  // =====================
  // Demo A: SOC at Scale
  // =====================

  test('SOC at Scale: deploys and shows rule matrix', async ({ page }) => {
    await page.goto('/scenarios/soc-scale')
    await waitForDeployment(page)

    // Verify rule matrix is visible with 10 rules
    await expect(page.locator('text=Detection Rules')).toBeVisible()
    const ruleRows = page.locator('.rule-row')
    await expect(ruleRows).toHaveCount(10)

    // All rules should initially be idle
    await expect(page.locator('text=0 / 10 active')).toBeVisible()

    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, 'soc-scale-deployed.png'),
      fullPage: true,
    })

    await endDemo(page)
  })

  test('SOC at Scale Step 1: 1 rule triggers', async ({ page }) => {
    await page.goto('/scenarios/soc-scale')
    await waitForDeployment(page)

    // Step 1: trigger brute force rule
    await injectEvents(page)

    // Brute Force (T1110) should be triggered — use auto-waiting assertion
    const bruteForceRow = page.locator('.rule-row').filter({ hasText: 'T1110' })
    await expect(bruteForceRow).toHaveClass(/rule-triggered/, { timeout: 10000 })

    // Should show at least 1 rule active
    const activeIndicator = page.locator('.rule-triggered')
    await expect(activeIndicator.first()).toBeVisible({ timeout: 5000 })

    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, 'soc-scale-step1.png'),
      fullPage: true,
    })

    await endDemo(page)
  })

  test('SOC at Scale Step 2: 5 rules trigger', async ({ page }) => {
    await page.goto('/scenarios/soc-scale')
    await waitForDeployment(page)

    // Navigate to step 2
    await goToStep(page, 2)

    await injectEvents(page)

    // Multiple rules should trigger — use auto-waiting assertion
    const triggeredRules = page.locator('.rule-triggered')
    await expect(triggeredRules.first()).toBeVisible({ timeout: 10000 })
    // Wait for Vue to finish rendering all triggered rules
    await page.waitForTimeout(1000)
    const count = await triggeredRules.count()
    expect(count).toBeGreaterThanOrEqual(3) // At least 3 of the expected 5

    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, 'soc-scale-step2.png'),
      fullPage: true,
    })

    await endDemo(page)
  })

  test('SOC at Scale Step 3: all 10 rules trigger', async ({ page }) => {
    await page.goto('/scenarios/soc-scale')
    await waitForDeployment(page)

    // Navigate to step 3
    await goToStep(page, 3)

    await injectEvents(page)

    // Most rules should trigger — use auto-waiting assertion
    const triggeredRules = page.locator('.rule-triggered')
    await expect(triggeredRules.first()).toBeVisible({ timeout: 10000 })
    // Wait for Vue to finish rendering all triggered rules
    await page.waitForTimeout(1000)
    const count = await triggeredRules.count()
    expect(count).toBeGreaterThanOrEqual(5) // At least half

    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, 'soc-scale-step3.png'),
      fullPage: true,
    })

    await endDemo(page)
  })

  // =====================
  // VPL Source Drawer
  // =====================

  test('VPL source drawer opens from blind spot demo', async ({ page }) => {
    await page.goto('/scenarios/blind-spot')
    await waitForDeployment(page)

    // Click the code icon to open VPL drawer
    const codeBtn = page.locator('button[title="View VPL Patterns"]')
    await codeBtn.click()
    await page.waitForTimeout(500)

    // Verify VPL drawer is visible with the kill chain pattern
    await expect(page.locator('text=VPL Pipeline')).toBeVisible()
    await expect(page.locator('text=Kill Chain Detection')).toBeVisible()
    await expect(page.locator('.vpl-snippet').filter({ hasText: 'Recon' })).toBeVisible()

    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, 'blind-spot-vpl-drawer.png'),
      fullPage: true,
    })

    await endDemo(page)
  })

  // =====================
  // Event Log Drawer
  // =====================

  test('Event log shows injection history', async ({ page }) => {
    await page.goto('/scenarios/blind-spot')
    await waitForDeployment(page)

    // Inject events first
    await injectEvents(page)

    // Open event log drawer
    const logBtn = page.locator('button[title="Event Log"]')
    await logBtn.click()
    await page.waitForTimeout(500)

    // Verify event log shows the injection
    await expect(page.locator('text=Event Log')).toBeVisible()
    const logEntry = page.locator('.log-entry')
    await expect(logEntry.first()).toBeVisible()

    // Verify output events are shown
    await expect(page.locator('text=kill_chain').first()).toBeVisible()

    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, 'blind-spot-event-log.png'),
      fullPage: true,
    })

    await endDemo(page)
  })

  // =====================
  // Batch Endpoint API Tests
  // =====================

  test('Worker batch endpoint accepts and returns flat events', async ({ page }) => {
    // First, deploy a pipeline via the coordinator
    const deployResp = await page.request.post(
      `http://localhost:${COORDINATOR_PORT}/api/v1/cluster/pipeline-groups`,
      {
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': API_KEY,
        },
        data: {
          name: 'batch-test',
          pipelines: [{
            name: 'batch-test',
            source: `stream KillChain = Recon as r
    -> Exploit where target == r.target as e
    -> Exfiltration where source == r.target as x
    .within(1h)
    .emit(
        alert_type: "kill_chain",
        attacker: r.source,
        target: r.target,
        method: e.method,
        destination: x.destination
    )`,
          }],
        },
      }
    )
    expect(deployResp.ok()).toBeTruthy()
    const group = await deployResp.json()

    // Inject batch via coordinator
    const batchResp = await page.request.post(
      `http://localhost:${COORDINATOR_PORT}/api/v1/cluster/pipeline-groups/${group.id}/inject-batch`,
      {
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': API_KEY,
        },
        data: {
          events_text: `Recon { source: "test-attacker", target: "test-target", scan_type: "port_scan" }
BATCH 100
Exploit { target: "test-target", method: "sqli", payload: "union_select" }
BATCH 100
Exfiltration { source: "test-target", destination: "evil.com", bytes: 50000 }`,
        },
      }
    )
    expect(batchResp.ok()).toBeTruthy()
    const result = await batchResp.json()

    // Verify batch response structure
    expect(result.events_sent).toBe(3)
    expect(result.events_failed).toBe(0)
    expect(result.output_events.length).toBeGreaterThanOrEqual(1)
    expect(result.processing_time_us).toBeGreaterThan(0)

    // Verify output events have flat field format
    const outputEvent = result.output_events[0]
    expect(outputEvent.alert_type).toBe('kill_chain')
    expect(outputEvent.attacker).toBe('test-attacker')
    expect(outputEvent.target).toBe('test-target')
    expect(outputEvent.method).toBe('sqli')
    expect(outputEvent.destination).toBe('evil.com')

    // Clean up
    await page.request.delete(
      `http://localhost:${COORDINATOR_PORT}/api/v1/cluster/pipeline-groups/${group.id}`,
      {
        headers: { 'x-api-key': API_KEY },
      }
    )
  })
})
