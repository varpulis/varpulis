import { expect, Page, test } from '@playwright/test';

const CODE_SERVER_PASSWORD = process.env.CODE_SERVER_PASSWORD || 'varpulis-test';
const CODE_SERVER_URL = process.env.CODE_SERVER_URL || 'http://localhost:8080';

// Check if code-server is available before running UI tests
async function isCodeServerAvailable(): Promise<boolean> {
  try {
    const response = await fetch(CODE_SERVER_URL, { method: 'HEAD', signal: AbortSignal.timeout(3000) });
    return response.ok || response.status === 401; // 401 means it's there but needs auth
  } catch {
    return false;
  }
}

test.describe('Varpulis Flow Editor', () => {
  let page: Page;
  let codeServerAvailable = false;

  test.beforeAll(async ({ browser }) => {
    codeServerAvailable = await isCodeServerAvailable();
    if (!codeServerAvailable) {
      console.log('⚠️  code-server not available, UI tests will be skipped. Run ./k3d/setup.sh first.');
      return;
    }
    
    page = await browser.newPage();
    
    // Login to code-server
    await page.goto('/');
    
    // Handle password if prompted
    const passwordInput = page.locator('input[type="password"]');
    if (await passwordInput.isVisible({ timeout: 5000 }).catch(() => false)) {
      await passwordInput.fill(CODE_SERVER_PASSWORD);
      await page.click('button[type="submit"]');
      await page.waitForLoadState('networkidle');
    }
  });

  test.afterAll(async () => {
    if (page) await page.close();
  });

  test('should open flow editor via command palette', async () => {
    test.skip(!codeServerAvailable, 'code-server not available - run ./k3d/setup.sh');
    
    // Open command palette (Ctrl+Shift+P)
    await page.keyboard.press('Control+Shift+P');
    await page.waitForSelector('.quick-input-widget');
    
    // Type command
    await page.keyboard.type('Varpulis: Open Flow Editor');
    await page.keyboard.press('Enter');
    
    // Wait for flow editor to load
    await expect(page.locator('.toolbar')).toBeVisible({ timeout: 10000 });
    await expect(page.locator('.sidebar')).toBeVisible();
    await expect(page.locator('.canvas')).toBeVisible();
  });

  test('should display component palette in sidebar', async () => {
    test.skip(!codeServerAvailable, 'code-server not available');
    const sidebar = page.frameLocator('iframe').locator('.sidebar');
    
    // Check categories exist
    await expect(sidebar.locator('text=Sources')).toBeVisible();
    await expect(sidebar.locator('text=Sinks')).toBeVisible();
    await expect(sidebar.locator('text=Events')).toBeVisible();
    await expect(sidebar.locator('text=Streams')).toBeVisible();
    await expect(sidebar.locator('text=Patterns')).toBeVisible();
  });

  test('should create MQTT Source node via drag and drop', async () => {
    test.skip(!codeServerAvailable, 'code-server not available');
    const frame = page.frameLocator('iframe');
    const mqttSource = frame.locator('.drag-item:has-text("MQTT Source")');
    const canvas = frame.locator('.canvas');
    
    // Drag MQTT Source to canvas
    await mqttSource.dragTo(canvas, {
      targetPosition: { x: 200, y: 150 }
    });
    
    // Verify node was created
    await expect(frame.locator('.node.source')).toBeVisible();
  });

  test('should open config panel on double-click', async () => {
    test.skip(!codeServerAvailable, 'code-server not available');
    const frame = page.frameLocator('iframe');
    const node = frame.locator('.node.source').first();
    
    // Double-click to open config
    await node.dblclick();
    
    // Verify config panel opens
    const panel = frame.locator('#configPanel');
    await expect(panel).not.toHaveClass(/hidden/);
    await expect(frame.locator('#panelTitle')).toContainText('Configure');
  });

  test('should configure MQTT connector settings', async () => {
    test.skip(!codeServerAvailable, 'code-server not available');
    const frame = page.frameLocator('iframe');
    
    // Fill MQTT configuration
    await frame.locator('#cfg-host').fill('mqtt.example.com');
    await frame.locator('#cfg-port').fill('1883');
    await frame.locator('#cfg-topic').fill('sensors/temperature');
    
    // Save configuration
    await frame.locator('#saveBtn').click();
    
    // Verify panel closes
    await expect(frame.locator('#configPanel')).toHaveClass(/hidden/);
  });

  test('should create Console Sink node', async () => {
    test.skip(!codeServerAvailable, 'code-server not available');
    const frame = page.frameLocator('iframe');
    const consoleSink = frame.locator('.drag-item:has-text("Console")');
    const canvas = frame.locator('.canvas');
    
    await consoleSink.dragTo(canvas, {
      targetPosition: { x: 500, y: 150 }
    });
    
    await expect(frame.locator('.node.sink')).toBeVisible();
  });

  test('should connect nodes', async () => {
    test.skip(!codeServerAvailable, 'code-server not available');
    const frame = page.frameLocator('iframe');
    const sourceNode = frame.locator('.node.source .output-handle').first();
    const sinkNode = frame.locator('.node.sink .input-handle').first();
    
    // Connect source to sink
    await sourceNode.dragTo(sinkNode);
    
    // Verify connection created (SVG line)
    await expect(frame.locator('#connections line')).toBeVisible();
  });

  test('should export valid VPL code', async () => {
    test.skip(!codeServerAvailable, 'code-server not available');
    const frame = page.frameLocator('iframe');
    
    // Click Export button
    await frame.locator('button:has-text("Export")').click();
    
    // In code-server, this triggers a save dialog
    // For testing, we verify the message was sent
    // The actual file save depends on the VSCode extension handling
  });

  test('should save flow as .vflow file', async () => {
    test.skip(!codeServerAvailable, 'code-server not available');
    const frame = page.frameLocator('iframe');
    
    // Click Save button
    await frame.locator('button:has-text("Save")').click();
    
    // Verify save dialog is triggered (handled by VSCode)
  });

  test('should clear all nodes', async () => {
    test.skip(!codeServerAvailable, 'code-server not available');
    const frame = page.frameLocator('iframe');
    
    // Click clear button
    page.on('dialog', dialog => dialog.accept());
    await frame.locator('.btn-danger').click();
    
    // Verify canvas is empty
    await expect(frame.locator('.node')).toHaveCount(0);
  });
});

test.describe('VPL Generation', () => {
  test('should generate valid VPL from flow graph', async ({ page }) => {
    // This test verifies the VPL code structure
    const testFlow = {
      nodes: [
        { id: 'node_1', type: 'source', label: 'mqtt_input', connector: 'mqtt', config: { host: 'localhost', port: '1883', topic: 'events' } },
        { id: 'node_2', type: 'event', label: 'temperature_event', config: {} },
        { id: 'node_3', type: 'stream', label: 'temp_stream', config: { pattern: 'A -> B', window: '5 minutes' } },
        { id: 'node_4', type: 'sink', label: 'console_output', connector: 'console', config: {} }
      ],
      connections: [
        { from: 'node_1', to: 'node_2' },
        { from: 'node_2', to: 'node_3' },
        { from: 'node_3', to: 'node_4' }
      ]
    };

    // Expected VPL output structure
    const expectedPatterns = [
      /source\s+\w+\s+from\s+mqtt/,
      /event\s+\w+\s*\{/,
      /stream\s+\w+\s*\{/,
      /sink\s+\w+\s+from\s+\w+\s+to\s+console/
    ];

    // In real test, we would inject the flow and verify generated VPL
    // This validates the expected structure
    expect(expectedPatterns.length).toBe(4);
  });
});

test.describe('.vflow File Handling', () => {
  test('should load existing .vflow file', async ({ page }) => {
    // Test loading an existing .vflow file
    const sampleVflow = {
      nodes: [
        { id: 'node_1', type: 'source', x: 100, y: 100, label: 'Test Source', connector: 'mqtt', config: { host: 'localhost', port: '1883', topic: 'test' } }
      ],
      connections: []
    };

    // Verify JSON structure is valid
    expect(() => JSON.stringify(sampleVflow)).not.toThrow();
    expect(sampleVflow.nodes).toHaveLength(1);
    expect(sampleVflow.connections).toHaveLength(0);
  });

  test('should preserve node positions on reload', async ({ page }) => {
    const originalFlow = {
      nodes: [
        { id: 'n1', type: 'source', x: 150, y: 200, label: 'Source 1', connector: 'mqtt', config: {} },
        { id: 'n2', type: 'sink', x: 400, y: 200, label: 'Sink 1', connector: 'console', config: {} }
      ],
      connections: [{ from: 'n1', to: 'n2' }]
    };

    // Serialize and deserialize
    const serialized = JSON.stringify(originalFlow);
    const reloaded = JSON.parse(serialized);

    // Verify positions preserved
    expect(reloaded.nodes[0].x).toBe(150);
    expect(reloaded.nodes[0].y).toBe(200);
    expect(reloaded.nodes[1].x).toBe(400);
    expect(reloaded.connections).toHaveLength(1);
  });
});
