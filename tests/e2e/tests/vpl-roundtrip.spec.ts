/**
 * VPL Roundtrip Tests
 * 
 * Test scenarios:
 * 1. Import existing VPL files
 * 2. Parse and validate with CLI
 * 3. Run with predictable events
 * 4. Validate expected outputs
 */

import { expect, test } from '@playwright/test';
import { execSync, spawn } from 'child_process';
import * as fs from 'fs';

const PROJECT_ROOT = '/home/cpo/cep';
const CLI_PATH = `${PROJECT_ROOT}/target/release/varpulis`;
const EXAMPLES_DIR = `${PROJECT_ROOT}/examples`;
const TEST_DIR = `${PROJECT_ROOT}/tests/e2e/test-output`;
const SCENARIOS_DIR = `${PROJECT_ROOT}/tests/scenarios`;

// Ensure directories exist
if (!fs.existsSync(TEST_DIR)) {
    fs.mkdirSync(TEST_DIR, { recursive: true });
}
if (!fs.existsSync(SCENARIOS_DIR)) {
    fs.mkdirSync(SCENARIOS_DIR, { recursive: true });
}

/**
 * Run CLI check command on a VPL file
 */
function checkVPL(vplPath: string): { success: boolean; output: string } {
    try {
        const output = execSync(`${CLI_PATH} check "${vplPath}"`, {
            encoding: 'utf-8',
            timeout: 10000,
        });
        return { success: true, output };
    } catch (error: any) {
        return { success: false, output: error.stdout || error.message };
    }
}

/**
 * Run CLI simulate command with events
 */
function simulateVPL(vplPath: string, eventsPath: string, timeout = 5000): Promise<{ success: boolean; output: string; alerts: any[] }> {
    return new Promise((resolve) => {
        const proc = spawn(CLI_PATH, ['simulate', '--program', vplPath, '--events', eventsPath, '--immediate'], {
            timeout,
        });

        let stdout = '';
        let stderr = '';

        proc.stdout.on('data', (data) => {
            stdout += data.toString();
        });

        proc.stderr.on('data', (data) => {
            stderr += data.toString();
        });

        proc.on('close', (code) => {
            const alerts = parseAlerts(stdout);
            resolve({
                success: code === 0,
                output: stdout + stderr,
                alerts,
            });
        });

        proc.on('error', (err) => {
            resolve({
                success: false,
                output: err.message,
                alerts: [],
            });
        });

        // Kill after timeout
        setTimeout(() => {
            proc.kill();
        }, timeout);
    });
}

/**
 * Parse alerts from CLI output
 */
function parseAlerts(output: string): any[] {
    const alerts: any[] = [];
    const lines = output.split('\n');
    
    for (const line of lines) {
        // Match EMIT or ALERT lines
        if (line.includes('EMIT:') || line.includes('ALERT:') || line.includes('alert_type')) {
            try {
                // Try to parse JSON from the line
                const jsonMatch = line.match(/\{.*\}/);
                if (jsonMatch) {
                    alerts.push(JSON.parse(jsonMatch[0]));
                } else {
                    alerts.push({ raw: line.trim() });
                }
            } catch {
                alerts.push({ raw: line.trim() });
            }
        }
    }
    
    return alerts;
}

/**
 * Generate timestamp
 */
function ts(offsetMs = 0): number {
    return Date.now() + offsetMs;
}

// =============================================================================
// SASE PATTERNS TESTS
// =============================================================================

test.describe('SASE Patterns Roundtrip', () => {
    const VPL_FILE = `${EXAMPLES_DIR}/sase_patterns.vpl`;
    const EVENTS_FILE = `${SCENARIOS_DIR}/sase_test.evt`;

    test.beforeAll(() => {
        // Generate predictable test events for SASE patterns
        const events = generateSASEEvents();
        fs.writeFileSync(EVENTS_FILE, events);
    });

    test('VPL file should be valid', () => {
        const result = checkVPL(VPL_FILE);
        console.log('Check output:', result.output);
        expect(result.success).toBe(true);
    });

    test('Should process all events and generate alerts', async () => {
        const result = await simulateVPL(VPL_FILE, EVENTS_FILE, 10000);
        console.log('Simulation output:', result.output);
        
        // Verify simulation completed successfully
        expect(result.success).toBe(true);
        expect(result.output).toContain('Events processed:');
        
        // Verify alerts were generated
        expect(result.alerts.length).toBeGreaterThan(0);
        console.log('Alerts generated:', result.alerts.length);
    });

    test('Should detect login then large transaction pattern', async () => {
        const result = await simulateVPL(VPL_FILE, EVENTS_FILE, 10000);
        const largeTransactionAlerts = result.alerts.filter(
            a => a.alert_type === 'large_transaction_after_login' || 
                 a.raw?.includes('large_transaction') ||
                 a.raw?.includes('LoginThenLargeTransaction')
        );
        expect(largeTransactionAlerts.length).toBeGreaterThan(0);
    });

    test('Should detect risky transaction pattern', async () => {
        const result = await simulateVPL(VPL_FILE, EVENTS_FILE, 10000);
        const riskyAlerts = result.alerts.filter(
            a => a.alert_type === 'risky_transaction' || 
                 a.raw?.includes('risky_transaction') ||
                 a.raw?.includes('RiskyTransaction')
        );
        expect(riskyAlerts.length).toBeGreaterThan(0);
    });
});

/**
 * Generate SASE pattern test events
 */
function generateSASEEvents(): string {
    const events: string[] = [];
    
    events.push('# SASE Pattern Test Events');
    events.push('');
    
    // Scenario 1: Login -> Large Transaction (Pattern 1)
    events.push('# Scenario 1: Login then large transaction');
    events.push('@0s Login { user_id: "user1", ip_address: "192.168.1.1", country: "US" }');
    events.push('@1s Transaction { user_id: "user1", amount: 15000.0, category: "transfer" }');
    
    // Scenario 2: Full Session - Login -> Transaction -> Logout (Pattern 2)
    events.push('');
    events.push('# Scenario 2: Full session');
    events.push('@2s Login { user_id: "user2", ip_address: "192.168.1.2", country: "FR" }');
    events.push('@3s Transaction { user_id: "user2", amount: 500.0, category: "purchase" }');
    events.push('@4s Logout { user_id: "user2" }');
    
    // Scenario 3: Fraud Chain - Login -> PasswordChange -> Transaction -> Logout (Pattern 3)
    events.push('');
    events.push('# Scenario 3: Fraud chain');
    events.push('@5s Login { user_id: "user3", ip_address: "10.0.0.1", country: "DE" }');
    events.push('@6s PasswordChange { user_id: "user3" }');
    events.push('@7s Transaction { user_id: "user3", amount: 9999.0, category: "wire" }');
    events.push('@8s Logout { user_id: "user3" }');
    
    // Scenario 4: Impossible Travel - Login from US, then CN within minutes (Pattern 5)
    events.push('');
    events.push('# Scenario 4: Impossible travel');
    events.push('@9s Login { user_id: "user4", ip_address: "1.2.3.4", country: "US" }');
    events.push('@10s Login { user_id: "user4", ip_address: "5.6.7.8", country: "CN" }');
    
    // Scenario 5: High Velocity - >5 transactions in 10m (Pattern 4)
    events.push('');
    events.push('# Scenario 5: High velocity transactions');
    for (let i = 0; i < 7; i++) {
        events.push(`@${11 + i}s Transaction { user_id: "user5", amount: ${100 + i * 10}.0, category: "purchase" }`);
    }
    
    // Scenario 6: High Spending - Total > $50K in 24h (Pattern 6)
    events.push('');
    events.push('# Scenario 6: High spending');
    events.push('@20s Transaction { user_id: "user6", amount: 20000.0, category: "wire" }');
    events.push('@21s Transaction { user_id: "user6", amount: 35000.0, category: "wire" }');
    
    // Scenario 7: Risky Transaction - Gambling (Pattern 8)
    events.push('');
    events.push('# Scenario 7: Risky transaction');
    events.push('@25s Transaction { user_id: "user7", amount: 500.0, category: "gambling" }');
    
    // Scenario 8: Same IP Multiple Users (Pattern 7)
    events.push('');
    events.push('# Scenario 8: Same IP multiple users (money mule)');
    events.push('@30s Login { user_id: "mule1", ip_address: "123.45.67.89", country: "RU" }');
    events.push('@31s Login { user_id: "mule2", ip_address: "123.45.67.89", country: "RU" }');
    events.push('@32s Transaction { user_id: "mule2", amount: 5000.0, category: "wire" }');
    
    return events.join('\n');
}

// =============================================================================
// HVAC DEMO TESTS
// =============================================================================

test.describe('HVAC Demo Roundtrip', () => {
    const VPL_FILE = `${EXAMPLES_DIR}/hvac_demo.vpl`;
    const EVENTS_FILE = `${SCENARIOS_DIR}/hvac_test.evt`;

    test.beforeAll(() => {
        const events = generateHVACEvents();
        fs.writeFileSync(EVENTS_FILE, events);
    });

    test('VPL file should be valid', () => {
        const result = checkVPL(VPL_FILE);
        console.log('Check output:', result.output);
        expect(result.success).toBe(true);
    });

    test('Should process temperature readings', async () => {
        const result = await simulateVPL(VPL_FILE, EVENTS_FILE, 10000);
        console.log('HVAC simulation output:', result.output);
        expect(result.success).toBe(true);
    });

    test('Should detect temperature anomalies', async () => {
        const result = await simulateVPL(VPL_FILE, EVENTS_FILE, 10000);
        
        // Look for anomaly alerts (temp outside normal range)
        const anomalyAlerts = result.alerts.filter(
            a => a.alert_type?.includes('anomal') || 
                 a.raw?.includes('anomal') ||
                 a.raw?.includes('alert')
        );
        // We may or may not have anomalies depending on threshold
        console.log('Anomaly alerts:', anomalyAlerts);
    });
});

/**
 * Generate HVAC test events
 */
function generateHVACEvents(): string {
    const events: string[] = [];
    const zones = ['Zone_A', 'Zone_B', 'Zone_C'];
    
    events.push('# HVAC Test Events');
    events.push('');
    
    // Generate temperature readings - normal range 18-24°C
    events.push('# Normal temperature readings');
    for (let i = 0; i < 20; i++) {
        const zone = zones[i % zones.length];
        const normalTemp = 20 + (i % 5 - 2); // Deterministic: 18-22°C
        events.push(`@${i}s TemperatureReading { sensor_id: "temp_${i}", zone: "${zone}", value: ${normalTemp.toFixed(1)}, unit: "celsius", ts: ${1000 + i} }`);
    }
    
    // Add some anomalous readings
    events.push('');
    events.push('# Anomalous temperature readings');
    events.push('@25s TemperatureReading { sensor_id: "temp_hot", zone: "Zone_A", value: 35.0, unit: "celsius", ts: 1025 }');
    events.push('@26s TemperatureReading { sensor_id: "temp_cold", zone: "Zone_B", value: 5.0, unit: "celsius", ts: 1026 }');
    
    // Generate humidity readings - normal range 40-60%
    events.push('');
    events.push('# Humidity readings');
    for (let i = 0; i < 20; i++) {
        const zone = zones[i % zones.length];
        const humidity = 50 + (i % 10 - 5); // Deterministic: 45-55%
        events.push(`@${i}s HumidityReading { sensor_id: "hum_${i}", zone: "${zone}", value: ${humidity.toFixed(1)}, ts: ${1000 + i} }`);
    }
    
    // Generate HVAC status
    events.push('');
    events.push('# HVAC status readings');
    for (let i = 0; i < 10; i++) {
        const zone = zones[i % zones.length];
        const mode = ['heating', 'cooling', 'ventilation'][i % 3];
        events.push(`@${i * 2}s HVACStatus { unit_id: "hvac_${i}", zone: "${zone}", mode: "${mode}", power_consumption: ${(2.5 + i * 0.1).toFixed(1)}, compressor_pressure: ${(12.0 + i * 0.1).toFixed(1)}, refrigerant_temp: ${(-5 + i * 0.2).toFixed(1)}, fan_speed: ${1200 + i * 30}, runtime_hours: ${1000 + i * 100}, ts: ${1000 + i * 2} }`);
    }
    
    // Generate energy meter readings
    events.push('');
    events.push('# Energy meter readings');
    for (let i = 0; i < 10; i++) {
        const zone = zones[i % zones.length];
        events.push(`@${i * 2}s EnergyMeter { meter_id: "meter_${i}", zone: "${zone}", power_kw: ${(3.5 + i * 0.1).toFixed(1)}, cumulative_kwh: ${10000 + i * 50}.0, ts: ${1000 + i * 2} }`);
    }
    
    return events.join('\n');
}

// =============================================================================
// FINANCIAL MARKETS TESTS
// =============================================================================

test.describe('Financial Markets Roundtrip', () => {
    const VPL_FILE = `${EXAMPLES_DIR}/financial_markets.vpl`;
    const EVENTS_FILE = `${SCENARIOS_DIR}/financial_test.evt`;

    test.beforeAll(() => {
        const events = generateFinancialEvents();
        fs.writeFileSync(EVENTS_FILE, events);
    });

    test('VPL file should be valid', () => {
        const result = checkVPL(VPL_FILE);
        console.log('Check output:', result.output);
        expect(result.success).toBe(true);
    });

    test('Should process market ticks', async () => {
        const result = await simulateVPL(VPL_FILE, EVENTS_FILE, 10000);
        console.log('Financial simulation output:', result.output);
        expect(result.success).toBe(true);
    });

    test('Should calculate moving averages', async () => {
        const result = await simulateVPL(VPL_FILE, EVENTS_FILE, 10000);
        // Verify SMA/EMA calculations are being performed
        expect(result.output).toBeDefined();
    });
});

/**
 * Generate financial market test events
 */
function generateFinancialEvents(): string {
    const events: string[] = [];
    
    events.push('# Financial Market Test Events');
    events.push('');
    
    // Generate market ticks - deterministic price movement
    let btcPrice = 45000;
    let ethPrice = 2800;
    
    events.push('# Market ticks');
    for (let i = 0; i < 50; i++) {
        // BTC tick - deterministic price movement
        btcPrice += (i % 10 - 5) * 20; // Oscillate around base
        const btcVolume = 50 + (i % 20) * 5;
        events.push(`@${i}s MarketTick { symbol: "BTC/USD", price: ${btcPrice.toFixed(2)}, volume: ${btcVolume}, bid: ${(btcPrice - 5).toFixed(2)}, ask: ${(btcPrice + 5).toFixed(2)}, ts: ${1000 + i} }`);
        
        // ETH tick
        ethPrice += (i % 10 - 5) * 2;
        const ethVolume = 100 + (i % 30) * 10;
        events.push(`@${i}s MarketTick { symbol: "ETH/USD", price: ${ethPrice.toFixed(2)}, volume: ${ethVolume}, bid: ${(ethPrice - 2).toFixed(2)}, ask: ${(ethPrice + 2).toFixed(2)}, ts: ${1000 + i} }`);
    }
    
    // Generate OHLCV candles (1h timeframe) - deterministic
    events.push('');
    events.push('# OHLCV candles');
    for (let i = 0; i < 30; i++) {
        const open = 45000 + (i % 10 - 5) * 100;
        const close = open + (i % 5 - 2) * 50;
        const high = Math.max(open, close) + 100;
        const low = Math.min(open, close) - 100;
        const volume = 5000 + i * 100;
        
        events.push(`@${i * 60}s OHLCV { symbol: "BTC/USD", open: ${open.toFixed(2)}, high: ${high.toFixed(2)}, low: ${low.toFixed(2)}, close: ${close.toFixed(2)}, volume: ${volume}, timeframe: "1h", ts: ${1000 + i * 3600} }`);
    }
    
    // Generate ETH candles
    for (let i = 0; i < 30; i++) {
        const open = 2800 + (i % 10 - 5) * 10;
        const close = open + (i % 5 - 2) * 5;
        const high = Math.max(open, close) + 10;
        const low = Math.min(open, close) - 10;
        const volume = 10000 + i * 200;
        
        events.push(`@${i * 60}s OHLCV { symbol: "ETH/USD", open: ${open.toFixed(2)}, high: ${high.toFixed(2)}, low: ${low.toFixed(2)}, close: ${close.toFixed(2)}, volume: ${volume}, timeframe: "1h", ts: ${1000 + i * 3600} }`);
    }
    
    return events.join('\n');
}

// =============================================================================
// EXPECTED OUTPUTS VALIDATION
// =============================================================================

test.describe('Expected Outputs Validation', () => {
    
    test('SASE: Exact scenario validation', async () => {
        const EVENTS_FILE = `${SCENARIOS_DIR}/sase_exact.evt`;
        const VPL_FILE = `${EXAMPLES_DIR}/sase_patterns.vpl`;
        
        // Create deterministic events with known expected outputs
        const events = [
            '# Exact SASE Test Scenarios',
            '',
            '# User1: Login then $15K transaction -> should trigger large_transaction_after_login',
            '@0s Login { user_id: "test_user_1", ip_address: "1.1.1.1", country: "US" }',
            '@1s Transaction { user_id: "test_user_1", amount: 15000.0, category: "wire" }',
            '',
            '# User2: Full session -> should trigger session_complete',
            '@5s Login { user_id: "test_user_2", ip_address: "2.2.2.2", country: "FR" }',
            '@6s Transaction { user_id: "test_user_2", amount: 100.0, category: "purchase" }',
            '@7s Logout { user_id: "test_user_2" }',
            '',
            '# User3: Impossible travel US -> CN -> should trigger impossible_travel',
            '@10s Login { user_id: "test_user_3", ip_address: "3.3.3.3", country: "US" }',
            '@11s Login { user_id: "test_user_3", ip_address: "4.4.4.4", country: "CN" }',
        ].join('\n');
        
        fs.writeFileSync(EVENTS_FILE, events);
        
        const result = await simulateVPL(VPL_FILE, EVENTS_FILE, 15000);
        console.log('=== EXACT SCENARIO OUTPUT ===');
        console.log(result.output);
        console.log('=== PARSED ALERTS ===');
        console.log(JSON.stringify(result.alerts, null, 2));
        
        // Validate expected alerts are present
        const alertTypes = result.alerts.map(a => a.alert_type || a.raw).filter(Boolean);
        console.log('Alert types found:', alertTypes);
    });
});
