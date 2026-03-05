import { test, expect } from '@playwright/test'

// ---------------------------------------------------------------------------
// Config — expects k3d-saas cluster with MailPit + backend running
// ---------------------------------------------------------------------------

const API_BASE = 'http://localhost:8080'
const MAILPIT_API = 'http://localhost:8025'

const ADMIN_CREDS = { username: 'admin', password: 'admin123' }

const TENANT_A = {
  orgName: `IsoA_${Date.now()}`,
  username: `isoa_${Date.now()}`,
  email: `isoa_${Date.now()}@varpulis.test`,
  password: 'TestPass123!',
  apiKey: '',
  jwt: '',
}

const TENANT_B = {
  orgName: `IsoB_${Date.now()}`,
  username: `isob_${Date.now()}`,
  email: `isob_${Date.now()}@varpulis.test`,
  password: 'TestPass123!',
  apiKey: '',
  jwt: '',
}

let adminJwt = ''

// VPL source for test pipelines
const TEST_VPL = 'stream TestStream = Events .emit()'
const GLOBAL_VPL = 'stream GlobalStream = Events .emit()'
const GLOBAL_NAME = `global-test-${Date.now()}`
let globalTemplateId = ''

// Track A's pipeline ID for cross-tenant tests
let tenantAPipelineId = ''

// ---------------------------------------------------------------------------
// MailPit helpers
// ---------------------------------------------------------------------------

async function clearMailpit() {
  await fetch(`${MAILPIT_API}/api/v1/messages`, { method: 'DELETE' })
}

async function getVerificationToken(toAddress: string, maxWaitMs = 15000): Promise<string> {
  const deadline = Date.now() + maxWaitMs
  while (Date.now() < deadline) {
    const res = await fetch(
      `${MAILPIT_API}/api/v1/search?query=to:${encodeURIComponent(toAddress)}`
    )
    const data = await res.json()
    if (data.messages && data.messages.length > 0) {
      const msgId = data.messages[0].ID
      const msgRes = await fetch(`${MAILPIT_API}/api/v1/message/${msgId}`)
      const msg = await msgRes.json()
      const text: string = msg.Text || ''
      const match = text.match(/token=([A-Za-z0-9]+)/)
      if (match) return match[1]
    }
    await new Promise((r) => setTimeout(r, 500))
  }
  throw new Error(`No verification email found for ${toAddress} within ${maxWaitMs}ms`)
}

// ---------------------------------------------------------------------------
// Auth helpers
// ---------------------------------------------------------------------------

async function signup(tenant: typeof TENANT_A) {
  const res = await fetch(`${API_BASE}/auth/register`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      org_name: tenant.orgName,
      username: tenant.username,
      email: tenant.email,
      password: tenant.password,
    }),
  })
  expect(res.status).toBe(201)
}

async function verifyEmail(email: string) {
  const token = await getVerificationToken(email)
  const res = await fetch(`${API_BASE}/auth/verify?token=${token}`)
  expect(res.status).toBe(200)
}

async function login(username: string, password: string): Promise<string> {
  const res = await fetch(`${API_BASE}/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username, password }),
  })
  expect(res.status).toBe(200)
  const data = await res.json()
  return data.token
}

async function getOrgId(jwt: string): Promise<string> {
  const res = await fetch(`${API_BASE}/api/v1/orgs`, {
    headers: { Authorization: `Bearer ${jwt}` },
  })
  expect(res.status).toBe(200)
  const data = await res.json()
  expect(data.organizations.length).toBeGreaterThan(0)
  return data.organizations[0].id
}

async function createApiKey(jwt: string): Promise<string> {
  const orgId = await getOrgId(jwt)
  const res = await fetch(`${API_BASE}/api/v1/orgs/${orgId}/api-keys`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${jwt}`,
    },
    body: JSON.stringify({ name: 'e2e-test' }),
  })
  expect(res.status).toBe(201)
  const data = await res.json()
  return data.api_key
}

// ---------------------------------------------------------------------------
// Pipeline API helpers (X-API-Key auth)
// ---------------------------------------------------------------------------

async function deployPipeline(
  apiKey: string,
  name: string,
  source: string
): Promise<string> {
  const res = await fetch(`${API_BASE}/api/v1/pipelines`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-API-Key': apiKey,
    },
    body: JSON.stringify({ name, source }),
  })
  expect(res.status).toBe(201)
  const data = await res.json()
  return data.id
}

async function listPipelines(
  apiKey: string
): Promise<Array<{ id: string; name: string; global_template_id?: string }>> {
  const res = await fetch(`${API_BASE}/api/v1/pipelines`, {
    headers: { 'X-API-Key': apiKey },
  })
  expect(res.status).toBe(200)
  const data = await res.json()
  return data.pipelines
}

async function deletePipeline(apiKey: string, pipelineId: string): Promise<number> {
  const res = await fetch(`${API_BASE}/api/v1/pipelines/${pipelineId}`, {
    method: 'DELETE',
    headers: { 'X-API-Key': apiKey },
  })
  return res.status
}

// ---------------------------------------------------------------------------
// Admin API helpers (JWT auth)
// ---------------------------------------------------------------------------

async function adminDeployGlobal(
  jwt: string,
  name: string,
  vplSource: string
): Promise<{ template_id: string; copies_created: number }> {
  const res = await fetch(`${API_BASE}/api/v1/admin/global-pipelines`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${jwt}`,
    },
    body: JSON.stringify({ name, vpl_source: vplSource }),
  })
  expect(res.status).toBe(201)
  return res.json()
}

async function adminUndeployGlobal(jwt: string, templateId: string): Promise<number> {
  const res = await fetch(`${API_BASE}/api/v1/admin/global-pipelines/${templateId}`, {
    method: 'DELETE',
    headers: { Authorization: `Bearer ${jwt}` },
  })
  return res.status
}

// ---------------------------------------------------------------------------
// Test suite — serial (tests depend on each other)
// ---------------------------------------------------------------------------

test.describe.serial('Pipeline tenant isolation & global pipelines', () => {
  test('0. Setup: create Tenant A and Tenant B', async () => {
    await clearMailpit()

    // Sign up Tenant A
    await signup(TENANT_A)
    await verifyEmail(TENANT_A.email)
    TENANT_A.jwt = await login(TENANT_A.username, TENANT_A.password)
    TENANT_A.apiKey = await createApiKey(TENANT_A.jwt)

    await clearMailpit()

    // Sign up Tenant B
    await signup(TENANT_B)
    await verifyEmail(TENANT_B.email)
    TENANT_B.jwt = await login(TENANT_B.username, TENANT_B.password)
    TENANT_B.apiKey = await createApiKey(TENANT_B.jwt)

    // Admin login
    adminJwt = await login(ADMIN_CREDS.username, ADMIN_CREDS.password)

    expect(TENANT_A.apiKey).toBeTruthy()
    expect(TENANT_B.apiKey).toBeTruthy()
    expect(adminJwt).toBeTruthy()
  })

  test('1. Tenant A deploys pipeline, B cannot see it', async () => {
    // A deploys a pipeline
    tenantAPipelineId = await deployPipeline(TENANT_A.apiKey, 'a-private', TEST_VPL)
    expect(tenantAPipelineId).toBeTruthy()

    // A can see it
    const aPipelines = await listPipelines(TENANT_A.apiKey)
    expect(aPipelines.some((p) => p.id === tenantAPipelineId)).toBe(true)

    // B cannot see it
    const bPipelines = await listPipelines(TENANT_B.apiKey)
    expect(bPipelines.some((p) => p.id === tenantAPipelineId)).toBe(false)
  })

  test('2. Cross-tenant delete rejected', async () => {
    // B tries to delete A's pipeline
    const status = await deletePipeline(TENANT_B.apiKey, tenantAPipelineId)
    // Should get 404 (pipeline not found in B's namespace)
    expect(status).toBe(404)

    // A's pipeline still exists
    const aPipelines = await listPipelines(TENANT_A.apiKey)
    expect(aPipelines.some((p) => p.id === tenantAPipelineId)).toBe(true)
  })

  test('3. Admin deploys global pipeline, both tenants see it', async () => {
    const result = await adminDeployGlobal(adminJwt, GLOBAL_NAME, GLOBAL_VPL)
    globalTemplateId = result.template_id
    expect(globalTemplateId).toBeTruthy()
    expect(result.copies_created).toBeGreaterThanOrEqual(2)

    // Both tenants should see the global pipeline
    const aPipelines = await listPipelines(TENANT_A.apiKey)
    const aGlobal = aPipelines.find((p) => p.global_template_id === globalTemplateId)
    expect(aGlobal).toBeTruthy()
    expect(aGlobal!.name).toBe(GLOBAL_NAME)

    const bPipelines = await listPipelines(TENANT_B.apiKey)
    const bGlobal = bPipelines.find((p) => p.global_template_id === globalTemplateId)
    expect(bGlobal).toBeTruthy()
    expect(bGlobal!.name).toBe(GLOBAL_NAME)
  })

  test('4. Tenant cannot delete global pipeline', async () => {
    // Find A's global pipeline ID
    const aPipelines = await listPipelines(TENANT_A.apiKey)
    const aGlobal = aPipelines.find((p) => p.global_template_id === globalTemplateId)
    expect(aGlobal).toBeTruthy()

    // A tries to delete it — should get 403
    const status = await deletePipeline(TENANT_A.apiKey, aGlobal!.id)
    expect(status).toBe(403)

    // Pipeline still exists
    const aPipelinesAfter = await listPipelines(TENANT_A.apiKey)
    expect(aPipelinesAfter.some((p) => p.global_template_id === globalTemplateId)).toBe(true)
  })

  test('5. Admin undeploys global, removed from all tenants', async () => {
    const status = await adminUndeployGlobal(adminJwt, globalTemplateId)
    expect(status).toBe(200)

    // Both tenants should no longer have the global pipeline
    const aPipelines = await listPipelines(TENANT_A.apiKey)
    expect(aPipelines.some((p) => p.global_template_id === globalTemplateId)).toBe(false)

    const bPipelines = await listPipelines(TENANT_B.apiKey)
    expect(bPipelines.some((p) => p.global_template_id === globalTemplateId)).toBe(false)
  })

  test('6. Cleanup: delete A test pipeline', async () => {
    const status = await deletePipeline(TENANT_A.apiKey, tenantAPipelineId)
    expect(status).toBe(200)

    const aPipelines = await listPipelines(TENANT_A.apiKey)
    expect(aPipelines.some((p) => p.id === tenantAPipelineId)).toBe(false)
  })
})
