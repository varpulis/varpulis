import { test, expect } from '@playwright/test'

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const API_BASE = 'http://localhost:8080'
const MAILPIT_API = 'http://localhost:8025'

const ADMIN_CREDS = { username: 'admin', password: 'admin123' }

const TENANT = {
  orgName: `UIHier_${Date.now()}`,
  username: `uihier_${Date.now()}`,
  email: `uihier_${Date.now()}@varpulis.test`,
  password: 'TestPass123',
  jwt: '',
  orgId: '',
  apiKey: '',
}

const SUB_TENANT = {
  name: `SubUIHier_${Date.now()}`,
  id: '',
}

let adminJwt = ''

// ---------------------------------------------------------------------------
// Helpers
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

async function signup(orgName: string, username: string, email: string, password: string) {
  const res = await fetch(`${API_BASE}/auth/register`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ org_name: orgName, username, email, password }),
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

async function createApiKey(jwt: string, orgId: string): Promise<string> {
  const res = await fetch(`${API_BASE}/api/v1/orgs/${orgId}/api-keys`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${jwt}`,
    },
    body: JSON.stringify({ name: 'e2e-ui-hierarchy' }),
  })
  expect(res.status).toBe(201)
  const data = await res.json()
  return data.api_key
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test.describe.serial('Phase 6: UI Hierarchy', () => {
  test('0. Setup: create tenant with sub-tenant', async () => {
    await clearMailpit()

    await signup(TENANT.orgName, TENANT.username, TENANT.email, TENANT.password)
    await verifyEmail(TENANT.email)
    TENANT.jwt = await login(TENANT.username, TENANT.password)
    TENANT.orgId = await getOrgId(TENANT.jwt)
    TENANT.apiKey = await createApiKey(TENANT.jwt, TENANT.orgId)

    // Create sub-tenant
    const subRes = await fetch(`${API_BASE}/api/v1/orgs/${TENANT.orgId}/sub-tenants`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${TENANT.jwt}`,
      },
      body: JSON.stringify({ name: SUB_TENANT.name }),
    })
    expect(subRes.status).toBe(201)
    SUB_TENANT.id = (await subRes.json()).id

    adminJwt = await login(ADMIN_CREDS.username, ADMIN_CREDS.password)

    expect(TENANT.orgId).toBeTruthy()
    expect(SUB_TENANT.id).toBeTruthy()
    expect(adminJwt).toBeTruthy()
  })

  test('1. Org list includes org_type and parent_org_id', async () => {
    const res = await fetch(`${API_BASE}/api/v1/orgs`, {
      headers: { Authorization: `Bearer ${TENANT.jwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()

    // Find the tenant org
    const tenantOrg = data.organizations.find((o: any) => o.id === TENANT.orgId)
    expect(tenantOrg).toBeTruthy()
    expect(tenantOrg.org_type).toBe('tenant')
    expect(tenantOrg.parent_org_id).toBeNull()
  })

  test('2. Admin tenant list includes org_type for hierarchy display', async () => {
    const res = await fetch(`${API_BASE}/api/v1/admin/tenants`, {
      headers: { Authorization: `Bearer ${adminJwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()

    // Find our tenant in the list
    const tenant = data.tenants.find((t: any) => t.id === TENANT.orgId)
    expect(tenant).toBeTruthy()
    expect(tenant.org_type).toBeDefined()
  })

  test('3. Admin tenant detail includes sub_tenants array', async () => {
    const res = await fetch(`${API_BASE}/api/v1/admin/tenants/${TENANT.orgId}`, {
      headers: { Authorization: `Bearer ${adminJwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()

    expect(data.sub_tenants).toBeDefined()
    expect(data.sub_tenants.length).toBeGreaterThan(0)

    const sub = data.sub_tenants.find((s: any) => s.id === SUB_TENANT.id)
    expect(sub).toBeTruthy()
    expect(sub.name).toBe(SUB_TENANT.name)
    expect(sub.org_type).toBe('sub_tenant')
  })

  test('4. Admin tenant detail pipelines include read_only field', async () => {
    // Deploy a pipeline first
    const deployRes = await fetch(`${API_BASE}/api/v1/pipelines`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-API-Key': TENANT.apiKey,
      },
      body: JSON.stringify({
        name: 'ui-hier-pipeline',
        source: 'stream UIHier = Events .emit()',
      }),
    })
    expect(deployRes.status).toBe(201)

    // Check admin detail
    const res = await fetch(`${API_BASE}/api/v1/admin/tenants/${TENANT.orgId}`, {
      headers: { Authorization: `Bearer ${adminJwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()

    expect(data.pipelines.length).toBeGreaterThan(0)
    for (const p of data.pipelines) {
      expect(p.read_only).toBeDefined()
      expect(typeof p.read_only).toBe('boolean')
    }

    // Own pipeline should not be read_only
    const ownPipeline = data.pipelines.find((p: any) => p.name === 'ui-hier-pipeline')
    if (ownPipeline) {
      expect(ownPipeline.read_only).toBe(false)
    }
  })

  test('5. Pipeline list via runtime API includes scope_level', async () => {
    const res = await fetch(`${API_BASE}/api/v1/pipelines`, {
      headers: { 'X-API-Key': TENANT.apiKey },
    })
    expect(res.status).toBe(200)
    const data = await res.json()
    expect(data.pipelines.length).toBeGreaterThan(0)

    for (const p of data.pipelines) {
      expect(p.scope_level).toBeDefined()
      expect(typeof p.read_only).toBe('boolean')
    }
  })

  test('6. Org-level pipeline list includes scope badges data', async () => {
    // Deploy a global pipeline
    const globalRes = await fetch(`${API_BASE}/api/v1/admin/global-pipelines`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${adminJwt}`,
      },
      body: JSON.stringify({
        name: 'ui-global-badge',
        vpl_source: 'stream UIGlobalBadge = Events .emit()',
      }),
    })
    expect(globalRes.status).toBe(201)

    // Check org-level pipeline list
    const res = await fetch(`${API_BASE}/api/v1/orgs/${TENANT.orgId}/pipelines`, {
      headers: { Authorization: `Bearer ${TENANT.jwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()

    // Should have at least own + global
    expect(data.pipelines.length).toBeGreaterThan(0)

    // Each pipeline has read_only and scope_level
    for (const p of data.pipelines) {
      expect(p.read_only).toBeDefined()
      expect(p.scope_level).toBeDefined()
    }

    // Global pipeline should be read_only
    const globalPipeline = data.pipelines.find((p: any) => p.name === 'ui-global-badge')
    if (globalPipeline) {
      expect(globalPipeline.read_only).toBe(true)
      expect(globalPipeline.scope_level).toBe('global')
    }

    // Own pipeline should NOT be read_only
    const ownPipeline = data.pipelines.find((p: any) => p.name === 'ui-hier-pipeline')
    if (ownPipeline) {
      expect(ownPipeline.read_only).toBe(false)
      expect(ownPipeline.scope_level).toBe('own')
    }
  })
})
