import { test, expect } from '@playwright/test'

// ---------------------------------------------------------------------------
// Config -- expects k3d-saas cluster with MailPit + backend running
// ---------------------------------------------------------------------------

const API_BASE = 'http://localhost:8080'
const MAILPIT_API = 'http://localhost:8025'

const ADMIN_CREDS = { username: 'admin', password: 'admin123' }

const TENANT_A = {
  orgName: `HierA_${Date.now()}`,
  username: `hiera_${Date.now()}`,
  email: `hiera_${Date.now()}@varpulis.test`,
  password: 'TestPass123!',
  jwt: '',
  orgId: '',
  apiKey: '',
}

const SUB_TENANT_A1 = {
  name: `SubA1_${Date.now()}`,
  id: '',
  apiKey: '',
}

let adminJwt = ''

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

async function createApiKey(jwt: string, orgId?: string): Promise<string> {
  const oid = orgId || (await getOrgId(jwt))
  const res = await fetch(`${API_BASE}/api/v1/orgs/${oid}/api-keys`, {
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
// Pipeline helpers
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
): Promise<Array<{ id: string; name: string; scope_level?: string; inherited_from_org_id?: string }>> {
  const res = await fetch(`${API_BASE}/api/v1/pipelines`, {
    headers: { 'X-API-Key': apiKey },
  })
  expect(res.status).toBe(200)
  const data = await res.json()
  return data.pipelines
}

// ---------------------------------------------------------------------------
// Test suite -- serial (tests depend on each other)
// ---------------------------------------------------------------------------

test.describe.serial('Tenant hierarchy: Global -> Tenant -> Sub-Tenant', () => {
  test('0. Setup: create Tenant A and admin login', async () => {
    await clearMailpit()

    // Sign up Tenant A
    await signup(TENANT_A)
    await verifyEmail(TENANT_A.email)
    TENANT_A.jwt = await login(TENANT_A.username, TENANT_A.password)
    TENANT_A.orgId = await getOrgId(TENANT_A.jwt)
    TENANT_A.apiKey = await createApiKey(TENANT_A.jwt)

    // Admin login
    adminJwt = await login(ADMIN_CREDS.username, ADMIN_CREDS.password)

    expect(TENANT_A.orgId).toBeTruthy()
    expect(TENANT_A.apiKey).toBeTruthy()
    expect(adminJwt).toBeTruthy()
  })

  test('1. Org list returns org_type and parent_org_id', async () => {
    const res = await fetch(`${API_BASE}/api/v1/orgs`, {
      headers: { Authorization: `Bearer ${TENANT_A.jwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()
    const org = data.organizations.find((o: any) => o.id === TENANT_A.orgId)
    expect(org).toBeTruthy()
    expect(org.org_type).toBe('tenant')
    expect(org.parent_org_id).toBeNull()
  })

  test('2. Tenant A creates a sub-tenant', async () => {
    const res = await fetch(`${API_BASE}/api/v1/orgs/${TENANT_A.orgId}/sub-tenants`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${TENANT_A.jwt}`,
      },
      body: JSON.stringify({ name: SUB_TENANT_A1.name }),
    })
    expect(res.status).toBe(201)
    const data = await res.json()
    SUB_TENANT_A1.id = data.id
    expect(data.org_type).toBe('sub_tenant')
    expect(data.parent_org_id).toBe(TENANT_A.orgId)
    expect(data.name).toBe(SUB_TENANT_A1.name)
  })

  test('3. List sub-tenants returns the created sub-tenant', async () => {
    const res = await fetch(`${API_BASE}/api/v1/orgs/${TENANT_A.orgId}/sub-tenants`, {
      headers: { Authorization: `Bearer ${TENANT_A.jwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()
    expect(data.sub_tenants.length).toBeGreaterThanOrEqual(1)
    const sub = data.sub_tenants.find((s: any) => s.id === SUB_TENANT_A1.id)
    expect(sub).toBeTruthy()
    expect(sub.org_type).toBe('sub_tenant')
    expect(sub.parent_org_id).toBe(TENANT_A.orgId)
  })

  test('4. Sub-tenant cannot create sub-sub-tenant (max 3 levels)', async () => {
    // Create an API key for the sub-tenant by using admin jwt with X-Org-Id
    SUB_TENANT_A1.apiKey = await createApiKey(TENANT_A.jwt, SUB_TENANT_A1.id)

    // Try to create a sub-sub-tenant under the sub-tenant
    const res = await fetch(`${API_BASE}/api/v1/orgs/${SUB_TENANT_A1.id}/sub-tenants`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${TENANT_A.jwt}`,
      },
      body: JSON.stringify({ name: 'SubSub_should_fail' }),
    })
    // Should be 400 because sub_tenant org_type != tenant
    expect(res.status).toBe(400)
  })

  test('5. Admin tenant list includes hierarchy info', async () => {
    const res = await fetch(`${API_BASE}/api/v1/admin/tenants`, {
      headers: { Authorization: `Bearer ${adminJwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()

    // Find tenant A
    const tenantA = data.tenants.find((t: any) => t.id === TENANT_A.orgId)
    expect(tenantA).toBeTruthy()
    expect(tenantA.org_type).toBe('tenant')

    // Find sub-tenant A1
    const subA1 = data.tenants.find((t: any) => t.id === SUB_TENANT_A1.id)
    expect(subA1).toBeTruthy()
    expect(subA1.org_type).toBe('sub_tenant')
    expect(subA1.parent_org_id).toBe(TENANT_A.orgId)
  })

  test('6. Admin tenant detail includes sub_tenants array', async () => {
    const res = await fetch(`${API_BASE}/api/v1/admin/tenants/${TENANT_A.orgId}`, {
      headers: { Authorization: `Bearer ${adminJwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()
    expect(data.org_type).toBe('tenant')
    expect(data.sub_tenants).toBeDefined()
    expect(data.sub_tenants.length).toBeGreaterThanOrEqual(1)
    const sub = data.sub_tenants.find((s: any) => s.id === SUB_TENANT_A1.id)
    expect(sub).toBeTruthy()
    expect(sub.org_type).toBe('sub_tenant')
  })

  test('7. Pipeline scoping: tenant pipeline has own scope by default', async () => {
    const pipelineId = await deployPipeline(
      TENANT_A.apiKey,
      'tenant-private',
      'stream TenantPrivate = Events .emit()'
    )
    expect(pipelineId).toBeTruthy()

    const pipelines = await listPipelines(TENANT_A.apiKey)
    const p = pipelines.find((pl) => pl.id === pipelineId)
    expect(p).toBeTruthy()
    expect(p!.scope_level).toBe('own')
    expect(p!.inherited_from_org_id).toBeFalsy()
  })
})
