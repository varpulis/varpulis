import { test, expect } from '@playwright/test'

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const API_BASE = 'http://localhost:8080'
const MAILPIT_API = 'http://localhost:8025'

const ADMIN_CREDS = { username: 'admin', password: 'admin123' }

const TENANT = {
  orgName: `NSTest_${Date.now()}`,
  username: `nstest_${Date.now()}`,
  email: `nstest_${Date.now()}@varpulis.test`,
  password: 'TestPass123',
  jwt: '',
  orgId: '',
}

const SUB_TENANT = {
  name: `SubNS_${Date.now()}`,
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test.describe.serial('Phase 3: K8s Per-Tenant Namespaces', () => {
  test('0. Setup: create tenant with sub-tenant', async () => {
    await clearMailpit()

    await signup(TENANT.orgName, TENANT.username, TENANT.email, TENANT.password)
    await verifyEmail(TENANT.email)
    TENANT.jwt = await login(TENANT.username, TENANT.password)
    TENANT.orgId = await getOrgId(TENANT.jwt)

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

  test('1. Namespace not provisioned initially', async () => {
    const res = await fetch(`${API_BASE}/api/v1/admin/tenants/${TENANT.orgId}/namespace`, {
      headers: { Authorization: `Bearer ${adminJwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()
    expect(data.namespace).toBeNull()
    expect(data.org_id).toBe(TENANT.orgId)
  })

  test('2. Provision namespace for tenant', async () => {
    const res = await fetch(`${API_BASE}/api/v1/admin/tenants/${TENANT.orgId}/namespace`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${adminJwt}` },
    })
    expect(res.status).toBe(201)
    const data = await res.json()
    expect(data.namespace).toContain('varpulis-tenant-')
    expect(data.status).toBe('provisioned')
  })

  test('3. Namespace info returns provisioned namespace', async () => {
    const res = await fetch(`${API_BASE}/api/v1/admin/tenants/${TENANT.orgId}/namespace`, {
      headers: { Authorization: `Bearer ${adminJwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()
    expect(data.namespace).toContain('varpulis-tenant-')
    expect(data.org_id).toBe(TENANT.orgId)
  })

  test('4. Duplicate provision returns 409', async () => {
    const res = await fetch(`${API_BASE}/api/v1/admin/tenants/${TENANT.orgId}/namespace`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${adminJwt}` },
    })
    expect(res.status).toBe(409)
  })

  test('5. Sub-tenant cannot provision own namespace', async () => {
    const res = await fetch(`${API_BASE}/api/v1/admin/tenants/${SUB_TENANT.id}/namespace`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${adminJwt}` },
    })
    expect(res.status).toBe(400)
    const data = await res.json()
    expect(data.error).toContain('Sub-tenants share parent namespace')
  })

  test('6. Admin tenant list includes k8s_namespace', async () => {
    const res = await fetch(`${API_BASE}/api/v1/admin/tenants`, {
      headers: { Authorization: `Bearer ${adminJwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()

    const tenant = data.tenants.find((t: any) => t.id === TENANT.orgId)
    expect(tenant).toBeTruthy()
    expect(tenant.k8s_namespace).toContain('varpulis-tenant-')
  })

  test('7. Admin tenant detail includes k8s_namespace', async () => {
    const res = await fetch(`${API_BASE}/api/v1/admin/tenants/${TENANT.orgId}`, {
      headers: { Authorization: `Bearer ${adminJwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()
    expect(data.k8s_namespace).toContain('varpulis-tenant-')
  })

  test('8. Deprovision namespace', async () => {
    const res = await fetch(`${API_BASE}/api/v1/admin/tenants/${TENANT.orgId}/namespace`, {
      method: 'DELETE',
      headers: { Authorization: `Bearer ${adminJwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()
    expect(data.deleted).toBe(true)

    // Verify it's gone
    const checkRes = await fetch(`${API_BASE}/api/v1/admin/tenants/${TENANT.orgId}/namespace`, {
      headers: { Authorization: `Bearer ${adminJwt}` },
    })
    const checkData = await checkRes.json()
    expect(checkData.namespace).toBeNull()
  })
})
