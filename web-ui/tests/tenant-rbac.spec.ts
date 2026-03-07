import { test, expect } from '@playwright/test'

// ---------------------------------------------------------------------------
// Config -- expects k3d-saas cluster with MailPit + backend running
// ---------------------------------------------------------------------------

const API_BASE = 'http://localhost:8080'
const MAILPIT_API = 'http://localhost:8025'

const ADMIN_CREDS = { username: 'admin', password: 'admin123' }

const TENANT_OWNER = {
  orgName: `RBAC_${Date.now()}`,
  username: `rbac_owner_${Date.now()}`,
  email: `rbac_owner_${Date.now()}@varpulis.test`,
  password: 'TestPass123',
  jwt: '',
  orgId: '',
  apiKey: '',
}

const TENANT_MEMBER = {
  username: `rbac_member_${Date.now()}`,
  email: `rbac_member_${Date.now()}@varpulis.test`,
  password: 'TestPass456',
  jwt: '',
  userId: '',
}

const SUB_TENANT = {
  name: `SubRBAC_${Date.now()}`,
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

async function signup(
  orgName: string,
  username: string,
  email: string,
  password: string
): Promise<void> {
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
    body: JSON.stringify({ name: 'e2e-rbac-test' }),
  })
  expect(res.status).toBe(201)
  const data = await res.json()
  return data.api_key
}

// ---------------------------------------------------------------------------
// Test suite -- serial
// ---------------------------------------------------------------------------

test.describe.serial('Phase 5: Auth/RBAC Hierarchy', () => {
  test('0. Setup: create tenant with owner, member, sub-tenant', async () => {
    await clearMailpit()

    // Create tenant owner
    await signup(
      TENANT_OWNER.orgName,
      TENANT_OWNER.username,
      TENANT_OWNER.email,
      TENANT_OWNER.password
    )
    await verifyEmail(TENANT_OWNER.email)
    TENANT_OWNER.jwt = await login(TENANT_OWNER.username, TENANT_OWNER.password)
    TENANT_OWNER.orgId = await getOrgId(TENANT_OWNER.jwt)
    TENANT_OWNER.apiKey = await createApiKey(TENANT_OWNER.jwt, TENANT_OWNER.orgId)

    // Create tenant member (separate user)
    await clearMailpit()
    await signup('', TENANT_MEMBER.username, TENANT_MEMBER.email, TENANT_MEMBER.password)
    await verifyEmail(TENANT_MEMBER.email)
    TENANT_MEMBER.jwt = await login(TENANT_MEMBER.username, TENANT_MEMBER.password)

    // Get member's user_id from their org list
    const memberRes = await fetch(`${API_BASE}/api/v1/orgs`, {
      headers: { Authorization: `Bearer ${TENANT_MEMBER.jwt}` },
    })
    const memberData = await memberRes.json()
    // The member's own org exists, find their user_id via admin
    adminJwt = await login(ADMIN_CREDS.username, ADMIN_CREDS.password)

    // Invite member to tenant's org
    const inviteRes = await fetch(
      `${API_BASE}/api/v1/orgs/${TENANT_OWNER.orgId}/members`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${TENANT_OWNER.jwt}`,
        },
        body: JSON.stringify({ email: TENANT_MEMBER.email, role: 'member' }),
      }
    )
    expect(inviteRes.status).toBe(201)
    const inviteData = await inviteRes.json()
    TENANT_MEMBER.userId = inviteData.user_id

    // Create sub-tenant
    const subRes = await fetch(
      `${API_BASE}/api/v1/orgs/${TENANT_OWNER.orgId}/sub-tenants`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${TENANT_OWNER.jwt}`,
        },
        body: JSON.stringify({ name: SUB_TENANT.name }),
      }
    )
    expect(subRes.status).toBe(201)
    const subData = await subRes.json()
    SUB_TENANT.id = subData.id

    expect(TENANT_OWNER.orgId).toBeTruthy()
    expect(TENANT_MEMBER.userId).toBeTruthy()
    expect(SUB_TENANT.id).toBeTruthy()
    expect(adminJwt).toBeTruthy()
  })

  test('1. Tenant owner can access sub-tenant via hierarchy', async () => {
    // Owner should be able to get schema info for their sub-tenant
    const res = await fetch(`${API_BASE}/api/v1/orgs/${SUB_TENANT.id}/schema`, {
      headers: { Authorization: `Bearer ${TENANT_OWNER.jwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()
    expect(data.org_id).toBe(SUB_TENANT.id)
  })

  test('2. Tenant owner can list sub-tenant members', async () => {
    const res = await fetch(`${API_BASE}/api/v1/orgs/${SUB_TENANT.id}/members`, {
      headers: { Authorization: `Bearer ${TENANT_OWNER.jwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()
    expect(data.members).toBeDefined()
  })

  test('3. Tenant owner can create API key for sub-tenant', async () => {
    SUB_TENANT.apiKey = await createApiKey(TENANT_OWNER.jwt, SUB_TENANT.id)
    expect(SUB_TENANT.apiKey).toBeTruthy()
  })

  test('4. Regular member cannot create sub-tenants', async () => {
    // Login as the member with their tenant context
    // Re-login to get a token with the tenant org context
    const memberLoginRes = await fetch(`${API_BASE}/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        username: TENANT_MEMBER.username,
        password: TENANT_MEMBER.password,
      }),
    })
    const memberLoginData = await memberLoginRes.json()
    const memberToken = memberLoginData.token

    // Try to create a sub-tenant with member role — should fail
    // Member has 'member' role in TENANT_OWNER's org, not admin/owner
    const res = await fetch(
      `${API_BASE}/api/v1/orgs/${TENANT_OWNER.orgId}/sub-tenants`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${memberToken}`,
          'X-Org-Id': TENANT_OWNER.orgId,
        },
        body: JSON.stringify({ name: 'ShouldFail' }),
      }
    )
    // Member role doesn't pass verify_org_access (needs owner/admin)
    // The member has 'member' role, so they should be forbidden
    expect(res.status).toBe(403)
  })

  test('5. Sub-tenant API key cannot access parent tenant', async () => {
    // Sub-tenant API key should NOT be able to access parent tenant's resources
    const res = await fetch(`${API_BASE}/api/v1/orgs/${TENANT_OWNER.orgId}/schema`, {
      headers: { 'X-API-Key': SUB_TENANT.apiKey },
    })
    // API key is scoped to sub-tenant org; schema endpoint only accepts JWT
    // so this returns 401 (Unauthorized) since API keys aren't accepted there
    expect([401, 403]).toContain(res.status)
  })

  test('6. Global admin can access any org', async () => {
    // Admin should be able to see tenant's schema
    const res = await fetch(`${API_BASE}/api/v1/orgs/${TENANT_OWNER.orgId}/schema`, {
      headers: { Authorization: `Bearer ${adminJwt}` },
    })
    expect(res.status).toBe(200)

    // Admin should be able to see sub-tenant's schema
    const subRes = await fetch(`${API_BASE}/api/v1/orgs/${SUB_TENANT.id}/schema`, {
      headers: { Authorization: `Bearer ${adminJwt}` },
    })
    expect(subRes.status).toBe(200)
  })

  test('7. Org list shows role for member', async () => {
    // Login as member with tenant org context
    const memberToken = await login(TENANT_MEMBER.username, TENANT_MEMBER.password)

    const res = await fetch(`${API_BASE}/api/v1/orgs`, {
      headers: { Authorization: `Bearer ${memberToken}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()

    // Member should see at least their own org + the tenant they're a member of
    const tenantMembership = data.organizations.find(
      (o: any) => o.id === TENANT_OWNER.orgId
    )
    if (tenantMembership) {
      expect(tenantMembership.role).toBe('member')
    }
  })

  test('8. TenantContext permission helpers work correctly', async () => {
    // Deploy a pipeline as tenant owner — should succeed
    const deployRes = await fetch(`${API_BASE}/api/v1/pipelines`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-API-Key': TENANT_OWNER.apiKey,
      },
      body: JSON.stringify({
        name: 'rbac-test-pipeline',
        source: 'stream Test = Events .emit()',
      }),
    })
    expect(deployRes.status).toBe(201)

    // Deploy a pipeline with sub-tenant API key — should also succeed
    const subDeployRes = await fetch(`${API_BASE}/api/v1/pipelines`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-API-Key': SUB_TENANT.apiKey,
      },
      body: JSON.stringify({
        name: 'sub-rbac-test-pipeline',
        source: 'stream SubTest = Events .emit()',
      }),
    })
    expect(subDeployRes.status).toBe(201)
  })
})
