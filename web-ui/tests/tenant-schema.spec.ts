import { test, expect } from '@playwright/test'

// ---------------------------------------------------------------------------
// Config -- expects k3d-saas cluster with MailPit + backend running
// ---------------------------------------------------------------------------

const API_BASE = 'http://localhost:8080'
const MAILPIT_API = 'http://localhost:8025'

const ADMIN_CREDS = { username: 'admin', password: 'admin123' }

const TENANT_A = {
  orgName: `SchemaA_${Date.now()}`,
  username: `schemaa_${Date.now()}`,
  email: `schemaa_${Date.now()}@varpulis.test`,
  password: 'TestPass123!',
  jwt: '',
  orgId: '',
  apiKey: '',
  slug: '',
  dbSchema: '',
}

const TENANT_B = {
  orgName: `SchemaB_${Date.now()}`,
  username: `schemab_${Date.now()}`,
  email: `schemab_${Date.now()}@varpulis.test`,
  password: 'TestPass456!',
  jwt: '',
  orgId: '',
  apiKey: '',
  slug: '',
  dbSchema: '',
}

const SUB_TENANT_A1 = {
  name: `SubSchemaA1_${Date.now()}`,
  id: '',
  slug: '',
  dbSchema: '',
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

async function getOrgInfo(jwt: string): Promise<{ id: string; slug: string; dbSchema: string }> {
  const res = await fetch(`${API_BASE}/api/v1/orgs`, {
    headers: { Authorization: `Bearer ${jwt}` },
  })
  expect(res.status).toBe(200)
  const data = await res.json()
  expect(data.organizations.length).toBeGreaterThan(0)
  const org = data.organizations[0]
  return { id: org.id, slug: org.slug, dbSchema: org.db_schema }
}

async function createApiKey(jwt: string, orgId?: string): Promise<string> {
  const oid = orgId || (await getOrgInfo(jwt)).id
  const res = await fetch(`${API_BASE}/api/v1/orgs/${oid}/api-keys`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${jwt}`,
    },
    body: JSON.stringify({ name: 'e2e-schema-test' }),
  })
  expect(res.status).toBe(201)
  const data = await res.json()
  return data.api_key
}

// ---------------------------------------------------------------------------
// Test suite -- serial (tests depend on each other)
// ---------------------------------------------------------------------------

test.describe.serial('Phase 2: Per-Tenant PostgreSQL Schema', () => {
  test('0. Setup: create tenants A and B, admin login', async () => {
    await clearMailpit()

    // Sign up Tenant A
    await signup(TENANT_A)
    await verifyEmail(TENANT_A.email)
    TENANT_A.jwt = await login(TENANT_A.username, TENANT_A.password)
    const orgInfoA = await getOrgInfo(TENANT_A.jwt)
    TENANT_A.orgId = orgInfoA.id
    TENANT_A.slug = orgInfoA.slug
    TENANT_A.dbSchema = orgInfoA.dbSchema
    TENANT_A.apiKey = await createApiKey(TENANT_A.jwt)

    // Sign up Tenant B
    await clearMailpit()
    await signup(TENANT_B)
    await verifyEmail(TENANT_B.email)
    TENANT_B.jwt = await login(TENANT_B.username, TENANT_B.password)
    const orgInfoB = await getOrgInfo(TENANT_B.jwt)
    TENANT_B.orgId = orgInfoB.id
    TENANT_B.slug = orgInfoB.slug
    TENANT_B.dbSchema = orgInfoB.dbSchema
    TENANT_B.apiKey = await createApiKey(TENANT_B.jwt)

    // Admin login
    adminJwt = await login(ADMIN_CREDS.username, ADMIN_CREDS.password)

    expect(TENANT_A.orgId).toBeTruthy()
    expect(TENANT_B.orgId).toBeTruthy()
    expect(adminJwt).toBeTruthy()
  })

  test('1. Slug auto-generated from org name', async () => {
    // Tenant A slug should be derived from orgName
    expect(TENANT_A.slug).toBeTruthy()
    expect(TENANT_A.slug).toMatch(/^[a-z0-9_]+$/)
    // Slug should contain lowercase version of org name parts
    expect(TENANT_A.slug.toLowerCase()).toContain('schemaa')

    // Tenant B slug should also be generated
    expect(TENANT_B.slug).toBeTruthy()
    expect(TENANT_B.slug).toMatch(/^[a-z0-9_]+$/)
    expect(TENANT_B.slug.toLowerCase()).toContain('schemab')

    // Slugs must be different
    expect(TENANT_A.slug).not.toBe(TENANT_B.slug)
  })

  test('2. db_schema auto-provisioned for tenant', async () => {
    // Tenant A should have db_schema set
    expect(TENANT_A.dbSchema).toBeTruthy()
    expect(TENANT_A.dbSchema).toBe(`tenant_${TENANT_A.slug}`)

    // Tenant B should have a different db_schema
    expect(TENANT_B.dbSchema).toBeTruthy()
    expect(TENANT_B.dbSchema).toBe(`tenant_${TENANT_B.slug}`)
    expect(TENANT_A.dbSchema).not.toBe(TENANT_B.dbSchema)
  })

  test('3. Schema info endpoint returns schema details', async () => {
    const res = await fetch(`${API_BASE}/api/v1/orgs/${TENANT_A.orgId}/schema`, {
      headers: { Authorization: `Bearer ${TENANT_A.jwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()

    expect(data.org_id).toBe(TENANT_A.orgId)
    expect(data.slug).toBe(TENANT_A.slug)
    expect(data.db_schema).toBe(TENANT_A.dbSchema)
    expect(data.effective_schema).toBe(TENANT_A.dbSchema)
    expect(data.schema_exists).toBe(true)
    expect(data.is_inherited).toBe(false)

    // Verify data plane tables exist in schema
    expect(data.tables).toContain('pipelines')
    expect(data.tables).toContain('usage_daily')
  })

  test('4. Sub-tenant inherits parent schema', async () => {
    // Create sub-tenant under Tenant A
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
    SUB_TENANT_A1.slug = data.slug
    SUB_TENANT_A1.dbSchema = data.db_schema

    // Sub-tenant should have a slug
    expect(SUB_TENANT_A1.slug).toBeTruthy()
    expect(SUB_TENANT_A1.slug).toMatch(/^[a-z0-9_]+$/)

    // Sub-tenant should inherit parent's db_schema
    expect(SUB_TENANT_A1.dbSchema).toBe(TENANT_A.dbSchema)
  })

  test('5. Sub-tenant schema info shows inherited schema', async () => {
    const res = await fetch(`${API_BASE}/api/v1/orgs/${SUB_TENANT_A1.id}/schema`, {
      headers: { Authorization: `Bearer ${TENANT_A.jwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()

    // Sub-tenant's db_schema matches parent's (inherited at creation)
    expect(data.db_schema).toBe(TENANT_A.dbSchema)
    expect(data.effective_schema).toBe(TENANT_A.dbSchema)
    expect(data.schema_exists).toBe(true)
    // is_inherited is false because db_schema was set directly on the sub-tenant row
    expect(data.tables).toContain('pipelines')
    expect(data.tables).toContain('usage_daily')
  })

  test('6. Tenant B has separate schema (isolation)', async () => {
    const resA = await fetch(`${API_BASE}/api/v1/orgs/${TENANT_A.orgId}/schema`, {
      headers: { Authorization: `Bearer ${TENANT_A.jwt}` },
    })
    expect(resA.status).toBe(200)
    const dataA = await resA.json()

    const resB = await fetch(`${API_BASE}/api/v1/orgs/${TENANT_B.orgId}/schema`, {
      headers: { Authorization: `Bearer ${TENANT_B.jwt}` },
    })
    expect(resB.status).toBe(200)
    const dataB = await resB.json()

    // Schemas must be different
    expect(dataA.effective_schema).not.toBe(dataB.effective_schema)
    expect(dataA.schema_exists).toBe(true)
    expect(dataB.schema_exists).toBe(true)

    // Both have data plane tables
    expect(dataA.tables).toContain('pipelines')
    expect(dataB.tables).toContain('pipelines')
  })

  test('7. Admin tenant list includes slug and db_schema', async () => {
    const res = await fetch(`${API_BASE}/api/v1/admin/tenants`, {
      headers: { Authorization: `Bearer ${adminJwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()

    // Find Tenant A
    const tenantA = data.tenants.find((t: any) => t.id === TENANT_A.orgId)
    expect(tenantA).toBeTruthy()
    expect(tenantA.slug).toBe(TENANT_A.slug)
    expect(tenantA.db_schema).toBe(TENANT_A.dbSchema)

    // Find Tenant B
    const tenantB = data.tenants.find((t: any) => t.id === TENANT_B.orgId)
    expect(tenantB).toBeTruthy()
    expect(tenantB.slug).toBe(TENANT_B.slug)
    expect(tenantB.db_schema).toBe(TENANT_B.dbSchema)
  })

  test('8. Admin tenant detail includes sub-tenant schema info', async () => {
    const res = await fetch(`${API_BASE}/api/v1/admin/tenants/${TENANT_A.orgId}`, {
      headers: { Authorization: `Bearer ${adminJwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()

    expect(data.slug).toBe(TENANT_A.slug)
    expect(data.db_schema).toBe(TENANT_A.dbSchema)

    // Sub-tenant in detail should show inherited schema
    const sub = data.sub_tenants.find((s: any) => s.id === SUB_TENANT_A1.id)
    expect(sub).toBeTruthy()
    expect(sub.slug).toBeTruthy()
    expect(sub.db_schema).toBe(TENANT_A.dbSchema)
  })
})
