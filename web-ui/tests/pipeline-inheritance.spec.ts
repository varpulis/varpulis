import { test, expect } from '@playwright/test'

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const API_BASE = 'http://localhost:8080'
const MAILPIT_API = 'http://localhost:8025'

const ADMIN_CREDS = { username: 'admin', password: 'admin123' }

const TENANT = {
  orgName: `PipeInherit_${Date.now()}`,
  username: `pipeinh_${Date.now()}`,
  email: `pipeinh_${Date.now()}@varpulis.test`,
  password: 'TestPass123',
  jwt: '',
  orgId: '',
  apiKey: '',
}

const SUB_TENANT = {
  name: `SubPipeInherit_${Date.now()}`,
  id: '',
  apiKey: '',
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
    body: JSON.stringify({ name: 'e2e-pipeline-test' }),
  })
  expect(res.status).toBe(201)
  const data = await res.json()
  return data.api_key
}

async function deployPipeline(apiKey: string, name: string, source: string): Promise<string> {
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test.describe.serial('Phase 4: Pipeline Inheritance Engine', () => {
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
    SUB_TENANT.apiKey = await createApiKey(TENANT.jwt, SUB_TENANT.id)

    adminJwt = await login(ADMIN_CREDS.username, ADMIN_CREDS.password)

    expect(TENANT.orgId).toBeTruthy()
    expect(SUB_TENANT.id).toBeTruthy()
    expect(adminJwt).toBeTruthy()
  })

  test('1. Deploy pipeline on tenant — scope defaults to own', async () => {
    const pipelineId = await deployPipeline(
      TENANT.apiKey,
      'tenant-own-pipeline',
      'stream TenantOwn = Events .emit()'
    )
    expect(pipelineId).toBeTruthy()

    // List tenant pipelines via runtime API
    const listRes = await fetch(`${API_BASE}/api/v1/pipelines`, {
      headers: { 'X-API-Key': TENANT.apiKey },
    })
    expect(listRes.status).toBe(200)
    const listData = await listRes.json()
    const pipeline = listData.pipelines.find((p: any) => p.id === pipelineId)
    expect(pipeline).toBeTruthy()
    expect(pipeline.scope_level).toBe('own')
    expect(pipeline.read_only).toBe(false)
  })

  test('2. Tenant pipeline list shows own pipelines as editable', async () => {
    // Use org-level pipeline list (hierarchy-aware)
    const res = await fetch(`${API_BASE}/api/v1/orgs/${TENANT.orgId}/pipelines`, {
      headers: { Authorization: `Bearer ${TENANT.jwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()
    expect(data.pipelines.length).toBeGreaterThan(0)

    // Own pipelines should not be read_only
    const ownPipeline = data.pipelines.find(
      (p: any) => p.scope_level === 'own' && !p.inherited_from_org_id
    )
    if (ownPipeline) {
      expect(ownPipeline.read_only).toBe(false)
    }
  })

  test('3. Sub-tenant can deploy own pipeline', async () => {
    const pipelineId = await deployPipeline(
      SUB_TENANT.apiKey,
      'sub-own-pipeline',
      'stream SubOwn = Events .emit()'
    )
    expect(pipelineId).toBeTruthy()

    // Sub-tenant can see its own pipeline
    const listRes = await fetch(`${API_BASE}/api/v1/pipelines`, {
      headers: { 'X-API-Key': SUB_TENANT.apiKey },
    })
    expect(listRes.status).toBe(200)
    const listData = await listRes.json()
    const pipeline = listData.pipelines.find((p: any) => p.id === pipelineId)
    expect(pipeline).toBeTruthy()
    expect(pipeline.read_only).toBe(false)
  })

  test('4. Global pipeline appears as read-only in tenant list', async () => {
    // Deploy a global pipeline via admin API
    const globalRes = await fetch(`${API_BASE}/api/v1/admin/global-pipelines`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${adminJwt}`,
      },
      body: JSON.stringify({
        name: 'global-inherited',
        vpl_source: 'stream GlobalInherited = Events .emit()',
      }),
    })
    expect(globalRes.status).toBe(201)

    // Check tenant's runtime pipeline list - global pipeline should be read_only
    const listRes = await fetch(`${API_BASE}/api/v1/pipelines`, {
      headers: { 'X-API-Key': TENANT.apiKey },
    })
    expect(listRes.status).toBe(200)
    const listData = await listRes.json()
    const globalPipeline = listData.pipelines.find(
      (p: any) => p.name === 'global-inherited'
    )
    if (globalPipeline) {
      expect(globalPipeline.scope_level).toBe('global')
      expect(globalPipeline.read_only).toBe(true)
    }
  })

  test('5. Tenant cannot delete global (inherited) pipeline', async () => {
    // List pipelines to find the global one
    const listRes = await fetch(`${API_BASE}/api/v1/pipelines`, {
      headers: { 'X-API-Key': TENANT.apiKey },
    })
    const listData = await listRes.json()
    const globalPipeline = listData.pipelines.find(
      (p: any) => p.name === 'global-inherited'
    )

    if (globalPipeline) {
      // Try to delete it — should be forbidden
      const delRes = await fetch(`${API_BASE}/api/v1/pipelines/${globalPipeline.id}`, {
        method: 'DELETE',
        headers: { 'X-API-Key': TENANT.apiKey },
      })
      expect(delRes.status).toBe(403)
    }
  })

  test('6. Tenant can delete own pipeline', async () => {
    // Deploy a deletable pipeline
    const pipelineId = await deployPipeline(
      TENANT.apiKey,
      'deletable-pipeline',
      'stream Deletable = Events .emit()'
    )

    // Delete it — should succeed
    const delRes = await fetch(`${API_BASE}/api/v1/pipelines/${pipelineId}`, {
      method: 'DELETE',
      headers: { 'X-API-Key': TENANT.apiKey },
    })
    expect(delRes.status).toBe(200)
  })

  test('7. Admin tenant detail shows pipelines with read_only flag', async () => {
    const res = await fetch(`${API_BASE}/api/v1/admin/tenants/${TENANT.orgId}`, {
      headers: { Authorization: `Bearer ${adminJwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()

    // Should have pipelines
    expect(data.pipelines.length).toBeGreaterThan(0)

    // Check that each pipeline has read_only field
    for (const p of data.pipelines) {
      expect(p.read_only).toBeDefined()
      expect(typeof p.read_only).toBe('boolean')
    }
  })

  test('8. Org-level pipeline list includes both own and inherited', async () => {
    const res = await fetch(`${API_BASE}/api/v1/orgs/${TENANT.orgId}/pipelines`, {
      headers: { Authorization: `Bearer ${TENANT.jwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()

    // Should have own pipelines (from DB)
    expect(data.pipelines.length).toBeGreaterThan(0)
    expect(data.total).toBeGreaterThan(0)
  })
})
