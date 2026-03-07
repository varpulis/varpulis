import { test, expect } from '@playwright/test'

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const API_BASE = 'http://localhost:8080'
const MAILPIT_API = 'http://localhost:8025'

const ADMIN_CREDS = { username: 'admin', password: 'admin123' }

const TENANT = {
  orgName: `KafkaTest_${Date.now()}`,
  username: `kafkatest_${Date.now()}`,
  email: `kafkatest_${Date.now()}@varpulis.test`,
  password: 'TestPass123',
  jwt: '',
  orgId: '',
}

const SUB_TENANT = {
  name: `SubKafka_${Date.now()}`,
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

test.describe.serial('Phase 7: Kafka Topic Isolation', () => {
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

  test('1. Kafka prefix not configured initially', async () => {
    const res = await fetch(`${API_BASE}/api/v1/admin/tenants/${TENANT.orgId}/kafka`, {
      headers: { Authorization: `Bearer ${adminJwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()
    expect(data.kafka_topic_prefix).toBeNull()
    expect(data.effective_prefix).toBeNull()
    expect(data.org_id).toBe(TENANT.orgId)
  })

  test('2. Configure Kafka topic prefix for tenant', async () => {
    const res = await fetch(`${API_BASE}/api/v1/admin/tenants/${TENANT.orgId}/kafka`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${adminJwt}`,
      },
      body: JSON.stringify({ prefix: 'kafkatest-tenant' }),
    })
    expect(res.status).toBe(201)
    const data = await res.json()
    expect(data.kafka_topic_prefix).toBe('kafkatest-tenant')
    expect(data.topic_pattern).toBe('kafkatest-tenant.*')
    expect(data.status).toBe('configured')
  })

  test('3. Kafka prefix info returns configured prefix', async () => {
    const res = await fetch(`${API_BASE}/api/v1/admin/tenants/${TENANT.orgId}/kafka`, {
      headers: { Authorization: `Bearer ${adminJwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()
    expect(data.kafka_topic_prefix).toBe('kafkatest-tenant')
    expect(data.effective_prefix).toBe('kafkatest-tenant')
    expect(data.org_id).toBe(TENANT.orgId)
  })

  test('4. Duplicate configure returns 409', async () => {
    const res = await fetch(`${API_BASE}/api/v1/admin/tenants/${TENANT.orgId}/kafka`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${adminJwt}`,
      },
      body: JSON.stringify({ prefix: 'another-prefix' }),
    })
    expect(res.status).toBe(409)
  })

  test('5. Sub-tenant cannot configure own Kafka prefix', async () => {
    const res = await fetch(`${API_BASE}/api/v1/admin/tenants/${SUB_TENANT.id}/kafka`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${adminJwt}`,
      },
      body: JSON.stringify({ prefix: 'sub-prefix' }),
    })
    expect(res.status).toBe(400)
    const data = await res.json()
    expect(data.error).toContain('Sub-tenants share parent')
  })

  test('6. Sub-tenant inherits parent Kafka prefix', async () => {
    const res = await fetch(`${API_BASE}/api/v1/admin/tenants/${SUB_TENANT.id}/kafka`, {
      headers: { Authorization: `Bearer ${adminJwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()
    expect(data.kafka_topic_prefix).toBeNull()
    expect(data.effective_prefix).toBe('kafkatest-tenant')
  })

  test('7. Admin tenant list includes kafka_topic_prefix', async () => {
    const res = await fetch(`${API_BASE}/api/v1/admin/tenants`, {
      headers: { Authorization: `Bearer ${adminJwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()

    const tenant = data.tenants.find((t: any) => t.id === TENANT.orgId)
    expect(tenant).toBeTruthy()
    expect(tenant.kafka_topic_prefix).toBe('kafkatest-tenant')
  })

  test('8. Admin tenant detail includes kafka_topic_prefix', async () => {
    const res = await fetch(`${API_BASE}/api/v1/admin/tenants/${TENANT.orgId}`, {
      headers: { Authorization: `Bearer ${adminJwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()
    expect(data.kafka_topic_prefix).toBe('kafkatest-tenant')
  })

  test('9. Remove Kafka topic prefix', async () => {
    const res = await fetch(`${API_BASE}/api/v1/admin/tenants/${TENANT.orgId}/kafka`, {
      method: 'DELETE',
      headers: { Authorization: `Bearer ${adminJwt}` },
    })
    expect(res.status).toBe(200)
    const data = await res.json()
    expect(data.deleted).toBe(true)

    // Verify it's gone
    const checkRes = await fetch(`${API_BASE}/api/v1/admin/tenants/${TENANT.orgId}/kafka`, {
      headers: { Authorization: `Bearer ${adminJwt}` },
    })
    const checkData = await checkRes.json()
    expect(checkData.kafka_topic_prefix).toBeNull()
    expect(checkData.effective_prefix).toBeNull()
  })

  test('10. Auto-generate prefix from slug', async () => {
    // Configure without explicit prefix — should auto-generate from org slug/name
    const res = await fetch(`${API_BASE}/api/v1/admin/tenants/${TENANT.orgId}/kafka`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${adminJwt}`,
      },
      body: JSON.stringify({}),
    })
    expect(res.status).toBe(201)
    const data = await res.json()
    // Should be auto-generated from org name (lowercased, special chars replaced)
    expect(data.kafka_topic_prefix).toBeTruthy()
    expect(data.kafka_topic_prefix.length).toBeGreaterThan(0)
    expect(data.status).toBe('configured')
  })
})
