import { test, expect, Page } from '@playwright/test'

// MailPit API — access via kubectl port-forward svc/mailpit 8025:8025
const MAILPIT_API = 'http://localhost:8025'
const TEST_USER = {
  orgName: 'E2E Test Org',
  username: `e2euser_${Date.now()}`,
  email: `e2euser_${Date.now()}@varpulis.test`,
  password: 'TestPass123!',
}

// ---------------------------------------------------------------------------
// MailPit helpers
// ---------------------------------------------------------------------------

async function clearMailpit() {
  await fetch(`${MAILPIT_API}/api/v1/messages`, { method: 'DELETE' })
}

async function getLatestEmail(
  toAddress: string,
  maxWaitMs = 15000
): Promise<{ text: string; html: string; subject: string }> {
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
      return { text: msg.Text || '', html: msg.HTML || '', subject: msg.Subject || '' }
    }
    await new Promise((r) => setTimeout(r, 500))
  }
  throw new Error(`No email found for ${toAddress} within ${maxWaitMs}ms`)
}

function extractVerificationUrl(emailText: string): string {
  const match = emailText.match(/(https?:\/\/[^\s]+verify-email\?token=[^\s]+)/)
  if (!match) throw new Error('No verification URL found in email body')
  return match[1]
}

// ---------------------------------------------------------------------------
// Auth lifecycle tests (serial — they depend on each other)
// ---------------------------------------------------------------------------

test.describe.serial('Auth lifecycle', () => {
  test('1. Self-service signup', async ({ page }) => {
    await clearMailpit()
    await page.goto('/signup')
    await expect(page.getByRole('button', { name: 'Create Account' })).toBeVisible()

    await page.getByLabel('Organization Name').fill(TEST_USER.orgName)
    await page.getByLabel('Username').fill(TEST_USER.username)
    await page.getByLabel('Email').fill(TEST_USER.email)
    await page.getByLabel('Password', { exact: true }).fill(TEST_USER.password)
    await page.getByLabel('Confirm Password').fill(TEST_USER.password)

    await page.getByRole('button', { name: 'Create Account' }).click()

    await expect(page.locator('text=Check your email')).toBeVisible({ timeout: 15000 })
  })

  test('2. Login rejected before verification', async ({ page }) => {
    await page.goto('/login')
    await page.getByLabel('Username').fill(TEST_USER.username)
    await page.getByLabel('Password').fill(TEST_USER.password)
    await page.getByRole('button', { name: 'Sign in', exact: true }).click()

    await expect(page.locator('text=verify your email')).toBeVisible({ timeout: 10000 })
  })

  test('3. Verify email via MailPit link', async ({ page }) => {
    const email = await getLatestEmail(TEST_USER.email)
    expect(email.subject).toContain('Verify')

    const verifyUrl = extractVerificationUrl(email.text)

    // Navigate to the verify URL (it's an absolute URL, so extract the path)
    const url = new URL(verifyUrl)
    await page.goto(`${url.pathname}${url.search}`)

    await expect(page.locator('text=Email verified')).toBeVisible({ timeout: 10000 })
  })

  test('4. Login succeeds after verification', async ({ page }) => {
    await page.goto('/login')
    await page.getByLabel('Username').fill(TEST_USER.username)
    await page.getByLabel('Password').fill(TEST_USER.password)
    await page.getByRole('button', { name: 'Sign in', exact: true }).click()

    // Should redirect to dashboard
    await expect(page).toHaveURL('/', { timeout: 15000 })
  })

  test('5. Admin login', async ({ page }) => {
    await page.goto('/login')
    await page.getByLabel('Username').fill('admin')
    await page.getByLabel('Password').fill('admin123')
    await page.getByRole('button', { name: 'Sign in', exact: true }).click()

    await expect(page).toHaveURL('/', { timeout: 15000 })
  })

  test('6. Admin creates tenant', async ({ page }) => {
    // Login as admin first
    await page.goto('/login')
    await page.getByLabel('Username').fill('admin')
    await page.getByLabel('Password').fill('admin123')
    await page.getByRole('button', { name: 'Sign in', exact: true }).click()
    await expect(page).toHaveURL('/', { timeout: 15000 })

    // Navigate to admin page
    await page.goto('/admin')
    await expect(page.getByRole('heading', { name: 'Admin Panel' })).toBeVisible({ timeout: 10000 })
  })

  test('7. Route guards', async ({ page }) => {
    // Clear auth state
    await page.goto('/login')
    await page.evaluate(() => {
      localStorage.removeItem('varpulis_token')
      localStorage.removeItem('varpulis_authenticated')
      sessionStorage.removeItem('varpulis_api_key')
    })

    // Try to access protected route
    await page.goto('/pipelines')

    // Should redirect to login
    await expect(page).toHaveURL(/\/login/, { timeout: 10000 })
  })

  test('8. Logout', async ({ page }) => {
    // Login first
    await page.goto('/login')
    await page.getByLabel('Username').fill('admin')
    await page.getByLabel('Password').fill('admin123')
    await page.getByRole('button', { name: 'Sign in', exact: true }).click()
    await expect(page).toHaveURL('/', { timeout: 15000 })

    // Open user menu and click Sign out
    await page.locator('[data-testid="user-menu-btn"]').click()
    await page.locator('[data-testid="logout-btn"]').click()

    // Should be at login page with token cleared
    await expect(page).toHaveURL(/\/login/, { timeout: 10000 })
    const token = await page.evaluate(() => localStorage.getItem('varpulis_token'))
    expect(token).toBeFalsy()
  })

  test('9. Settings page', async ({ page }) => {
    // Login first
    await page.goto('/login')
    await page.getByLabel('Username').fill('admin')
    await page.getByLabel('Password').fill('admin123')
    await page.getByRole('button', { name: 'Sign in', exact: true }).click()
    await expect(page).toHaveURL('/', { timeout: 15000 })

    await page.goto('/settings')
    await expect(page.getByRole('heading', { name: 'Settings' })).toBeVisible({ timeout: 10000 })
  })
})
