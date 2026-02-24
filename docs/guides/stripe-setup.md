# Stripe Billing Setup

This guide walks through configuring Stripe for the Varpulis SaaS deployment.

## Prerequisites

- A [Stripe account](https://dashboard.stripe.com/register) (test mode is fine for development)
- [Stripe CLI](https://stripe.com/docs/stripe-cli) installed and authenticated (`stripe login`)

## Quick Setup (Script)

```bash
# Test mode — creates product, price, and webhook in Stripe
./scripts/setup-stripe.sh

# With a custom server URL
./scripts/setup-stripe.sh --server-url=https://demo.varpulis-cep.com
```

The script outputs env vars to add to your `.env` file.

## Manual Setup

### 1. Create the Pro Product

In the [Stripe Dashboard](https://dashboard.stripe.com/products):

1. Click **Add product**
2. Name: `Varpulis Pro`
3. Description: `Varpulis CEP Pro tier: 10M events/month, priority support`
4. Pricing: **Recurring**, $49.00/month
5. Save and copy the **Price ID** (starts with `price_`)

### 2. Get Your API Keys

From [Developers > API keys](https://dashboard.stripe.com/apikeys):

- **Secret key**: starts with `sk_test_` (test) or `sk_live_` (production)
- Never commit secret keys to git

### 3. Create a Webhook Endpoint

From [Developers > Webhooks](https://dashboard.stripe.com/webhooks):

1. Click **Add endpoint**
2. URL: `https://your-domain.com/api/v1/billing/webhook`
3. Select these events:
   - `checkout.session.completed`
   - `customer.subscription.deleted`
   - `customer.subscription.updated`
   - `invoice.payment_failed`
4. Save and copy the **Signing secret** (starts with `whsec_`)

### 4. Configure Environment Variables

Add to your `.env` file (see `deploy/docker/.env.example`):

```bash
STRIPE_SECRET_KEY=sk_test_...
STRIPE_WEBHOOK_SECRET=whsec_...
STRIPE_PRO_PRICE_ID=price_...
```

## Local Development

For local development, use the Stripe CLI to forward webhook events:

```bash
# Forward webhooks to your local server
stripe listen --forward-to http://localhost:9000/api/v1/billing/webhook

# The CLI prints a webhook signing secret — use that for STRIPE_WEBHOOK_SECRET
```

In a separate terminal, trigger a test event:

```bash
stripe trigger checkout.session.completed
```

## Billing Flow

1. User clicks **Upgrade to Pro** in the web UI
2. Frontend calls `POST /api/v1/billing/checkout`
3. Backend creates a Stripe Checkout Session and returns the URL
4. User completes payment on Stripe's hosted page
5. Stripe sends `checkout.session.completed` webhook
6. Backend upgrades the org tier from `free` to `pro`
7. User's event limit increases from 10K to 10M events/month

## Tier Limits

| Tier | Events/Month | Price |
|------|-------------|-------|
| Free | 10,000 | $0 |
| Pro | 10,000,000 | $49/mo |
| Enterprise | Unlimited | Contact us |

When an org exceeds their limit, the API returns `429 Too Many Requests` with upgrade instructions.

## Customer Portal

Pro users can manage their subscription via the Stripe Customer Portal:

```bash
# Frontend calls POST /api/v1/billing/portal
# Returns a URL to Stripe's self-service portal
```

From the portal, users can:
- Update payment methods
- View invoice history
- Cancel their subscription

## Subscription Lifecycle

| Stripe Event | Action |
|---|---|
| `checkout.session.completed` | Upgrade org to Pro, save Stripe customer ID |
| `customer.subscription.deleted` | Downgrade org to Free |
| `customer.subscription.updated` | Log (future: handle plan changes) |
| `invoice.payment_failed` | Log warning (future: grace period) |

All webhook events are recorded in the audit log (`data/audit.jsonl`).
