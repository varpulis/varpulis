#!/usr/bin/env bash
# setup-stripe.sh — Configure Stripe products, prices, and webhook for Varpulis SaaS.
#
# Prerequisites:
#   - Stripe CLI installed: https://stripe.com/docs/stripe-cli
#   - Logged in: stripe login
#
# Usage:
#   ./scripts/setup-stripe.sh              # Test mode (default)
#   ./scripts/setup-stripe.sh --live       # Live mode (production)
#
# Outputs:
#   - Creates the Pro product and price in Stripe
#   - Creates a webhook endpoint pointing to your server
#   - Prints env vars to add to .env

set -euo pipefail

LIVE_MODE=false
SERVER_URL="${SERVER_URL:-http://localhost:9000}"

for arg in "$@"; do
    case "$arg" in
        --live) LIVE_MODE=true ;;
        --server-url=*) SERVER_URL="${arg#*=}" ;;
        --help|-h)
            echo "Usage: $0 [--live] [--server-url=URL]"
            echo ""
            echo "  --live           Use live mode (default: test mode)"
            echo "  --server-url=URL Server URL for webhook (default: \$SERVER_URL or http://localhost:9000)"
            exit 0
            ;;
    esac
done

# Verify Stripe CLI is installed
if ! command -v stripe &>/dev/null; then
    echo "Error: Stripe CLI not found. Install from https://stripe.com/docs/stripe-cli"
    exit 1
fi

echo "=== Varpulis Stripe Setup ==="
if $LIVE_MODE; then
    echo "Mode: LIVE (real charges!)"
    STRIPE_FLAGS=""
else
    echo "Mode: TEST"
    STRIPE_FLAGS=""
fi
echo "Server URL: $SERVER_URL"
echo ""

# 1. Create the Pro product
echo "Creating Pro product..."
PRODUCT_JSON=$(stripe products create \
    --name="Varpulis Pro" \
    --description="Varpulis CEP Pro tier: 10M events/month, priority support" \
    --metadata[tier]=pro \
    $STRIPE_FLAGS 2>&1)

PRODUCT_ID=$(echo "$PRODUCT_JSON" | grep '"id":' | head -1 | sed 's/.*"id": "\(.*\)".*/\1/')
echo "  Product ID: $PRODUCT_ID"

# 2. Create the Pro monthly price ($49/mo)
echo "Creating Pro price ($49/mo)..."
PRICE_JSON=$(stripe prices create \
    --product="$PRODUCT_ID" \
    --unit-amount=4900 \
    --currency=usd \
    --recurring[interval]=month \
    $STRIPE_FLAGS 2>&1)

PRICE_ID=$(echo "$PRICE_JSON" | grep '"id":' | head -1 | sed 's/.*"id": "\(.*\)".*/\1/')
echo "  Price ID: $PRICE_ID"

# 3. Create webhook endpoint
echo "Creating webhook endpoint..."
WEBHOOK_JSON=$(stripe webhook_endpoints create \
    --url="${SERVER_URL}/api/v1/billing/webhook" \
    --enabled-events="checkout.session.completed,customer.subscription.deleted,customer.subscription.updated,invoice.payment_failed" \
    $STRIPE_FLAGS 2>&1)

WEBHOOK_SECRET=$(echo "$WEBHOOK_JSON" | grep '"secret":' | head -1 | sed 's/.*"secret": "\(.*\)".*/\1/')
echo "  Webhook secret: ${WEBHOOK_SECRET:0:12}..."

echo ""
echo "=== Done! Add these to your .env file ==="
echo ""
echo "STRIPE_SECRET_KEY=<your-stripe-secret-key>"
echo "STRIPE_WEBHOOK_SECRET=$WEBHOOK_SECRET"
echo "STRIPE_PRO_PRICE_ID=$PRICE_ID"
echo ""
echo "For local development with Stripe CLI webhook forwarding:"
echo "  stripe listen --forward-to ${SERVER_URL}/api/v1/billing/webhook"
