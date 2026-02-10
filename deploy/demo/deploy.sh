#!/bin/bash
# Varpulis Demo - Deployment Script (non-sudo)
set -e

DEMO_DIR="$HOME/varpulis-demo"
REPO_URL="git@github.com:varpulis/varpulis.git"

echo "=== Varpulis Demo Deployment ==="

# Clone or update repo
if [ -d "$DEMO_DIR/repo" ]; then
    echo "Updating existing repo..."
    cd "$DEMO_DIR/repo" && git pull
else
    echo "Cloning repo..."
    mkdir -p "$DEMO_DIR"
    git clone "$REPO_URL" "$DEMO_DIR/repo"
fi

# Verify TLS certs
if [ ! -f "/etc/caddy/certs/origin.pem" ]; then
    echo "ERROR: Place origin.pem and origin-key.pem in /etc/caddy/certs/"
    exit 1
fi

# Generate .env if missing
ENV_FILE="$DEMO_DIR/repo/deploy/demo/.env"
if [ ! -f "$ENV_FILE" ]; then
    API_KEY=$(openssl rand -hex 16)
    GRAFANA_PASS=$(openssl rand -hex 12)
    cat > "$ENV_FILE" <<EOF
VARPULIS_API_KEY=$API_KEY
VARPULIS_WORKER_KEY=$API_KEY
RUST_LOG=info
GRAFANA_USER=admin
GRAFANA_PASSWORD=$GRAFANA_PASS
EOF
    echo "Generated .env with API key: $API_KEY"
fi

# Build and start
cd "$DEMO_DIR/repo/deploy/demo"
docker compose build
docker compose up -d

echo ""
echo "Waiting for setup to complete..."
docker compose logs -f setup --no-log-prefix 2>/dev/null || true

echo ""
echo "=== Demo is live at https://demo.varpulis-cep.com ==="
echo "API key: $(grep VARPULIS_API_KEY "$ENV_FILE" | cut -d= -f2)"
echo "Grafana: https://demo.varpulis-cep.com:3000 (internal only)"
echo ""
echo "Useful commands:"
echo "  docker compose logs -f generator    # Watch event generation"
echo "  docker compose logs -f worker-0     # Watch event processing"
echo "  docker compose ps                   # Check service status"
echo "  docker compose down                 # Stop all services"
echo "  docker compose up -d --build        # Rebuild and restart"
