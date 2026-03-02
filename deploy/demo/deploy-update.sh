#!/usr/bin/env bash
set -euo pipefail
#
# deploy-update.sh — Zero-surprise deployment for Varpulis demo
#
# 1. Pull latest code
# 2. Build new Docker images (no downtime yet)
# 3. Enable maintenance page via Caddy (shows version being deployed)
# 4. Restart services with new images
# 5. Wait for health checks to pass
# 6. Restore normal Caddyfile
#
# Usage:
#   ssh cpo@95.216.191.129 'bash ~/varpulis-demo/repo/deploy/demo/deploy-update.sh'
#   OR from local: ssh cpo@95.216.191.129 < deploy/demo/deploy-update.sh

DEMO_DIR="${HOME}/varpulis-demo/repo/deploy/demo"
REPO_DIR="${HOME}/varpulis-demo/repo"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

step()  { echo -e "\n${GREEN}==> $1${NC}"; }
warn()  { echo -e "${YELLOW}    $1${NC}"; }
fail()  { echo -e "${RED}ERROR: $1${NC}"; exit 1; }
timer() { date +%s; }

cd "$REPO_DIR"

# ─── Step 1: Pull latest code ────────────────────────────────────────────
step "Pulling latest code..."
git pull --quiet || fail "git pull failed"

# Extract version from Cargo.toml
VERSION=$(grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)"/\1/')
COMMIT=$(git rev-parse --short HEAD)
echo "  Version: ${VERSION} (${COMMIT})"

cd "$DEMO_DIR"

# ─── Step 2: Build images (no downtime) ──────────────────────────────────
step "Building Docker images (services still running)..."
START=$(timer)
docker compose build 2>&1 | grep -E '(Built|ERROR|FAILED)' || true
END=$(timer)
echo "  Build completed in $((END - START))s"

# ─── Step 3: Enable maintenance page ─────────────────────────────────────
step "Enabling maintenance page (v${VERSION})..."

# Inject version into maintenance HTML
MAINT_DIR="/tmp/varpulis-maintenance"
mkdir -p "$MAINT_DIR"
sed "s/<!-- VERSION -->/${VERSION}/g" maintenance.html > "$MAINT_DIR/maintenance.html"

# Copy maintenance Caddyfile and mount maintenance dir
docker cp "$MAINT_DIR/maintenance.html" demo-caddy:/srv/maintenance/maintenance.html 2>/dev/null || {
    docker exec demo-caddy mkdir -p /srv/maintenance
    docker cp "$MAINT_DIR/maintenance.html" demo-caddy:/srv/maintenance/maintenance.html
}
docker cp Caddyfile.maintenance demo-caddy:/etc/caddy/Caddyfile
docker exec demo-caddy caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile 2>&1 | tail -1
echo "  Maintenance page active"

# ─── Step 4: Restart services ────────────────────────────────────────────
step "Restarting services with new images..."
docker compose up -d --no-build 2>&1 | grep -v "Running" || true

# ─── Step 5: Wait for health ─────────────────────────────────────────────
step "Waiting for services to become healthy..."
MAX_WAIT=120
for i in $(seq 1 $MAX_WAIT); do
    HEALTHY=$(docker ps --filter "name=demo-coordinator-1" --filter "health=healthy" -q)
    if [ -n "$HEALTHY" ]; then
        echo "  Coordinator-1 healthy (${i}s)"
        break
    fi
    [ "$i" -eq "$MAX_WAIT" ] && warn "Coordinator not healthy after ${MAX_WAIT}s — restoring anyway"
    sleep 1
done

# Also wait for web-ui
for i in $(seq 1 60); do
    HEALTHY=$(docker ps --filter "name=demo-web-ui" --filter "health=healthy" -q)
    if [ -n "$HEALTHY" ]; then
        echo "  Web UI healthy (${i}s)"
        break
    fi
    [ "$i" -eq 60 ] && warn "Web UI not healthy after 60s"
    sleep 1
done

# ─── Step 6: Restore normal Caddy config ─────────────────────────────────
step "Restoring normal Caddy config..."
docker cp Caddyfile demo-caddy:/etc/caddy/Caddyfile
docker exec demo-caddy caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile 2>&1 | tail -1
echo "  Normal routing restored"

# ─── Summary ──────────────────────────────────────────────────────────────
step "Deployment complete!"
echo "  Version: ${VERSION} (${COMMIT})"
echo "  URL:     https://demo.varpulis-cep.com"
echo ""
docker compose ps --format "table {{.Name}}\t{{.Status}}" 2>/dev/null | head -20
