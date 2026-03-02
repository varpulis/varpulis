#!/usr/bin/env bash
set -euo pipefail
#
# deploy-update.sh — Zero-surprise deployment for Varpulis demo
#
# 1. Pull latest code
# 2. Build new Docker images (no downtime yet — old services keep running)
# 3. Enable maintenance page via Caddy
# 4. Undeploy all pipelines to avoid stale/duplicate state
# 5. Restart services with new images
# 6. Wait for health checks to pass (setup container redeploys pipelines)
# 7. Restore normal Caddy routing
#
# Usage:
#   ssh cpo@95.216.191.129 'bash ~/varpulis-demo/repo/deploy/demo/deploy-update.sh'

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

# Inject version into maintenance HTML and copy into Caddy container
MAINT_DIR="/tmp/varpulis-maintenance"
mkdir -p "$MAINT_DIR"
sed "s/<!-- VERSION -->/${VERSION}/g" maintenance.html > "$MAINT_DIR/maintenance.html"

docker exec demo-caddy mkdir -p /srv/maintenance 2>/dev/null || true
docker cp "$MAINT_DIR/maintenance.html" demo-caddy:/srv/maintenance/maintenance.html

# Swap Caddyfile on host (it's bind-mounted) and reload Caddy
cp Caddyfile Caddyfile.backup
cp Caddyfile.maintenance Caddyfile
docker exec demo-caddy caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile 2>&1 | tail -1
echo "  Maintenance page active"

# ─── Step 4: Clean up pipelines ──────────────────────────────────────────
step "Cleaning up deployed pipelines..."
API_KEY=$(grep VARPULIS_API_KEY .env | cut -d= -f2)

# Get all pipeline group IDs and delete them
GROUPS_JSON=$(docker exec demo-coordinator-1 curl -sf \
    -H "X-API-Key: ${API_KEY}" \
    http://localhost:9100/api/v1/cluster/pipeline-groups 2>/dev/null || echo '{"pipeline_groups":[]}')

GROUP_IDS=$(echo "$GROUPS_JSON" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    for g in data.get('pipeline_groups', []):
        gid = g.get('id', '')
        name = g.get('name', '')
        if gid:
            print(f'{gid} {name}')
except: pass
" 2>/dev/null || true)

if [ -n "$GROUP_IDS" ]; then
    echo "$GROUP_IDS" | while read -r gid name; do
        echo "  Removing: $name ($gid)"
        docker exec demo-coordinator-1 curl -sf -X DELETE \
            -H "X-API-Key: ${API_KEY}" \
            "http://localhost:9100/api/v1/cluster/pipeline-groups/${gid}" 2>/dev/null || true
    done
else
    echo "  No pipeline groups to clean up"
fi

# ─── Step 5: Restart services ────────────────────────────────────────────
step "Restarting services with new images..."
docker compose up -d --no-build 2>&1 | grep -v "Running" || true

# ─── Step 6: Wait for health ─────────────────────────────────────────────
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

# Wait for setup container to finish (deploys pipelines + connectors)
step "Waiting for setup to finish deploying pipelines..."
for i in $(seq 1 90); do
    STATUS=$(docker inspect -f '{{.State.Status}}' demo-setup 2>/dev/null || echo "unknown")
    if [ "$STATUS" = "exited" ]; then
        EXIT_CODE=$(docker inspect -f '{{.State.ExitCode}}' demo-setup 2>/dev/null || echo "?")
        echo "  Setup completed (exit code: $EXIT_CODE, ${i}s)"
        if [ "$EXIT_CODE" != "0" ]; then
            warn "Setup failed! Check: docker logs demo-setup"
        fi
        break
    fi
    [ "$i" -eq 90 ] && warn "Setup still running after 90s"
    sleep 1
done

# ─── Step 7: Restore normal Caddy config ─────────────────────────────────
step "Restoring normal Caddy config..."
cp Caddyfile.backup Caddyfile
rm -f Caddyfile.backup
docker exec demo-caddy caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile 2>&1 | tail -1
echo "  Normal routing restored"

# ─── Summary ──────────────────────────────────────────────────────────────
step "Deployment complete!"
echo "  Version: ${VERSION} (${COMMIT})"
echo "  URL:     https://demo.varpulis-cep.com"
echo ""
docker compose ps --format "table {{.Name}}\t{{.Status}}" 2>/dev/null | head -20
