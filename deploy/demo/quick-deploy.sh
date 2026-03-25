#!/usr/bin/env bash
set -euo pipefail

# Quick Deploy: build locally → push to GHCR → update Hetzner
#
# This is the fastest iteration cycle:
#   1. Builds release binary locally (incremental: ~30s if only a few files changed)
#   2. Creates Docker image from prebuilt binary (~10s)
#   3. Pushes to GHCR (~20s)
#   4. SSHs to Hetzner and pulls + restarts (~30s)
#
# Total: ~90 seconds for a code change to be live on Hetzner
#
# Prerequisites:
#   - gh auth login (for GHCR push)
#   - SSH access to Hetzner (ssh cpo@95.216.191.129)
#   - cargo, docker installed locally
#
# Usage: ./deploy/demo/quick-deploy.sh

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

HETZNER_HOST="${HETZNER_HOST:-cpo@95.216.191.129}"
DEMO_DIR="~/varpulis-demo/repo/deploy/demo"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

step() { echo -e "\n${GREEN}==> $1${NC}"; }
warn() { echo -e "${YELLOW}==> $1${NC}"; }

# ─── Step 1: Build release binary ──────────────────────────────────────────
step "Building release binary (incremental)..."
START=$(date +%s)
cargo build -p varpulis-cli --release --features "kafka,raft,persistent,saas" 2>&1 | tail -3
END=$(date +%s)
echo "  Binary built in $((END - START))s"

# ─── Step 2: Build Docker image from prebuilt binary ───────────────────────
step "Building Docker image (prebuilt binary)..."
START=$(date +%s)
docker build -t ghcr.io/varpulis/varpulis:main \
    -f deploy/docker/Dockerfile.prebuilt . 2>&1 | tail -3
END=$(date +%s)
echo "  Image built in $((END - START))s"

# ─── Step 3: Web UI ───────────────────────────────────────────────────────
# Web UI is now in a separate repo: https://github.com/varpulis/varpulis-web-ui
# Build and push it from that repo, or pull the pre-built image.

# ─── Step 4: Push to GHCR ──────────────────────────────────────────────────
step "Pushing images to GHCR..."

# Login if needed
echo "$(gh auth token)" | docker login ghcr.io -u varpulis --password-stdin 2>/dev/null

START=$(date +%s)
docker push ghcr.io/varpulis/varpulis:main 2>&1 | tail -3
END=$(date +%s)
echo "  Pushed in $((END - START))s"

# ─── Step 5: Update Hetzner ────────────────────────────────────────────────
step "Updating Hetzner ($HETZNER_HOST)..."
ssh "$HETZNER_HOST" bash -s <<'REMOTE'
set -euo pipefail
cd ~/varpulis-demo/repo
git pull --quiet

cd deploy/demo

# Login to GHCR (uses gh token or existing credentials)
if ! docker pull ghcr.io/varpulis/varpulis:main >/dev/null 2>&1; then
    echo "GHCR login needed on Hetzner. Run: echo \$TOKEN | docker login ghcr.io -u varpulis --password-stdin"
    exit 1
fi

echo "Pulling images..."
docker compose -f docker-compose.yml -f docker-compose.prod.yml pull --quiet

echo "Restarting services..."
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --no-build

echo "Waiting for coordinator-1..."
for i in $(seq 1 60); do
    if docker compose exec -T coordinator-1 curl -sf http://localhost:9100/health >/dev/null 2>&1; then
        echo "  Coordinator healthy (${i}s)"
        break
    fi
    [ "$i" -eq 60 ] && echo "  WARNING: coordinator not healthy after 60s"
    sleep 1
done

echo "Running setup..."
docker compose -f docker-compose.yml -f docker-compose.prod.yml run --rm setup 2>&1 | tail -5

echo "Status:"
docker compose -f docker-compose.yml -f docker-compose.prod.yml ps --format "table {{.Name}}\t{{.Status}}" | head -20
REMOTE

step "Done! Demo updated at https://demo.varpulis-cep.com"
