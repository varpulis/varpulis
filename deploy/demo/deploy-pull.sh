#!/usr/bin/env bash
set -euo pipefail

# Pull-based deploy: use pre-built GHCR images instead of building on-server.
# Usage: ssh cpo@95.216.191.129 'cd ~/varpulis-demo/repo/deploy/demo && ./deploy-pull.sh'
#
# Prerequisites (one-time):
#   echo "$GHCR_TOKEN" | docker login ghcr.io -u varpulis --password-stdin

echo "=== Pulling latest images from GHCR ==="
docker compose -f docker-compose.yml -f docker-compose.prod.yml pull

echo "=== Restarting services ==="
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --no-build

echo "=== Waiting for coordinator health ==="
for i in $(seq 1 60); do
    if docker compose -f docker-compose.yml -f docker-compose.prod.yml exec -T coordinator-1 \
        curl -sf http://localhost:9100/health >/dev/null 2>&1; then
        echo "  Coordinator-1 healthy (${i}s)"
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo "WARNING: coordinator not healthy after 60s"
    fi
    sleep 1
done

echo "=== Running setup ==="
docker compose -f docker-compose.yml -f docker-compose.prod.yml run --rm setup

echo "=== Deploy complete ==="
docker compose -f docker-compose.yml -f docker-compose.prod.yml ps
