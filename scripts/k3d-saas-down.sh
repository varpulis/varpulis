#!/usr/bin/env bash
# k3d SaaS Environment Teardown
#
# Deletes the k3d SaaS cluster and all associated resources.
#
# Usage: ./scripts/k3d-saas-down.sh

set -euo pipefail

CLUSTER_NAME="varpulis-saas"

# Colors
if [[ -z "${NO_COLOR:-}" && -t 1 ]]; then
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    NC='\033[0m'
else
    GREEN='' YELLOW='' NC=''
fi

if k3d cluster list 2>/dev/null | grep -q "$CLUSTER_NAME"; then
    echo -e "${YELLOW}Deleting k3d cluster '$CLUSTER_NAME'...${NC}"
    k3d cluster delete "$CLUSTER_NAME"
    echo -e "${GREEN}Cluster '$CLUSTER_NAME' deleted.${NC}"
else
    echo "Cluster '$CLUSTER_NAME' does not exist."
fi
