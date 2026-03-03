#!/usr/bin/env bash
# k3d SaaS Smoke Tests
#
# Runs 18 smoke tests against the local k3d SaaS deployment.
# Each test prints colored PASS/FAIL. Exit code 0 if all pass, 1 otherwise.
#
# Usage: ./scripts/k3d-saas-test.sh

set -uo pipefail

NAMESPACE="varpulis-k3d-saas"
API_URL="http://127.0.0.1:9000"
UI_URL="http://127.0.0.1:8080"
API_KEY="k3d-saas-test-key"

# Colors
if [[ -z "${NO_COLOR:-}" && -t 1 ]]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BOLD='\033[1m'
    NC='\033[0m'
else
    RED='' GREEN='' YELLOW='' BOLD='' NC=''
fi

PASSED=0
FAILED=0
TOTAL=0

pass() {
    echo -e "  ${GREEN}PASS${NC}  $1"
    PASSED=$((PASSED + 1))
    TOTAL=$((TOTAL + 1))
}

fail() {
    echo -e "  ${RED}FAIL${NC}  $1${2:+ — $2}"
    FAILED=$((FAILED + 1))
    TOTAL=$((TOTAL + 1))
}

# ─── Extract admin credentials ─────────────────────────────────────────────
ADMIN_PASSWORD=""
for i in $(seq 1 10); do
    ADMIN_PASSWORD=$(kubectl logs -l app.kubernetes.io/component=worker -n "$NAMESPACE" --tail=-1 2>/dev/null \
        | grep -oP 'Password: \K\S+' | head -1 || true)
    if [[ -n "$ADMIN_PASSWORD" ]]; then
        break
    fi
    sleep 2
done

JWT_TOKEN=""
ORG_ID=""
ORG_API_KEY=""
PIPELINE_ID=""
TENANT_ID=""

echo ""
echo -e "${BOLD}Varpulis SaaS Smoke Tests${NC}"
echo "──────────────────────────────────────────────"
echo ""

# ─── Test 1: Health endpoint ───────────────────────────────────────────────
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$API_URL/health" 2>/dev/null || true)
if [[ "$HTTP_CODE" == "200" ]]; then
    pass "#1  Health endpoint (GET /health)"
else
    fail "#1  Health endpoint (GET /health)" "HTTP $HTTP_CODE"
fi

# ─── Test 2: Ready endpoint ───────────────────────────────────────────────
# In SaaS mode, /ready returns 503 when no VPL is loaded — that's expected.
# We just verify the endpoint responds (200 or 503).
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$API_URL/ready" 2>/dev/null || true)
if [[ "$HTTP_CODE" == "200" || "$HTTP_CODE" == "503" ]]; then
    pass "#2  Ready endpoint (GET /ready)"
else
    fail "#2  Ready endpoint (GET /ready)" "HTTP $HTTP_CODE"
fi

# ─── Test 3: Admin login ──────────────────────────────────────────────────
if [[ -z "$ADMIN_PASSWORD" ]]; then
    fail "#3  Admin login (POST /auth/login)" "Could not extract admin password"
else
    LOGIN_RESP=$(curl -s -w "\n%{http_code}" -X POST "$API_URL/auth/login" \
        -H "Content-Type: application/json" \
        -d "{\"username\":\"admin\",\"password\":\"$ADMIN_PASSWORD\"}" 2>/dev/null || true)
    LOGIN_BODY=$(echo "$LOGIN_RESP" | head -n -1)
    LOGIN_CODE=$(echo "$LOGIN_RESP" | tail -1)

    JWT_TOKEN=$(echo "$LOGIN_BODY" | jq -r '.token // empty' 2>/dev/null || true)
    if [[ "$LOGIN_CODE" == "200" && -n "$JWT_TOKEN" ]]; then
        pass "#3  Admin login (POST /auth/login)"
    else
        fail "#3  Admin login (POST /auth/login)" "HTTP $LOGIN_CODE"
    fi
fi

# ─── Test 4: List tenants ─────────────────────────────────────────────────
if [[ -n "$JWT_TOKEN" ]]; then
    RESP=$(curl -s -w "\n%{http_code}" "$API_URL/api/v1/admin/tenants" \
        -H "Authorization: Bearer $JWT_TOKEN" 2>/dev/null || true)
    BODY=$(echo "$RESP" | head -n -1)
    CODE=$(echo "$RESP" | tail -1)
    if [[ "$CODE" == "200" ]]; then
        TENANT_ID=$(echo "$BODY" | jq -r '.[0].id // empty' 2>/dev/null || true)
        pass "#4  List tenants (GET /api/v1/admin/tenants)"
    else
        fail "#4  List tenants (GET /api/v1/admin/tenants)" "HTTP $CODE"
    fi
else
    fail "#4  List tenants (GET /api/v1/admin/tenants)" "No JWT token"
fi

# ─── Test 5: Admin usage ──────────────────────────────────────────────────
if [[ -n "$JWT_TOKEN" ]]; then
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$API_URL/api/v1/admin/usage" \
        -H "Authorization: Bearer $JWT_TOKEN" 2>/dev/null || true)
    if [[ "$HTTP_CODE" == "200" ]]; then
        pass "#5  Admin usage (GET /api/v1/admin/usage)"
    else
        fail "#5  Admin usage (GET /api/v1/admin/usage)" "HTTP $HTTP_CODE"
    fi
else
    fail "#5  Admin usage (GET /api/v1/admin/usage)" "No JWT token"
fi

# ─── Test 6: Create org ───────────────────────────────────────────────────
if [[ -n "$JWT_TOKEN" ]]; then
    RESP=$(curl -s -w "\n%{http_code}" -X POST "$API_URL/api/v1/orgs" \
        -H "Authorization: Bearer $JWT_TOKEN" \
        -H "Content-Type: application/json" \
        -d '{"name":"smoke-test-org","display_name":"Smoke Test Org"}' 2>/dev/null || true)
    BODY=$(echo "$RESP" | head -n -1)
    CODE=$(echo "$RESP" | tail -1)
    ORG_ID=$(echo "$BODY" | jq -r '.id // empty' 2>/dev/null || true)
    if [[ "$CODE" == "201" && -n "$ORG_ID" ]]; then
        pass "#6  Create org (POST /api/v1/orgs)"
    else
        fail "#6  Create org (POST /api/v1/orgs)" "HTTP $CODE"
    fi
else
    fail "#6  Create org (POST /api/v1/orgs)" "No JWT token"
fi

# ─── Test 7: List orgs ────────────────────────────────────────────────────
if [[ -n "$JWT_TOKEN" ]]; then
    RESP=$(curl -s -w "\n%{http_code}" "$API_URL/api/v1/orgs" \
        -H "Authorization: Bearer $JWT_TOKEN" 2>/dev/null || true)
    BODY=$(echo "$RESP" | head -n -1)
    CODE=$(echo "$RESP" | tail -1)
    if [[ "$CODE" == "200" ]] && echo "$BODY" | jq -e '.[].name' &>/dev/null; then
        pass "#7  List orgs (GET /api/v1/orgs)"
    else
        fail "#7  List orgs (GET /api/v1/orgs)" "HTTP $CODE"
    fi
else
    fail "#7  List orgs (GET /api/v1/orgs)" "No JWT token"
fi

# ─── Test 8: Create API key ───────────────────────────────────────────────
if [[ -n "$JWT_TOKEN" && -n "$ORG_ID" ]]; then
    RESP=$(curl -s -w "\n%{http_code}" -X POST "$API_URL/api/v1/orgs/$ORG_ID/api-keys" \
        -H "Authorization: Bearer $JWT_TOKEN" \
        -H "Content-Type: application/json" \
        -d '{"name":"smoke-test-key"}' 2>/dev/null || true)
    BODY=$(echo "$RESP" | head -n -1)
    CODE=$(echo "$RESP" | tail -1)
    ORG_API_KEY=$(echo "$BODY" | jq -r '.key // empty' 2>/dev/null || true)
    if [[ "$CODE" == "201" ]]; then
        pass "#8  Create API key (POST /api/v1/orgs/{id}/api-keys)"
    else
        fail "#8  Create API key (POST /api/v1/orgs/{id}/api-keys)" "HTTP $CODE"
    fi
else
    fail "#8  Create API key (POST /api/v1/orgs/{id}/api-keys)" "Missing JWT or org ID"
fi

# ─── Test 9: Create pipeline ──────────────────────────────────────────────
RESP=$(curl -s -w "\n%{http_code}" -X POST "$API_URL/api/v1/pipelines" \
    -H "X-API-Key: $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"name":"smoke-test","source":"stream SmokeTest = Temperature as t .where(t.value > 100) .emit(temp: t.value)"}' 2>/dev/null || true)
BODY=$(echo "$RESP" | head -n -1)
CODE=$(echo "$RESP" | tail -1)
PIPELINE_ID=$(echo "$BODY" | jq -r '.id // .pipeline_id // empty' 2>/dev/null || true)
if [[ "$CODE" == "201" || "$CODE" == "200" ]]; then
    pass "#9  Create pipeline (POST /api/v1/pipelines)"
else
    fail "#9  Create pipeline (POST /api/v1/pipelines)" "HTTP $CODE"
fi

# ─── Test 10: List pipelines ──────────────────────────────────────────────
RESP=$(curl -s -w "\n%{http_code}" "$API_URL/api/v1/pipelines" \
    -H "X-API-Key: $API_KEY" 2>/dev/null || true)
BODY=$(echo "$RESP" | head -n -1)
CODE=$(echo "$RESP" | tail -1)
if [[ "$CODE" == "200" ]]; then
    pass "#10 List pipelines (GET /api/v1/pipelines)"
else
    fail "#10 List pipelines (GET /api/v1/pipelines)" "HTTP $CODE"
fi

# ─── Test 11: Delete pipeline ─────────────────────────────────────────────
if [[ -n "$PIPELINE_ID" ]]; then
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE "$API_URL/api/v1/pipelines/$PIPELINE_ID" \
        -H "X-API-Key: $API_KEY" 2>/dev/null || true)
    if [[ "$HTTP_CODE" == "200" || "$HTTP_CODE" == "204" ]]; then
        pass "#11 Delete pipeline (DELETE /api/v1/pipelines/{id})"
    else
        fail "#11 Delete pipeline (DELETE /api/v1/pipelines/{id})" "HTTP $HTTP_CODE"
    fi
else
    fail "#11 Delete pipeline (DELETE /api/v1/pipelines/{id})" "No pipeline ID"
fi

# ─── Test 12: Set trial status ─────────────────────────────────────────────
if [[ -n "$JWT_TOKEN" && -n "$TENANT_ID" ]]; then
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X PUT "$API_URL/api/v1/admin/tenants/$TENANT_ID/status" \
        -H "Authorization: Bearer $JWT_TOKEN" \
        -H "Content-Type: application/json" \
        -d '{"status":"trial"}' 2>/dev/null || true)
    if [[ "$HTTP_CODE" == "200" ]]; then
        pass "#12 Set trial status (PUT /api/v1/admin/tenants/{id}/status)"
    else
        fail "#12 Set trial status (PUT /api/v1/admin/tenants/{id}/status)" "HTTP $HTTP_CODE"
    fi
else
    fail "#12 Set trial status (PUT /api/v1/admin/tenants/{id}/status)" "Missing JWT or tenant ID"
fi

# ─── Test 13: Extend trial ────────────────────────────────────────────────
if [[ -n "$JWT_TOKEN" && -n "$TENANT_ID" ]]; then
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X PUT "$API_URL/api/v1/admin/tenants/$TENANT_ID/trial" \
        -H "Authorization: Bearer $JWT_TOKEN" \
        -H "Content-Type: application/json" \
        -d '{"days":30}' 2>/dev/null || true)
    if [[ "$HTTP_CODE" == "200" ]]; then
        pass "#13 Extend trial (PUT /api/v1/admin/tenants/{id}/trial)"
    else
        fail "#13 Extend trial (PUT /api/v1/admin/tenants/{id}/trial)" "HTTP $HTTP_CODE"
    fi
else
    fail "#13 Extend trial (PUT /api/v1/admin/tenants/{id}/trial)" "Missing JWT or tenant ID"
fi

# ─── Test 14: Update limits ───────────────────────────────────────────────
if [[ -n "$JWT_TOKEN" && -n "$TENANT_ID" ]]; then
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X PUT "$API_URL/api/v1/admin/tenants/$TENANT_ID/limits" \
        -H "Authorization: Bearer $JWT_TOKEN" \
        -H "Content-Type: application/json" \
        -d '{"max_events_per_day":1000,"max_pipelines":5}' 2>/dev/null || true)
    if [[ "$HTTP_CODE" == "200" ]]; then
        pass "#14 Update limits (PUT /api/v1/admin/tenants/{id}/limits)"
    else
        fail "#14 Update limits (PUT /api/v1/admin/tenants/{id}/limits)" "HTTP $HTTP_CODE"
    fi
else
    fail "#14 Update limits (PUT /api/v1/admin/tenants/{id}/limits)" "Missing JWT or tenant ID"
fi

# ─── Test 15: Inject events ───────────────────────────────────────────────
# Deploy a fresh pipeline for injection testing (we deleted the previous one in test 11)
INJECT_PIPELINE_ID=""
if [[ -n "$API_KEY" ]]; then
    INJECT_RESP=$(curl -s -w "\n%{http_code}" -X POST "$API_URL/api/v1/pipelines" \
        -H "X-API-Key: $API_KEY" \
        -H "Content-Type: application/json" \
        -d '{"name":"inject-test","source":"stream InjectTest = Temperature as t .where(t.value > 50) .emit(temp: t.value)"}' 2>/dev/null || true)
    INJECT_BODY=$(echo "$INJECT_RESP" | head -n -1)
    INJECT_PIPELINE_ID=$(echo "$INJECT_BODY" | jq -r '.id // .pipeline_id // empty' 2>/dev/null || true)
fi
if [[ -n "$INJECT_PIPELINE_ID" ]]; then
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$API_URL/api/v1/pipelines/$INJECT_PIPELINE_ID/events-batch" \
        -H "X-API-Key: $API_KEY" \
        -H "Content-Type: application/json" \
        -d '{"events":[{"event_type":"Temperature","fields":{"value":120,"unit":"celsius"}},{"event_type":"Temperature","fields":{"value":95,"unit":"celsius"}}]}' 2>/dev/null || true)
    if [[ "$HTTP_CODE" == "200" || "$HTTP_CODE" == "202" ]]; then
        pass "#15 Inject events (POST /api/v1/pipelines/{id}/events-batch)"
    else
        fail "#15 Inject events (POST /api/v1/pipelines/{id}/events-batch)" "HTTP $HTTP_CODE"
    fi
    # Clean up inject test pipeline
    curl -s -o /dev/null -X DELETE "$API_URL/api/v1/pipelines/$INJECT_PIPELINE_ID" \
        -H "X-API-Key: $API_KEY" 2>/dev/null || true
else
    fail "#15 Inject events (POST /api/v1/pipelines/{id}/events-batch)" "Could not create inject test pipeline"
fi

# ─── Test 16: Verify usage ────────────────────────────────────────────────
if [[ -n "$JWT_TOKEN" && -n "$TENANT_ID" ]]; then
    RESP=$(curl -s -w "\n%{http_code}" "$API_URL/api/v1/admin/tenants/$TENANT_ID" \
        -H "Authorization: Bearer $JWT_TOKEN" 2>/dev/null || true)
    CODE=$(echo "$RESP" | tail -1)
    if [[ "$CODE" == "200" ]]; then
        pass "#16 Verify usage (GET /api/v1/admin/tenants/{id})"
    else
        fail "#16 Verify usage (GET /api/v1/admin/tenants/{id})" "HTTP $CODE"
    fi
else
    fail "#16 Verify usage (GET /api/v1/admin/tenants/{id})" "Missing JWT or tenant ID"
fi

# ─── Test 17: Web UI serves ───────────────────────────────────────────────
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$UI_URL/" 2>/dev/null || true)
if [[ "$HTTP_CODE" == "200" ]]; then
    pass "#17 Web UI serves (GET http://localhost:8080/)"
else
    fail "#17 Web UI serves (GET http://localhost:8080/)" "HTTP $HTTP_CODE"
fi

# ─── Test 18: Billing graceful ─────────────────────────────────────────────
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$API_URL/api/v1/billing/plan" \
    -H "X-API-Key: $API_KEY" 2>/dev/null || true)
if [[ "$HTTP_CODE" == "503" || "$HTTP_CODE" == "404" || "$HTTP_CODE" == "401" || "$HTTP_CODE" == "400" ]]; then
    pass "#18 Billing graceful (GET /api/v1/billing/plan — no Stripe)"
else
    fail "#18 Billing graceful (GET /api/v1/billing/plan)" "HTTP $HTTP_CODE (expected 4xx/503)"
fi

# ─── Summary ───────────────────────────────────────────────────────────────
echo ""
echo "──────────────────────────────────────────────"
echo -e "  ${GREEN}Passed:${NC}  $PASSED"
echo -e "  ${RED}Failed:${NC}  $FAILED"
echo -e "  Total:   $TOTAL"
echo "──────────────────────────────────────────────"
echo ""

if [[ "$FAILED" -gt 0 ]]; then
    echo -e "${RED}Some tests failed.${NC}"
    exit 1
fi

echo -e "${GREEN}All smoke tests passed.${NC}"
exit 0
