#!/usr/bin/env bash
# Helm template rendering tests.
# Usage: bash tests/helm/test_templates.sh
# Requires: helm (any recent version)
set -euo pipefail

CHART_DIR="helm/dagster-codekit"

echo "=== 1. Default render (no auth) ==="
helm template test-release "$CHART_DIR" > /dev/null
echo "  OK"

echo "=== 2. Auth enabled with plaintext tokens ==="
output=$(helm template test-release "$CHART_DIR" \
  --set codekit.auth.enabled=true \
  --set codekit.auth.tokens[0]=my-plaintext-token)
if echo "$output" | grep -q "\- \"my-plaintext-token\""; then
  echo "  OK: plaintext token rendered in ConfigMap"
else
  echo "  FAIL: plaintext token not found in ConfigMap"
  exit 1
fi
# Should NOT contain the placeholder
if echo "$output" | grep -q "CODEKIT_AUTH_TOKENS"; then
  echo "  FAIL: placeholder should NOT appear when existingSecret is not set"
  exit 1
fi
echo "  OK: no placeholder (correct)"

echo "=== 3. Auth enabled with existingSecret ==="
output=$(helm template test-release "$CHART_DIR" \
  --set codekit.auth.enabled=true \
  --set codekit.auth.existingSecret=my-secret)
# ConfigMap should contain the placeholder
if echo "$output" | grep -q '\${CODEKIT_AUTH_TOKENS}'; then
  echo "  OK: placeholder rendered in ConfigMap"
else
  echo "  FAIL: placeholder not found in ConfigMap"
  exit 1
fi
# ConfigMap should NOT contain any plaintext token (tokens list is empty)
if echo "$output" | grep -A5 'tokens:' | grep -q '\- "'; then
  echo "  FAIL: plaintext token found in ConfigMap despite existingSecret"
  exit 1
fi
echo "  OK: no plaintext tokens (correct)"

# Deployment should contain CODEKIT_AUTH_TOKENS env var from secret
if echo "$output" | grep -A4 "CODEKIT_AUTH_TOKENS" | grep -q "secretKeyRef"; then
  echo "  OK: CODEKIT_AUTH_TOKENS env var with secretKeyRef in Deployment"
else
  echo "  FAIL: CODEKIT_AUTH_TOKENS env var not found in Deployment"
  exit 1
fi

# Deployment should also contain CODEKIT_TOKEN for CLI
if echo "$output" | grep -A4 "CODEKIT_TOKEN" | grep -q "secretKeyRef"; then
  echo "  OK: CODEKIT_TOKEN env var with secretKeyRef in Deployment"
else
  echo "  FAIL: CODEKIT_TOKEN env var not found in Deployment"
  exit 1
fi

# Default secret key should be "token"
# Scope to Deployment section (has "valueFrom" before the key)
deploy_section=$(echo "$output" | awk '/^---$/,0' | tail -n +2)
key_line=$(echo "$deploy_section" | grep -A5 "CODEKIT_AUTH_TOKENS" | grep "key:" | head -1)
if echo "$key_line" | grep -q "key: token"; then
  echo "  OK: default secret key is 'token'"
else
  echo "  FAIL: default secret key is not 'token', got: $key_line"
  exit 1
fi

echo "=== 4. Auth enabled with existingSecret and custom key ==="
output=$(helm template test-release "$CHART_DIR" \
  --set codekit.auth.enabled=true \
  --set codekit.auth.existingSecret=my-secret \
  --set codekit.auth.existingSecretKey=custom-key)
deploy_section=$(echo "$output" | awk '/^---$/,0' | tail -n +2)
key_line=$(echo "$deploy_section" | grep -A5 "CODEKIT_AUTH_TOKENS" | grep "key:" | head -1)
if echo "$key_line" | grep -q "key: custom-key"; then
  echo "  OK: custom secret key honored"
else
  echo "  FAIL: custom secret key not found, got: $key_line"
  exit 1
fi

echo ""
echo "All Helm template tests passed!"
