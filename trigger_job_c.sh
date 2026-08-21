#!/usr/bin/env bash
# Trigger the job-c deployment from outside Prefect, via its REST API.
# Requires PREFECT_AUTH_STRING to be set (same value as in .env, e.g. "admin:your-password").
set -euo pipefail

PREFECT_API_URL="${PREFECT_API_URL:-http://localhost:4200/api}"
: "${PREFECT_AUTH_STRING:?Set PREFECT_AUTH_STRING, e.g. export PREFECT_AUTH_STRING=admin:your-password}"

# 1. Look up job-c's deployment ID by name
DEPLOYMENT_ID=$(curl -s -u "${PREFECT_AUTH_STRING}" -X POST "${PREFECT_API_URL}/deployments/filter" \
  -H "Content-Type: application/json" \
  -d '{"deployments": {"name": {"any_": ["job-c"]}}}' \
  | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)

# 2. Trigger a new flow run for that deployment
curl -s -u "${PREFECT_AUTH_STRING}" -X POST "${PREFECT_API_URL}/deployments/${DEPLOYMENT_ID}/create_flow_run" \
  -H "Content-Type: application/json" \
  -d '{"parameters": {"triggered_by": "manual external trigger", "random_number": 66}}'
