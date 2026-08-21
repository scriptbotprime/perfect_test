#!/usr/bin/env bash
# Trigger the job-c deployment from outside Prefect, via its REST API.
set -euo pipefail

PREFECT_API_URL="${PREFECT_API_URL:-http://localhost:4200/api}"

# 1. Look up job-c's deployment ID by name
DEPLOYMENT_ID=$(curl -s -X POST "${PREFECT_API_URL}/deployments/filter" \
  -H "Content-Type: application/json" \
  -d '{"deployments": {"name": {"any_": ["job-c"]}}}' \
  | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)

# 2. Trigger a new flow run for that deployment
curl -s -X POST "${PREFECT_API_URL}/deployments/${DEPLOYMENT_ID}/create_flow_run" \
  -H "Content-Type: application/json" \
  -d '{"parameters": {"triggered_by": "manual external trigger", "random_number": 66}}'
