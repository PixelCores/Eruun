#!/usr/bin/env bash
set -euo pipefail

mode="${1:-instant}"
root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
api_addr="${API_ADDR:-http://localhost:8080/api/v1}"
namespace="${KUBECTL_NAMESPACE:-default}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

require_cmd curl
require_cmd kubectl
require_cmd python3

component_name=""
resource_kind=""
payload=""
start_time=""

case "$mode" in
  instant)
    payload="$root_dir/examples/job-components/instant-job.json"
    component_name="instant-task"
    resource_kind="job"
    ;;
  cron)
    payload="$root_dir/examples/job-components/scheduled-job-cron.json"
    component_name="cron-task"
    resource_kind="cronjob"
    ;;
  delay)
    component_name="delay-task"
    resource_kind="job"
    start_time="$(( $(date +%s) + 30 ))"
    payload="$(mktemp)"
    cat >"$payload" <<EOF
{
  "name": "job-demo-delay",
  "namespace": "default",
  "version": "1.0.0",
  "project": "demo-project",
  "description": "Job (startTime) demo",
  "component": [
    {
      "name": "delay-task",
      "type": "job",
      "image": "busybox:1.36",
      "namespace": "default",
      "replicas": 1,
      "properties": {
        "startTime": ${start_time},
        "runPolicy": "skip_if_completed",
        "command": ["/bin/sh", "-c", "echo delayed job"]
      },
      "traits": {}
    }
  ],
  "workflow": [
    {
      "name": "create-delay-task",
      "mode": "StepByStep",
      "components": ["delay-task"]
    }
  ]
}
EOF
    trap 'rm -f "$payload"' EXIT
    ;;
  *)
    echo "usage: $0 {instant|cron|delay}" >&2
    exit 1
    ;;
esac

app_resp="$(curl -fsS -X POST "${api_addr}/applications" \
  -H "Content-Type: application/json" \
  -d @"$payload")"

app_id="$(printf '%s' "$app_resp" | python3 -c 'import json,sys; data=json.load(sys.stdin); print(data.get("id",""))')"
workflow_ref="$(printf '%s' "$app_resp" | python3 -c 'import json,sys; data=json.load(sys.stdin); print(data.get("workflowId",""))')"

if [[ -z "$app_id" || -z "$workflow_ref" ]]; then
  echo "failed to parse id/workflowId from response: $app_resp" >&2
  exit 1
fi

curl -fsS -X POST "${api_addr}/applications/${app_id}/workflow/exec" \
  -H "Content-Type: application/json" \
  -d "{\"workflowId\":\"${workflow_ref}\"}" >/dev/null

label="eruun.io/component-name=${component_name}"

if [[ "$resource_kind" == "cronjob" ]]; then
  kubectl -n "$namespace" get cronjob -l "$label"
  exit 0
fi

if [[ "$mode" == "delay" && -n "$start_time" ]]; then
  now="$(date +%s)"
  if (( start_time > now )); then
    sleep $(( start_time - now + 2 ))
  fi
else
  sleep 2
fi

found=""
for _ in $(seq 1 30); do
  found="$(kubectl -n "$namespace" get job -l "$label" -o name | head -n 1 || true)"
  if [[ -n "$found" ]]; then
    break
  fi
  sleep 2
done

if [[ -z "$found" ]]; then
  echo "job not found with label $label" >&2
  exit 1
fi

kubectl -n "$namespace" get job -l "$label"
kubectl -n "$namespace" wait --for=condition=complete job -l "$label" --timeout=300s
