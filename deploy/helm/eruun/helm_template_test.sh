#!/usr/bin/env bash

set -Eeuo pipefail

TEST_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
HELM_BIN="${HELM_BIN:-helm}"

runHelm() {
  local command="$1"
  shift
  "${HELM_BIN}" "${command}" \
    --set-string mysql.rootPassword=helm-template-test \
    --set-string auth.existingSecret=eruun-account-config \
    --set-string redis.password=helm-template-test "$@"
}
TEST_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/eruun-helm-template-test.XXXXXX")
trap 'rm -rf "${TEST_ROOT}"' EXIT

fail() {
  printf 'FAIL: %s\n' "$1" >&2
  exit 1
}

assertEqual() {
  local actual="$1"
  local expected="$2"
  local message="$3"
  [ "${actual}" = "${expected}" ] || fail "${message}: expected ${expected}, got ${actual}"
}

assertNotEqual() {
  local first="$1"
  local second="$2"
  local message="$3"
  [ "${first}" != "${second}" ] || fail "${message}: both rendered ${first}"
}

repeatChar() {
  local character="$1"
  local count="$2"
  printf '%*s' "${count}" '' | tr ' ' "${character}"
}

renderRBAC() {
  local case_name="$1"
  local release_name="$2"
  local namespace="$3"
  local fullname_override="${4:-}"
  local output="${TEST_ROOT}/${case_name}.yaml"
  local args=(
    template
    "${release_name}"
    "${TEST_DIR}"
    --namespace "${namespace}"
    --show-only templates/serviceaccount-rbac.yaml
  )
  if [ -n "${fullname_override}" ]; then
    args+=(--set-string "fullnameOverride=${fullname_override}")
  fi

  runHelm "${args[@]}" > "${output}"
  printf '%s\n' "${output}"
}

resourceName() {
  local kind="$1"
  local manifest="$2"
  awk -v target="${kind}" '
    $1 == "kind:" { currentKind = $2; inMetadata = 0 }
    currentKind == target && $1 == "metadata:" { inMetadata = 1; next }
    currentKind == target && inMetadata && $1 == "name:" { print $2; exit }
  ' "${manifest}"
}

resourceNames() {
  local kind="$1"
  local manifest="$2"
  awk -v target="${kind}" '
    /^kind:/ { currentKind = $2; inMetadata = 0 }
    currentKind == target && /^metadata:/ { inMetadata = 1; next }
    currentKind == target && inMetadata && /^  name:/ { print $2; inMetadata = 0 }
  ' "${manifest}"
}

assertRuntimeNames() {
  local kind="$1"
  local manifest="$2"
  local names
  local count
  local unique_count
  local name
  names=$(resourceNames "${kind}" "${manifest}")
  count=$(printf '%s\n' "${names}" | awk 'NF { count++ } END { print count + 0 }')
  unique_count=$(printf '%s\n' "${names}" | awk 'NF && !seen[$0]++ { count++ } END { print count + 0 }')

  assertEqual "${count}" "1" "unified runtime must render one ${kind}"
  assertEqual "${unique_count}" "1" "long fullnameOverride must preserve the ${kind} runtime suffix"
  while IFS= read -r name; do
    [ -n "${name}" ] || continue
    [ "${#name}" -le 63 ] || fail "${kind} name exceeds 63 characters: ${name}"
    case "${name}" in
      *-runtime) ;;
      *) fail "${kind} name does not preserve a runtime role suffix: ${name}" ;;
    esac
  done <<< "${names}"
}

bindingRoleRefName() {
  local manifest="$1"
  awk '
    $1 == "kind:" && $2 == "ClusterRoleBinding" { inBinding = 1 }
    inBinding && $1 == "roleRef:" { inRoleRef = 1; next }
    inBinding && inRoleRef && $1 == "name:" { print $2; exit }
  ' "${manifest}"
}

bindingSubjectNamespace() {
  local manifest="$1"
  awk '
    $1 == "kind:" && $2 == "ClusterRoleBinding" { inBinding = 1 }
    inBinding && $1 == "subjects:" { inSubjects = 1; next }
    inBinding && inSubjects && $1 == "namespace:" { print $2; exit }
  ' "${manifest}"
}

bindingSubjectNamesFor() {
  local binding_name="$1"
  local manifest="$2"
  awk -v target="${binding_name}" '
    /^kind:/ {
      inBinding = ($2 == "ClusterRoleBinding")
      inMetadata = 0
      selected = 0
      inSubjects = 0
    }
    inBinding && /^metadata:/ { inMetadata = 1; next }
    inBinding && inMetadata && /^  name:/ {
      selected = ($2 == target)
      inMetadata = 0
      next
    }
    selected && /^subjects:/ { inSubjects = 1; next }
    selected && inSubjects && /^roleRef:/ { inSubjects = 0; next }
    selected && inSubjects && /^    name:/ { print $2 }
  ' "${manifest}"
}

clusterRoleRuleVerbs() {
  local manifest="$1"
  local api_group="$2"
  local resource="$3"
  awk -v targetGroup="${api_group}" -v targetResource="${resource}" '
    function listValue(line, value) {
      value = line
      sub(/^[^[]*\[/, "", value)
      sub(/\][[:space:]]*$/, "", value)
      gsub(/"/, "", value)
      gsub(/,[[:space:]]*/, " ", value)
      return value
    }
    /^kind:/ {
      inClusterRole = ($2 == "ClusterRole")
      matchingGroup = 0
      matchingResource = 0
    }
    inClusterRole && $1 == "-" && $2 == "apiGroups:" {
      matchingGroup = (listValue($0) == targetGroup)
      matchingResource = 0
      next
    }
    inClusterRole && matchingGroup && $1 == "resources:" {
      matchingResource = (listValue($0) == targetResource)
      next
    }
    inClusterRole && matchingGroup && matchingResource && $1 == "verbs:" {
      print listValue($0)
      exit
    }
  ' "${manifest}"
}

clusterRoleRuleFieldFor() {
  local manifest="$1"
  local role_name="$2"
  local api_group="$3"
  local resource="$4"
  awk -v targetRole="${role_name}" -v targetGroup="${api_group}" -v targetResource="${resource}" -v targetField="${5:-verbs}" '
    function listValue(line, value) {
      value = line
      sub(/^[^[]*\[/, "", value)
      sub(/\][[:space:]]*$/, "", value)
      gsub(/"/, "", value)
      gsub(/,[[:space:]]*/, " ", value)
      return value
    }
    /^kind:/ {
      inClusterRole = ($2 == "ClusterRole")
      inMetadata = 0
      selected = 0
      matchingGroup = 0
      matchingResource = 0
    }
    inClusterRole && /^metadata:/ { inMetadata = 1; next }
    inClusterRole && inMetadata && /^  name:/ {
      selected = ($2 == targetRole)
      inMetadata = 0
      next
    }
    selected && $1 == "-" && $2 == "apiGroups:" {
      matchingGroup = (listValue($0) == targetGroup)
      matchingResource = 0
      next
    }
    selected && matchingGroup && $1 == "resources:" {
      matchingResource = (listValue($0) == targetResource)
      next
    }
    selected && matchingGroup && matchingResource && $1 == (targetField ":") {
      print listValue($0)
      exit
    }
  ' "${manifest}"
}

assertRBACClosure() {
  local manifest="$1"
  local namespace="$2"
  local cluster_role_name
  local binding_name
  local role_ref_name
  local subject_namespace
  cluster_role_name=$(resourceName ClusterRole "${manifest}")
  binding_name=$(resourceName ClusterRoleBinding "${manifest}")
  role_ref_name=$(bindingRoleRefName "${manifest}")
  subject_namespace=$(bindingSubjectNamespace "${manifest}")

  [ -n "${cluster_role_name}" ] || fail "ClusterRole name was not rendered"
  assertEqual "${binding_name}" "${cluster_role_name}" "ClusterRoleBinding name must match ClusterRole"
  assertEqual "${role_ref_name}" "${cluster_role_name}" "roleRef.name must match ClusterRole"
  assertEqual "${subject_namespace}" "${namespace}" "binding subject namespace must match release namespace"
}

command -v "${HELM_BIN}" >/dev/null 2>&1 || fail "Helm binary not found: ${HELM_BIN}"
grep -q '^version: 0.1.0$' "${TEST_DIR}/Chart.yaml" ||
  fail "Chart version must match Eruun 0.1.0"
grep -q '^appVersion: "0.1.0"$' "${TEST_DIR}/Chart.yaml" ||
  fail "Chart appVersion must match Eruun 0.1.0"

default_manifest=$(renderRBAC default eruun eruun-system)
assertRBACClosure "${default_manifest}" eruun-system
assertEqual "$(resourceNames ClusterRole "${default_manifest}" | wc -l | tr -d ' ')" "1" "RBAC must render a single runtime ClusterRole"
assertEqual "$(resourceNames ClusterRoleBinding "${default_manifest}" | wc -l | tr -d ' ')" "1" "RBAC must render one runtime ClusterRoleBinding"
assertEqual \
  "$(resourceName ClusterRole "${default_manifest}")" \
  "eruun-eruun-eruun-system" \
  "default ClusterRole name must remain stable"
assertEqual "$(clusterRoleRuleVerbs "${default_manifest}" agents.kruise.io sandboxes)" "get list watch create update patch delete" "runtime retains Sandbox lifecycle permissions"
assertEqual "$(clusterRoleRuleVerbs "${default_manifest}" agents.kruise.io checkpoints)" "get list watch create delete" "runtime retains Checkpoint lifecycle permissions"
assertEqual "$(bindingSubjectNamesFor eruun-eruun-eruun-system "${default_manifest}")" "eruun-eruun-runtime" "all capabilities use the runtime ServiceAccount"
assertEqual "$(clusterRoleRuleFieldFor "${default_manifest}" eruun-eruun-eruun-system "" serviceaccounts resourceNames)" "eruun-runner" "workspace impersonation remains restricted to runner ServiceAccounts"
grep -Fq 'resourceNames: ["eruun-eruun"]' "${default_manifest}" || fail "leader route updates must name the business Service"
assertEqual \
  "$(clusterRoleRuleVerbs "${default_manifest}" batch jobs)" \
  "get list watch create update patch delete" \
  "Worker must observe Jobs and adopt reusable Jobs into a new execution generation"
assertEqual \
  "$(clusterRoleRuleVerbs "${default_manifest}" storage.k8s.io storageclasses)" \
  "get create" \
  "StorageClass rule must grant only cloudjob Get/Create access"
assertEqual \
  "$(clusterRoleRuleVerbs "${default_manifest}" "" pods)" \
  "get list watch patch delete" \
  "Pod rule must permit metadata patch for adopted status coordination"
assertEqual \
  "$(clusterRoleRuleVerbs "${default_manifest}" apps replicasets)" \
  "get list update delete" \
  "ReplicaSet rule must support adopted owner-chain scanning, quiesce, and signed runtime cleanup"
assertEqual \
  "$(clusterRoleRuleVerbs "${default_manifest}" apps controllerrevisions)" \
  "get list delete" \
  "ControllerRevision rule must support signed StatefulSet runtime cleanup"
assertEqual \
  "$(clusterRoleRuleVerbs "${default_manifest}" "" persistentvolumes)" \
  "get list" \
  "PV rule must remain read-only for adopted import reporting"
assertEqual \
  "$(clusterRoleRuleVerbs "${default_manifest}" "" persistentvolumeclaims)" \
  "get list create update patch delete" \
  "PVC rule must permit guarded adopted online expansion"
assertEqual \
  "$(clusterRoleRuleVerbs "${default_manifest}" autoscaling horizontalpodautoscalers)" \
  "get list" \
  "HPA rule must support adopted import conflict detection"
assertEqual \
  "$(clusterRoleRuleVerbs "${default_manifest}" policy poddisruptionbudgets)" \
  "get list create update delete" \
  "PDB rule must support adopted source-aware reconciliation and fingerprinted cleanup"
assertEqual \
  "$(clusterRoleRuleVerbs "${default_manifest}" networking.k8s.io networkpolicies)" \
  "get list create update delete" \
  "NetworkPolicy rule must support adopted source-aware reconciliation and fingerprinted cleanup"

long_release=$(repeatChar a 51)
long_alpha_manifest=$(renderRBAC long-alpha "${long_release}" team-alpha)
long_beta_manifest=$(renderRBAC long-beta "${long_release}" team-beta)
assertRBACClosure "${long_alpha_manifest}" team-alpha
assertRBACClosure "${long_beta_manifest}" team-beta
long_alpha_name=$(resourceName ClusterRole "${long_alpha_manifest}")
long_beta_name=$(resourceName ClusterRole "${long_beta_manifest}")
assertEqual "${long_alpha_name}" "${long_release}-eruun-team-alpha" "long release must preserve team-alpha"
assertEqual "${long_beta_name}" "${long_release}-eruun-team-beta" "long release must preserve team-beta"
assertNotEqual "${long_alpha_name}" "${long_beta_name}" "long release names must remain namespace-isolated"

long_override=$(repeatChar f 63)
override_alpha_manifest=$(renderRBAC override-alpha override-test override-alpha "${long_override}")
override_beta_manifest=$(renderRBAC override-beta override-test override-beta "${long_override}")
assertRBACClosure "${override_alpha_manifest}" override-alpha
assertRBACClosure "${override_beta_manifest}" override-beta
override_alpha_name=$(resourceName ClusterRole "${override_alpha_manifest}")
override_beta_name=$(resourceName ClusterRole "${override_beta_manifest}")
assertEqual "${override_alpha_name}" "${long_override}-override-alpha" "fullnameOverride must preserve override-alpha"
assertEqual "${override_beta_name}" "${long_override}-override-beta" "fullnameOverride must preserve override-beta"
assertNotEqual "${override_alpha_name}" "${override_beta_name}" "fullnameOverride names must remain namespace-isolated"

max_namespace=$(repeatChar n 63)
max_namespace_manifest=$(renderRBAC max-namespace max-namespace "${max_namespace}" "${long_override}")
assertRBACClosure "${max_namespace_manifest}" "${max_namespace}"
max_namespace_name=$(resourceName ClusterRole "${max_namespace_manifest}")
assertEqual "${max_namespace_name}" "${long_override}-${max_namespace}" "maximum namespace must be preserved"
assertEqual "${#max_namespace_name}" "127" "maximum combined RBAC name length"


serviceName() {
  local release_name="$1"
  local fullname_override="$2"
  local output="${TEST_ROOT}/service-${release_name}.yaml"
  local args=(
    template
    "${release_name}"
    "${TEST_DIR}"
    --show-only templates/eruun-service.yaml
  )
  if [ -n "${fullname_override}" ]; then
    args+=(--set-string "fullnameOverride=${fullname_override}")
  fi
  runHelm "${args[@]}" > "${output}"
  resourceName Service "${output}"
}

assertEqual   "$(serviceName eruun "$(repeatChar f 70)")"   "$(repeatChar f 63)"   "long fullnameOverride must truncate Service names"
long_workload_release=$(repeatChar r 53)
assertEqual   "$(serviceName "${long_workload_release}" "")"   "${long_workload_release}-eruun"   "maximum valid release name must render a valid Service name"
assertEqual   "$(serviceName eruun "$(repeatChar t 62)-suffix")"   "$(repeatChar t 62)"   "truncated trailing hyphen must be removed from Service names"

keyring_manifest="${TEST_ROOT}/keyring-deployment.yaml"
runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system \
  --show-only templates/runtime-deployments.yaml \
  --set-string importSecretKeyring.existingSecret=eruun-import-keyring \
  --set-string importSecretKeyring.key=keys.json > "${keyring_manifest}"
assertEqual "$(grep -c 'name: ERUUN_IMPORT_SECRET_KEYRING_FILE' "${keyring_manifest}")" "1" "runtime must expose the import keyring"
grep -q 'value: /var/run/secrets/eruun/import-secret-keyring/keyring.json' "${keyring_manifest}" ||
  fail "keyring file environment variable must point at the mounted file"
assertEqual "$(grep -c 'secretName: "eruun-import-keyring"' "${keyring_manifest}")" "1" "runtime must mount the import keyring"
assertEqual "$(grep -c 'name: import-secret-keyring' "${keyring_manifest}")" "2" "runtime must render one keyring volume and volumeMount"
grep -q 'key: "keys.json"' "${keyring_manifest}" ||
  fail "existing keyring Secret key was not rendered"

default_deployment_manifest="${TEST_ROOT}/default-deployment.yaml"
runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system > "${default_deployment_manifest}"
if grep -Eq 'name: ERUUN_(DATASTORE|CACHE)_TYPE' "${default_deployment_manifest}"; then
  fail "fixed MySQL and Redis backends must not render type selectors"
fi
assertEqual "$(grep -c 'name: ERUUN_GRPC_BIND_ADDR' "${default_deployment_manifest}")" "1" "runtime must configure the gRPC listener"
grep -q 'value: "0.0.0.0:9000"' "${default_deployment_manifest}" ||
  fail "runtime gRPC listener must bind the Pod network interface"
assertEqual "$(grep -c 'containerPort: 9000' "${default_deployment_manifest}")" "1" "runtime must expose a gRPC container port"
grep -A 2 'name: grpc' "${default_deployment_manifest}" | grep -q 'port: 9000' ||
  fail "ClusterIP Service must publish the gRPC port"
assertEqual "$(grep -c 'port: http' "${default_deployment_manifest}")" "3" "all runtime HTTP probes must remain on the HTTP port"
assertEqual \
  "$(grep -c '"helm.sh/resource-policy": keep' "${default_deployment_manifest}")" \
  "2" \
  "bundled datastore credential Secrets must be retained with their persistent volumes"
grep -q 'service-port:' "${default_deployment_manifest}" ||
  fail "retained Redis credentials must preserve the immutable service port"
if grep -q 'ERUUN_IMPORT_SECRET_KEYRING_FILE' "${default_deployment_manifest}"; then
  fail "default deployment must not configure an import keyring"
fi
if grep -q 'name: import-secret-keyring' "${default_deployment_manifest}"; then
  fail "default deployment must not mount an import keyring"
fi
assertEqual \
  "$(grep -c 'terminationGracePeriodSeconds: 90' "${default_deployment_manifest}")" \
  "1" \
  "default runtime deployments must use the safe termination grace"

custom_dependency_ports_manifest="${TEST_ROOT}/custom-dependency-ports.yaml"
runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system \
  --set mysql.servicePort=13306 \
  --set redis.servicePort=16379 > "${custom_dependency_ports_manifest}"
grep -q 'args: \["--port=13306"\]' "${custom_dependency_ports_manifest}" ||
  fail "bundled MySQL must listen on mysql.servicePort"
assertEqual \
  "$(grep -c 'mysqladmin ping -h 127.0.0.1 -P 13306' "${custom_dependency_ports_manifest}")" \
  "2" \
  "bundled MySQL probes must use mysql.servicePort"
grep -q 'exec redis-server --port 16379 ' "${custom_dependency_ports_manifest}" ||
  fail "bundled Redis must listen on redis.servicePort"
assertEqual \
  "$(grep -c 'redis-cli -h 127.0.0.1 -p 16379' "${custom_dependency_ports_manifest}")" \
  "2" \
  "bundled Redis probes must use redis.servicePort"

if runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system \
  --show-only templates/runtime-deployments.yaml \
  --set-string importSecretKeyring.existingSecret=eruun-import-keyring \
  --set-string importSecretKeyring.key= >/dev/null 2>&1; then
  fail "configured keyring Secret must require a non-empty key"
fi

runtime_manifest="${TEST_ROOT}/runtime.yaml"
if runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system \
  --set-string runtime.mode=all >/dev/null 2>&1; then
  fail "legacy runtime.mode must be rejected"
fi
if runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system \
  --set runtime.split.enabled=true >/dev/null 2>&1; then
  fail "legacy runtime.split must be rejected"
fi
if runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system \
  --set-string serviceAccount.roleNames.api=eruun >/dev/null 2>&1; then
  fail "removed serviceAccount.roleNames must be rejected"
fi
if runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system \
  --set replicaCount=7 >/dev/null 2>&1; then
  fail "legacy top-level replicaCount must be rejected"
fi
if runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system \
  --set resources.limits.cpu=1 >/dev/null 2>&1; then
  fail "legacy top-level resources must be rejected"
fi
if runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system \
  --set-string "env[0].name=ERUUN_ROLE" \
  --set-string "env[0].value=worker" >/dev/null 2>&1; then
  fail "env must not override Chart-managed ERUUN_ROLE"
fi
if runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system \
  --set-string "env[0].name=ERUUN_ID" \
  --set-string "env[0].value=shared-worker" >/dev/null 2>&1; then
  fail "env must not override Chart-managed ERUUN_ID"
fi
if runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system \
  --set-string "env[0].name=ERUUN_EXIT_ON_LOST_LEADER" \
  --set-string "env[0].value=true" >/dev/null 2>&1; then
  fail "env must not override Chart-managed leader-loss behavior"
fi
if runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system \
  --set-string "env[0].name=ERUUN_WORKFLOW_WORKER_DRAIN_TIMEOUT" \
  --set-string "env[0].value=300s" >/dev/null 2>&1; then
  fail "env must not override Chart-managed worker drain timeout"
fi
if runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system \
  --set-string "env[0].name=ERUUN_DATASTORE_SCHEMA_MODE" \
  --set-string "env[0].value=migrate" >/dev/null 2>&1; then
  fail "env must not override Chart-managed datastore schema mode"
fi
if runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system \
  --set-string "env[0].name=ERUUN_GRPC_BIND_ADDR" \
  --set-string "env[0].value=0.0.0.0:9001" >/dev/null 2>&1; then
  fail "env must not override Chart-managed gRPC bind address"
fi
if runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system \
  --set service.grpcPort=8000 >/dev/null 2>&1; then
  fail "gRPC and HTTP Service ports must differ"
fi
runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system > "${runtime_manifest}"

for kind in Deployment ServiceAccount PodDisruptionBudget; do
  assertEqual "$(resourceNames "${kind}" "${runtime_manifest}" | wc -l | tr -d ' ')" "1" "runtime must render one ${kind}"
done
for pattern in 'terminationGracePeriodSeconds: 90' 'startupProbe:' 'failureThreshold: 30' 'replicas: 4'; do
  assertEqual "$(grep -c "${pattern}" "${runtime_manifest}")" "1" "runtime must render ${pattern}"
done
if grep -Eq 'ERUUN_ROLE|ERUUN_EXIT_ON_LOST_LEADER|ERUUN_CONTROLLER_LOCK_NAME|ERUUN_SCHEDULER_LOCK_NAME' "${runtime_manifest}"; then
  fail "runtime must not configure removed roles or separate leaders"
fi
assertEqual "$(grep -c 'name: ERUUN_DATASTORE_SCHEMA_MODE' "${runtime_manifest}")" "2" "runtime and migration Job must declare schema handling"
assertEqual "$(grep -c 'value: "migrate"' "${runtime_manifest}")" "1" "runtime nodes migrate under the database lock on initial install"

upgrade_manifest="${TEST_ROOT}/upgrade-runtime.yaml"
runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system \
  --is-upgrade > "${upgrade_manifest}"
assertEqual \
  "$(grep -c 'value: \"validate\"' "${upgrade_manifest}")" \
  "1" \
  "all runtime nodes must validate schema after the pre-upgrade migration"
grep -q '"helm.sh/hook": pre-upgrade' "${upgrade_manifest}" ||
  fail "schema migration Job must run as a pre-upgrade hook"
grep -q 'value: migrate-only' "${upgrade_manifest}" ||
  fail "schema migration hook must exit after applying migrations"

assertEqual \
  "$(grep -c 'key: datastore-url' "${upgrade_manifest}")" \
  "2" \
  "runtime deployments and migration Job must default to the same datastore Secret"

external_datastore_manifest="${TEST_ROOT}/upgrade-external-datastore.yaml"
external_datastore_url='review:example@tcp(external-db.example:3306)/eruun?parseTime=true'
runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system \
  --is-upgrade \
  --set-string 'env[0].name=ERUUN_DATASTORE_URL' \
  --set-string "env[0].value=${external_datastore_url}" > "${external_datastore_manifest}"
assertEqual \
  "$(grep -Fc "value: \"${external_datastore_url}\"" "${external_datastore_manifest}")" \
  "2" \
  "all runtime deployments and migration Job must use the external datastore override"

expanded_datastore_manifest="${TEST_ROOT}/upgrade-expanded-datastore.yaml"
expanded_datastore_url='root:$(MYSQL_PASSWORD)@tcp($(EXTERNAL_MYSQL_HOST):3306)/$(MYSQL_DATABASE)?parseTime=true'
runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system \
  --is-upgrade \
  --set-string 'env[0].name=EXTERNAL_MYSQL_HOST' \
  --set-string 'env[0].value=external-db.example' \
  --set-string 'env[1].name=ERUUN_DATASTORE_URL' \
  --set-string "env[1].value=${expanded_datastore_url}" > "${expanded_datastore_manifest}"
awk -v expectedURL="${expanded_datastore_url}" '
  /^kind:/ { kind = $2; passwordReady = 0; databaseReady = 0; hostReady = 0 }
  kind != "Deployment" && kind != "Job" { next }
  $1 == "-" && $2 == "name:" { env = $3 }
  env == "MYSQL_PASSWORD" && $1 == "key:" && $2 == "password" { passwordReady = 1 }
  env == "MYSQL_DATABASE" && $1 == "value:" && $2 == "\"eruun\"" { databaseReady = 1 }
  env == "EXTERNAL_MYSQL_HOST" && $1 == "value:" && $2 == "\"external-db.example\"" { hostReady = 1 }
  env == "ERUUN_DATASTORE_URL" && $1 == "value:" {
    value = $0
    sub(/^[[:space:]]*value: "/, "", value)
    sub(/"$/, "", value)
    if (value != expectedURL || !passwordReady || !databaseReady || !hostReady) failed = 1
    count++
  }
  END { exit failed || count != 2 }
' "${expanded_datastore_manifest}" ||
  fail "runtime deployments and migration Job must define DSN expansion inputs before the override"

assertEqual "$(grep -c 'value: "eruun-eruun-runtime"' "${runtime_manifest}")" "1" "default Lease derives from the release fullname"
assertEqual "$(grep -c 'name: ERUUN_LEADER_SERVICE_NAME' "${runtime_manifest}")" "1" "runtime must publish its leader through the API Service"
assertEqual "$(grep -c 'name: ERUUN_POD_NAME' "${runtime_manifest}")" "1" "runtime must bind Pod identity independently of its unique Lease identity"
if grep -q 'name: ERUUN_ID' "${runtime_manifest}"; then
  fail "runtime must retain a fresh process UUID across Pod container restarts"
fi
isolated_lock_manifest="${TEST_ROOT}/isolated-lock-runtime.yaml"
runHelm template isolated "${TEST_DIR}" --namespace eruun-system --show-only templates/runtime-deployments.yaml > "${isolated_lock_manifest}"
assertEqual "$(grep -c 'value: "isolated-eruun-runtime"' "${isolated_lock_manifest}")" "1" "Lease defaults must be isolated by release fullname"
explicit_lock_manifest="${TEST_ROOT}/explicit-lock-runtime.yaml"
runHelm template eruun "${TEST_DIR}" --namespace eruun-system --show-only templates/runtime-deployments.yaml --set-string runtime.leaderLockName=shared-runtime > "${explicit_lock_manifest}"
assertEqual "$(grep -c 'value: "shared-runtime"' "${explicit_lock_manifest}")" "1" "explicit unified Lease override must be preserved"
for removed in runtime.controllerLockName=old runtime.schedulerLockName=old runtime.roles.worker.replicas=3; do
  if runHelm template eruun "${TEST_DIR}" --set "${removed}" >/dev/null 2>&1; then
    fail "removed runtime values must be rejected: ${removed}"
  fi
done
if runHelm template eruun "${TEST_DIR}" --set runtime.replicas=1 >/dev/null 2>&1; then
  fail "runtime must retain at least one Worker alongside the Leader"
fi
for managed in ERUUN_POD_NAME ERUUN_BIND_ADDR ERUUN_LEADER_LOCK_NAME ERUUN_LEADER_NAMESPACE ERUUN_LEADER_SERVICE_NAME ERUUN_CONTROLLER_LOCK_NAME ERUUN_SCHEDULER_LOCK_NAME; do
  if runHelm template eruun "${TEST_DIR}" --set-string "env[0].name=${managed}" --set-string 'env[0].value=override' >/dev/null 2>&1; then
    fail "runtime must reject unsafe environment override: ${managed}"
  fi
done

long_runtime_manifest="${TEST_ROOT}/runtime-long-fullname.yaml"
runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system \
  --set-string "fullnameOverride=$(repeatChar f 63)" > "${long_runtime_manifest}"

for kind in Deployment ServiceAccount PodDisruptionBudget; do
  assertRuntimeNames "${kind}" "${long_runtime_manifest}"
done

for kind in Service StatefulSet Secret; do
  names=$(resourceNames "${kind}" "${long_runtime_manifest}")
  count=$(printf '%s\n' "${names}" | awk 'NF { count++ } END { print count + 0 }')
  unique_count=$(printf '%s\n' "${names}" | awk 'NF && !seen[$0]++ { count++ } END { print count + 0 }')
  assertEqual "${unique_count}" "${count}" "long fullnameOverride must keep ${kind} names unique"
  while IFS= read -r name; do
    [ -n "${name}" ] || continue
    [ "${#name}" -le 63 ] || fail "${kind} name exceeds 63 characters: ${name}"
  done <<< "${names}"
done
grep -q -- '-mysql$' <<< "$(resourceNames StatefulSet "${long_runtime_manifest}")" ||
  fail "long fullnameOverride must preserve the MySQL suffix"
grep -q -- '-redis$' <<< "$(resourceNames StatefulSet "${long_runtime_manifest}")" ||
  fail "long fullnameOverride must preserve the Redis suffix"

service_manifest="${TEST_ROOT}/leader-service.yaml"
runHelm template eruun "${TEST_DIR}" --show-only templates/eruun-service.yaml > "${service_manifest}"
grep -q 'app.kubernetes.io/component: runtime' "${service_manifest}" || fail "Service must select runtime Pods"
grep -q 'eruun.io/runtime-id: unassigned' "${service_manifest}" || fail "Service must reject all Pods until the Leader publishes its identity"

if runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system \
  --set runtime.workerDrainTimeoutSeconds=120 \
  --set runtime.terminationGracePeriodSeconds=90 >/dev/null 2>&1; then
  fail "termination grace must be greater than worker drain timeout"
fi
if runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system \
  --set runtime.workerDrainTimeoutSeconds=90 \
  --set runtime.terminationGracePeriodSeconds=90 >/dev/null 2>&1; then
  fail "termination grace must not equal worker drain timeout"
fi
runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system \
  --set runtime.workerDrainTimeoutSeconds=89 \
  --set runtime.terminationGracePeriodSeconds=90 >/dev/null

if runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system \
  --set serviceAccount.create=false >/dev/null 2>&1; then
  fail "distributed runtime must require an existing runtime ServiceAccount name"
fi

external_sa_manifest="${TEST_ROOT}/runtime-external-service-account.yaml"
runHelm template eruun "${TEST_DIR}" --namespace eruun-system --show-only templates/serviceaccount-rbac.yaml --set serviceAccount.create=false --set-string serviceAccount.name=precreated-runtime > "${external_sa_manifest}"
assertEqual "$(bindingSubjectNamesFor eruun-eruun-eruun-system "${external_sa_manifest}")" "precreated-runtime" "external runtime ServiceAccount must receive the runtime permissions"
assertEqual "$(grep -c 'name: precreated-runtime' "${external_sa_manifest}")" "2" "external runtime ServiceAccount must appear in namespace and cluster bindings"

for bad_secret in '' '__REPLACE_WITH_ACCOUNT_SECRET__'; do
  if runHelm template account-auth "${TEST_DIR}" --set-string "auth.existingSecret=${bad_secret}" > "${TEST_ROOT}/bad-auth.yaml" 2>&1; then
    fail "empty or placeholder account Secret must be rejected"
  fi
done
if runHelm template account-auth "${TEST_DIR}" --set-string auth.key= > "${TEST_ROOT}/bad-auth-key.yaml" 2>&1; then
  fail "empty account Secret key must be rejected"
fi
runHelm template account-auth "${TEST_DIR}" --show-only templates/runtime-deployments.yaml > "${TEST_ROOT}/account-auth.yaml"
assertEqual "$(grep -c 'name: ERUUN_AUTH_CONFIG_FILE' "${TEST_ROOT}/account-auth.yaml")" "1" "all nodes require account config"
assertEqual "$(grep -c 'fsGroup: 1000' "${TEST_ROOT}/account-auth.yaml")" "1" "non-root runtime must be able to read the Secret"
assertEqual "$(grep -c 'defaultMode: 0440' "${TEST_ROOT}/account-auth.yaml")" "1" "account Secret must have restrictive group-readable permissions"
assertEqual "$(grep -c 'secretName: "eruun-account-config"' "${TEST_ROOT}/account-auth.yaml")" "1" "all nodes mount the shared account Secret"

# The bundled static manifest must carry the same unified runtime and RBAC closure.
static_manifest="${TEST_DIR}/../../eruun-stack.yaml"
for kind in Deployment ServiceAccount PodDisruptionBudget ClusterRole ClusterRoleBinding Role RoleBinding; do
  assertEqual "$(resourceNames "${kind}" "${static_manifest}" | wc -l | tr -d ' ')" "1" "static manifest must render one ${kind}"
done
assertEqual "$(bindingSubjectNamesFor eruun-platform-runtime "${static_manifest}")" "eruun-runtime" "static cluster permissions must bind only the runtime identity"
assertEqual "$(bindingRoleRefName "${static_manifest}")" "eruun-platform-runtime" "static binding must target the runtime ClusterRole"
assertEqual "$(clusterRoleRuleVerbs "${static_manifest}" agents.kruise.io checkpoints)" "get list watch create delete" "static runtime must retain checkpoint lifecycle access"
grep -q 'eruun.io/runtime-id: unassigned' "${static_manifest}" || fail "static Service must wait for leader identity"
if grep -Eq 'name: eruun-(api|controller|scheduler|worker)$|ERUUN_ROLE|ERUUN_CONTROLLER_LOCK_NAME|ERUUN_SCHEDULER_LOCK_NAME' "${static_manifest}"; then
  fail "static runtime must not retain removed role configuration or identities"
fi

printf '%s\n' "Helm template tests passed"
