#!/usr/bin/env bash

set -Eeuo pipefail

TEST_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
HELM_BIN="${HELM_BIN:-helm}"

runHelm() {
  local command="$1"
  shift
  "${HELM_BIN}" "${command}" \
    --set-string mysql.rootPassword=helm-template-test \
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

assertUniqueRoleNames() {
  local kind="$1"
  local manifest="$2"
  local names
  local count
  local unique_count
  local name
  names=$(resourceNames "${kind}" "${manifest}")
  count=$(printf '%s\n' "${names}" | awk 'NF { count++ } END { print count + 0 }')
  unique_count=$(printf '%s\n' "${names}" | awk 'NF && !seen[$0]++ { count++ } END { print count + 0 }')

  assertEqual "${count}" "4" "distributed runtime must render four ${kind} names"
  assertEqual "${unique_count}" "4" "long fullnameOverride must preserve unique ${kind} role suffixes"
  while IFS= read -r name; do
    [ -n "${name}" ] || continue
    [ "${#name}" -le 63 ] || fail "${kind} name exceeds 63 characters: ${name}"
    case "${name}" in
      *-api|*-controller|*-scheduler|*-worker) ;;
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

bindingSubjectNames() {
  local manifest="$1"
  awk '
    $1 == "kind:" { inBinding = ($2 == "ClusterRoleBinding"); inSubjects = 0 }
    inBinding && $1 == "subjects:" { inSubjects = 1; next }
    inBinding && inSubjects && $1 == "roleRef:" { inSubjects = 0; next }
    inBinding && inSubjects && $1 == "name:" { print $2 }
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

bindingRoleRefNameFor() {
  local binding_name="$1"
  local manifest="$2"
  awk -v target="${binding_name}" '
    /^kind:/ {
      inBinding = ($2 == "ClusterRoleBinding")
      inMetadata = 0
      selected = 0
      inRoleRef = 0
    }
    inBinding && /^metadata:/ { inMetadata = 1; next }
    inBinding && inMetadata && /^  name:/ {
      selected = ($2 == target)
      inMetadata = 0
      next
    }
    selected && /^roleRef:/ { inRoleRef = 1; next }
    selected && inRoleRef && /^  name:/ { print $2; exit }
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

clusterRoleRuleVerbsFor() {
  local manifest="$1"
  local role_name="$2"
  local api_group="$3"
  local resource="$4"
  awk -v targetRole="${role_name}" -v targetGroup="${api_group}" -v targetResource="${resource}" '
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
    selected && matchingGroup && matchingResource && $1 == "verbs:" {
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
assertEqual "$(resourceNames ClusterRole "${default_manifest}" | wc -l | tr -d ' ')" "2" "RBAC must render resource-manager and controller-observer ClusterRoles"
assertEqual "$(resourceNames ClusterRoleBinding "${default_manifest}" | wc -l | tr -d ' ')" "2" "RBAC must render one binding per ClusterRole"
assertEqual \
  "$(resourceName ClusterRole "${default_manifest}")" \
  "eruun-eruun-eruun-system" \
  "default ClusterRole name must remain stable"
controller_role_name="eruun-eruun-eruun-system-controller-observer"
assertEqual \
  "$(bindingRoleRefNameFor "${controller_role_name}" "${default_manifest}")" \
  "${controller_role_name}" \
  "controller observer binding must reference its narrow ClusterRole"
assertEqual \
  "$(bindingSubjectNamesFor eruun-eruun-eruun-system "${default_manifest}" | sort | tr '\n' ' ' | sed 's/ $//')" \
  "eruun-eruun-api eruun-eruun-worker" \
  "resource manager binding must include only API and Worker"
assertEqual \
  "$(bindingSubjectNamesFor "${controller_role_name}" "${default_manifest}")" \
  "eruun-eruun-controller" \
  "controller observer binding must include only Controller"
assertEqual \
  "$(clusterRoleRuleVerbsFor "${default_manifest}" "${controller_role_name}" "" pods)" \
  "get list watch patch delete" \
  "Controller must observe, label, and clean up completed Job Pods"
assertEqual \
  "$(clusterRoleRuleVerbsFor "${default_manifest}" "${controller_role_name}" "" pods/log)" \
  "get" \
  "ResultDispatcher must collect completed Job logs"
assertEqual \
  "$(clusterRoleRuleVerbsFor "${default_manifest}" "${controller_role_name}" batch jobs)" \
  "get create update delete" \
  "Controller must dispatch delayed Jobs, adopt execution identities, and clean up completed Jobs"
assertEqual \
  "$(clusterRoleRuleVerbsFor "${default_manifest}" "${controller_role_name}" apps replicasets)" \
  "get" \
  "Controller must only read ReplicaSet owners"
assertEqual \
  "$(clusterRoleRuleVerbsFor "${default_manifest}" "${controller_role_name}" "" secrets)" \
  "" \
  "Controller must not receive Secret permissions"
assertEqual \
  "$(clusterRoleRuleVerbsFor "${default_manifest}" "${controller_role_name}" "" pods/exec)" \
  "" \
  "Controller must not receive Pod exec permissions"
assertEqual \
  "$(clusterRoleRuleVerbsFor "${default_manifest}" "${controller_role_name}" apps deployments)" \
  "" \
  "Controller must not manage Deployments"
assertEqual \
  "$(clusterRoleRuleVerbsFor "${default_manifest}" "${controller_role_name}" rbac.authorization.k8s.io roles)" \
  "" \
  "Controller must not manage RBAC roles"
assertEqual \
  "$(clusterRoleRuleVerbs "${default_manifest}" batch jobs)" \
  "get list create update patch delete" \
  "Worker must adopt reusable Jobs into a new execution generation"
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
assertEqual "$(grep -c 'name: ERUUN_IMPORT_SECRET_KEYRING_FILE' "${keyring_manifest}")" "2" "distributed runtime must expose the keyring only to API and Worker"
grep -q 'value: /var/run/secrets/eruun/import-secret-keyring/keyring.json' "${keyring_manifest}" ||
  fail "keyring file environment variable must point at the mounted file"
assertEqual "$(grep -c 'secretName: "eruun-import-keyring"' "${keyring_manifest}")" "2" "distributed runtime must mount the keyring Secret only in API and Worker"
assertEqual "$(grep -c 'name: import-secret-keyring' "${keyring_manifest}")" "4" "API and Worker must each render one keyring volume and volumeMount"
grep -q 'key: "keys.json"' "${keyring_manifest}" ||
  fail "existing keyring Secret key was not rendered"

default_deployment_manifest="${TEST_ROOT}/default-deployment.yaml"
runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system > "${default_deployment_manifest}"
if grep -q 'ERUUN_IMPORT_SECRET_KEYRING_FILE' "${default_deployment_manifest}"; then
  fail "default deployment must not configure an import keyring"
fi
if grep -q 'name: import-secret-keyring' "${default_deployment_manifest}"; then
  fail "default deployment must not mount an import keyring"
fi
assertEqual \
  "$(grep -c 'terminationGracePeriodSeconds: 90' "${default_deployment_manifest}")" \
  "4" \
  "default runtime deployments must use the safe termination grace"

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
  --set-string serviceAccount.name=eruun >/dev/null 2>&1; then
  fail "legacy single-process serviceAccount.name must be rejected"
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
runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system > "${runtime_manifest}"

assertEqual \
  "$(awk '$1 == "kind:" && $2 == "Deployment" { count++ } END { print count + 0 }' "${runtime_manifest}")" \
  "4" \
  "distributed runtime must render one Deployment per runtime role"
assertEqual \
  "$(awk '$1 == "kind:" && $2 == "ServiceAccount" { count++ } END { print count + 0 }' "${runtime_manifest}")" \
  "4" \
  "distributed runtime must render one ServiceAccount per runtime role"
assertEqual \
  "$(awk '$1 == "kind:" && $2 == "PodDisruptionBudget" { count++ } END { print count + 0 }' "${runtime_manifest}")" \
  "4" \
  "distributed runtime must protect every replicated role with a PodDisruptionBudget"
assertEqual \
  "$(grep -c 'terminationGracePeriodSeconds: 90' "${runtime_manifest}")" \
  "4" \
  "distributed runtime termination grace must cover every role deployment"
assertEqual \
  "$(grep -c 'name: ERUUN_EXIT_ON_LOST_LEADER' "${runtime_manifest}")" \
  "4" \
  "distributed runtime roles must remain available as leader-election standbys"
assertEqual \
  "$(grep -c 'startupProbe:' "${runtime_manifest}")" \
  "4" \
  "distributed runtime must protect every role during startup"
assertEqual \
  "$(grep -c 'failureThreshold: 30' "${runtime_manifest}")" \
  "4" \
  "distributed runtime startup probes must allow slow initialization"
assertEqual \
  "$(grep -c 'name: ERUUN_DATASTORE_SCHEMA_MODE' "${runtime_manifest}")" \
  "5" \
  "runtime deployments and migration Job must declare schema ownership"
assertEqual \
  "$(grep -c 'value: \"migrate\"' "${runtime_manifest}")" \
  "1" \
  "initial install must assign schema migration to the API role only"
assertEqual \
  "$(grep -c 'value: \"validate\"' "${runtime_manifest}")" \
  "3" \
  "non-API roles must only validate schema on initial install"

upgrade_manifest="${TEST_ROOT}/upgrade-runtime.yaml"
runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system \
  --is-upgrade > "${upgrade_manifest}"
assertEqual \
  "$(grep -c 'value: \"validate\"' "${upgrade_manifest}")" \
  "4" \
  "all runtime roles must validate schema after the pre-upgrade migration"
grep -q '"helm.sh/hook": pre-upgrade' "${upgrade_manifest}" ||
  fail "schema migration Job must run as a pre-upgrade hook"
grep -q 'value: migrate-only' "${upgrade_manifest}" ||
  fail "schema migration hook must exit after applying migrations"

assertEqual \
  "$(grep -c 'key: datastore-url' "${upgrade_manifest}")" \
  "5" \
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
  "5" \
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
  END { exit failed || count != 5 }
' "${expanded_datastore_manifest}" ||
  fail "runtime deployments and migration Job must define DSN expansion inputs before the override"

assertEqual \
  "$(grep -c 'value: \"eruun-eruun-controller\"' "${runtime_manifest}")" \
  "4" \
  "default controller Lease must derive from the release fullname"
assertEqual \
  "$(grep -c 'value: \"eruun-eruun-scheduler\"' "${runtime_manifest}")" \
  "4" \
  "default scheduler Lease must derive from the release fullname"

isolated_lock_manifest="${TEST_ROOT}/isolated-lock-runtime.yaml"
runHelm template isolated "${TEST_DIR}" \
  --namespace eruun-system \
  --show-only templates/runtime-deployments.yaml > "${isolated_lock_manifest}"
assertEqual \
  "$(grep -c 'value: \"isolated-eruun-controller\"' "${isolated_lock_manifest}")" \
  "4" \
  "controller Lease defaults must be isolated by release fullname"
assertEqual \
  "$(grep -c 'value: \"isolated-eruun-scheduler\"' "${isolated_lock_manifest}")" \
  "4" \
  "scheduler Lease defaults must be isolated by release fullname"

explicit_lock_manifest="${TEST_ROOT}/explicit-lock-runtime.yaml"
runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system \
  --show-only templates/runtime-deployments.yaml \
  --set-string runtime.controllerLockName=shared-controller \
  --set-string runtime.schedulerLockName=shared-scheduler > "${explicit_lock_manifest}"
assertEqual "$(grep -c 'value: \"shared-controller\"' "${explicit_lock_manifest}")" "4" "explicit controller Lease override must be preserved"
assertEqual "$(grep -c 'value: \"shared-scheduler\"' "${explicit_lock_manifest}")" "4" "explicit scheduler Lease override must be preserved"

long_runtime_manifest="${TEST_ROOT}/runtime-long-fullname.yaml"
runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system \
  --set-string "fullnameOverride=$(repeatChar f 63)" > "${long_runtime_manifest}"

for kind in Deployment ServiceAccount PodDisruptionBudget; do
  assertUniqueRoleNames "${kind}" "${long_runtime_manifest}"
done

for role in api controller scheduler worker; do
  grep -q "value: \"${role}\"" "${runtime_manifest}" ||
    fail "distributed runtime must pass ERUUN_ROLE=${role} to its Deployment"
  grep -q "app.kubernetes.io/component: ${role}" "${runtime_manifest}" ||
    fail "distributed runtime must label the ${role} workload"
done

awk '
  $1 == "kind:" { kind = $2; inSelector = 0 }
  kind == "Service" && $1 == "selector:" { inSelector = 1; next }
  kind == "Service" && inSelector && $1 == "app.kubernetes.io/component:" {
    if ($2 == "api") found = 1
    exit
  }
  END { exit found ? 0 : 1 }
' "${runtime_manifest}" || fail "Service must select only API pods"

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
  fail "distributed runtime must require one existing ServiceAccount per role"
fi

external_sa_manifest="${TEST_ROOT}/runtime-external-service-accounts.yaml"
runHelm template eruun "${TEST_DIR}" \
  --namespace eruun-system \
  --show-only templates/serviceaccount-rbac.yaml \
  --set serviceAccount.create=false \
  --set-string serviceAccount.roleNames.api=precreated-api \
  --set-string serviceAccount.roleNames.controller=precreated-controller \
  --set-string serviceAccount.roleNames.scheduler=precreated-scheduler \
  --set-string serviceAccount.roleNames.worker=precreated-worker > "${external_sa_manifest}"

if runHelm template eruun "${TEST_DIR}" --namespace eruun-system --set serviceAccount.create=false --set-string serviceAccount.roleNames.api=shared-runtime --set-string serviceAccount.roleNames.controller=precreated-controller --set-string serviceAccount.roleNames.scheduler=shared-runtime --set-string serviceAccount.roleNames.worker=precreated-worker >/dev/null 2>&1; then
  fail "distributed runtime must reject a Scheduler ServiceAccount shared with a ClusterRole-bound role"
fi
if runHelm template eruun "${TEST_DIR}" --namespace eruun-system --set serviceAccount.create=false --set-string serviceAccount.roleNames.api=shared-runtime --set-string serviceAccount.roleNames.controller=shared-runtime --set-string serviceAccount.roleNames.scheduler=precreated-scheduler --set-string serviceAccount.roleNames.worker=precreated-worker >/dev/null 2>&1; then
  fail "distributed runtime must reject a Controller ServiceAccount shared with the resource-manager role"
fi

assertEqual \
  "$(bindingSubjectNamesFor eruun-eruun-eruun-system "${external_sa_manifest}" | sort | tr '\n' ' ' | sed 's/ $//')" \
  "precreated-api precreated-worker" \
  "resource manager binding must include only the API and Worker ServiceAccounts"
assertEqual \
  "$(bindingSubjectNamesFor "${controller_role_name}" "${external_sa_manifest}")" \
  "precreated-controller" \
  "controller observer binding must include only the Controller ServiceAccount"
assertEqual \
  "$(grep -c 'name: precreated-scheduler' "${external_sa_manifest}")" \
  "1" \
  "Scheduler ServiceAccount must appear only in the namespace-scoped RoleBinding"


printf '%s\n' "Helm template tests passed"
