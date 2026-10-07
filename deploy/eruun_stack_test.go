package deploy_test

import (
	"errors"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/yaml"
)

func TestEruunStackManifestUsesExplicitRBACBoundaries(t *testing.T) {
	manifest, err := os.Open("eruun-stack.yaml")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, manifest.Close())
	})

	decoder := yaml.NewYAMLOrJSONDecoder(manifest, 4096)
	clusterRoles := map[string]struct{}{}
	clusterRoleBindings := map[string]map[string]interface{}{}
	secrets := 0
	runtimeFlagsConfigMaps := 0

	for {
		var object map[string]interface{}
		err = decoder.Decode(&object)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		if len(object) == 0 {
			continue
		}

		metadata, ok := object["metadata"].(map[string]interface{})
		require.True(t, ok, "manifest object must contain metadata")
		name, _ := metadata["name"].(string)
		kind, _ := object["kind"].(string)
		apiVersion, _ := object["apiVersion"].(string)

		if apiVersion == "rbac.authorization.k8s.io/v1" && kind == "ClusterRole" {
			clusterRoles[name] = struct{}{}
		}
		if apiVersion == "rbac.authorization.k8s.io/v1" && kind == "ClusterRoleBinding" {
			clusterRoleBindings[name] = object
		}
		if apiVersion == "v1" && kind == "ConfigMap" && name == "eruun-flags" {
			runtimeFlagsConfigMaps++
			data, ok := object["data"].(map[string]interface{})
			require.True(t, ok, "runtime flags ConfigMap must contain data")
			require.Equal(t, "eruun-runtime", data["ERUUN_LEADER_LOCK_NAME"])
			require.Equal(t, "eruun", data["ERUUN_LEADER_SERVICE_NAME"])
			require.Equal(t, "15s", data["ERUUN_DURATION"])
			require.NotContains(t, data, "ERUUN_CONTROLLER_LOCK_NAME")
			require.NotContains(t, data, "ERUUN_SCHEDULER_LOCK_NAME")
			require.NotContains(t, data, "ERUUN_LOCK_NAME")
			require.NotContains(t, data, "ERUUN_DATASTORE_DATABASE")
		}
		if apiVersion == "v1" && kind == "Secret" {
			secrets++
		}
	}

	require.Equal(t, map[string]struct{}{
		"eruun-platform-runtime": {},
	}, clusterRoles)
	require.Len(t, clusterRoleBindings, 1)
	for name, binding := range clusterRoleBindings {
		roleName, found, nestedErr := unstructured.NestedString(binding, "roleRef", "name")
		require.NoError(t, nestedErr)
		require.True(t, found, "%s must contain roleRef.name", name)
		require.NotEqual(t, "cluster-admin", roleName)
	}
	require.Equal(t, 0, secrets)
	require.Equal(t, 1, runtimeFlagsConfigMaps)
}

func TestEruunStackUsesUnifiedDistributedRuntime(t *testing.T) {
	manifest, err := os.Open("eruun-stack.yaml")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, manifest.Close())
	})

	decoder := yaml.NewYAMLOrJSONDecoder(manifest, 4096)
	deployments := map[string]map[string]interface{}{}
	serviceAccounts := map[string]struct{}{}
	pdbs := map[string]struct{}{}
	var service map[string]interface{}
	var flags map[string]interface{}
	var leaderBinding map[string]interface{}
	clusterRoles := map[string]map[string]interface{}{}
	clusterBindings := map[string]map[string]interface{}{}

	for {
		var object map[string]interface{}
		err = decoder.Decode(&object)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		if len(object) == 0 {
			continue
		}
		kind, _, _ := unstructured.NestedString(object, "kind")
		name, _, _ := unstructured.NestedString(object, "metadata", "name")
		switch kind {
		case "Deployment":
			component, found, _ := unstructured.NestedString(object, "metadata", "labels", "app.kubernetes.io/component")
			if found {
				deployments[component] = object
			}
		case "ServiceAccount":
			serviceAccounts[name] = struct{}{}
		case "PodDisruptionBudget":
			pdbs[name] = struct{}{}
		case "Service":
			if name == "eruun" {
				service = object
			}
		case "ConfigMap":
			if name == "eruun-flags" {
				flags = object
			}
		case "RoleBinding":
			if name == "eruun-leader-election" {
				leaderBinding = object
			}
		case "ClusterRoleBinding":
			clusterBindings[name] = object
		case "ClusterRole":
			clusterRoles[name] = object
		}
	}

	require.Len(t, deployments, 1)
	require.Equal(t, map[string]struct{}{"eruun-runtime": {}}, serviceAccounts)
	require.Equal(t, map[string]struct{}{"eruun-runtime": {}}, pdbs)
	deployment, found := deployments["runtime"]
	require.True(t, found, "missing unified runtime Deployment")
	name, _, _ := unstructured.NestedString(deployment, "metadata", "name")
	require.Equal(t, "eruun-runtime", name)
	require.Equal(t, int64(4), stackNumberValue(t, deployment, "spec", "replicas"))
	serviceAccountName, found, err := unstructured.NestedString(deployment, "spec", "template", "spec", "serviceAccountName")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "eruun-runtime", serviceAccountName)
	require.Equal(t, int64(90), stackNumberValue(t, deployment, "spec", "template", "spec", "terminationGracePeriodSeconds"))

	containers, found, err := unstructured.NestedSlice(deployment, "spec", "template", "spec", "containers")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, containers, 1)
	container, ok := containers[0].(map[string]interface{})
	require.True(t, ok)
	env, found, err := unstructured.NestedSlice(container, "env")
	require.NoError(t, err)
	require.True(t, found)
	podNameFound := false
	for _, item := range env {
		entry, ok := item.(map[string]interface{})
		require.True(t, ok)
		require.NotEqual(t, "ERUUN_ROLE", entry["name"])
		require.NotEqual(t, "ERUUN_ID", entry["name"], "election identity must default to a fresh process UUID")
		if entry["name"] == "ERUUN_POD_NAME" {
			podNameFound = true
			fieldPath, _, err := unstructured.NestedString(entry, "valueFrom", "fieldRef", "fieldPath")
			require.NoError(t, err)
			require.Equal(t, "metadata.name", fieldPath)
		}
	}
	require.True(t, podNameFound, "runtime must locate its Pod independently of its election identity")

	selector, found, err := unstructured.NestedString(service, "spec", "selector", "app.kubernetes.io/component")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "runtime", selector)
	identity, found, err := unstructured.NestedString(service, "spec", "selector", "eruun.io/runtime-id")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "unassigned", identity)

	data, found, err := unstructured.NestedStringMap(flags, "data")
	require.NoError(t, err)
	require.True(t, found)
	for _, key := range []string{
		"ERUUN_LEADER_LOCK_NAME",
		"ERUUN_LEADER_SERVICE_NAME",
		"ERUUN_WORKFLOW_HEARTBEAT_INTERVAL",
		"ERUUN_WORKFLOW_LEASE_DURATION",
		"ERUUN_WORKFLOW_LEASE_REAPER_INTERVAL",
		"ERUUN_WORKFLOW_WORKER_DRAIN_TIMEOUT",
	} {
		require.NotEmpty(t, data[key], "missing %s", key)
	}
	require.NotContains(t, data, "ERUUN_WORKFLOW_LEASE_FENCING_ENABLED")
	require.NotContains(t, data, "ERUUN_LOCK_NAME")

	require.Equal(t, []string{"eruun-runtime"}, stackSubjectNames(t, leaderBinding))
	require.Equal(t, []string{"eruun-runtime"}, stackSubjectNames(t, clusterBindings["eruun-platform-runtime"]))

	roleRefName := func(binding map[string]interface{}) string {
		name, found, err := unstructured.NestedString(binding, "roleRef", "name")
		require.NoError(t, err)
		require.True(t, found)
		return name
	}
	require.Equal(t, "eruun-platform-runtime", roleRefName(clusterBindings["eruun-platform-runtime"]))
	require.Equal(t, "eruun-leader-election", roleRefName(leaderBinding))

	assertUnifiedRuntimePermissions(t, clusterRoles)
}

func assertUnifiedRuntimePermissions(t *testing.T, clusterRoles map[string]map[string]interface{}) {
	t.Helper()
	verbsFor := func(role map[string]interface{}, apiGroup, resource string) []string {
		rules, found, err := unstructured.NestedSlice(role, "rules")
		require.NoError(t, err)
		require.True(t, found)
		for _, rawRule := range rules {
			rule, ok := rawRule.(map[string]interface{})
			require.True(t, ok)
			apiGroups, _, err := unstructured.NestedStringSlice(rule, "apiGroups")
			require.NoError(t, err)
			resources, _, err := unstructured.NestedStringSlice(rule, "resources")
			require.NoError(t, err)
			if len(apiGroups) != 1 || apiGroups[0] != apiGroup {
				continue
			}
			for _, candidate := range resources {
				if candidate == resource {
					verbs, _, err := unstructured.NestedStringSlice(rule, "verbs")
					require.NoError(t, err)
					return verbs
				}
			}
		}
		return nil
	}
	runtimeRole := clusterRoles["eruun-platform-runtime"]
	require.ElementsMatch(t, []string{"get", "create", "update", "delete"}, verbsFor(runtimeRole, "", "namespaces"), "runtime must retain workspace lifecycle permissions")
	require.Equal(t, []string{"impersonate"}, verbsFor(runtimeRole, "", "serviceaccounts"))
	rules, _, err := unstructured.NestedSlice(runtimeRole, "rules")
	require.NoError(t, err)
	for _, item := range rules {
		rule := item.(map[string]interface{})
		verbs, _, err := unstructured.NestedStringSlice(rule, "verbs")
		require.NoError(t, err)
		for _, verb := range verbs {
			if verb == "impersonate" {
				names, _, err := unstructured.NestedStringSlice(rule, "resourceNames")
				require.NoError(t, err)
				require.Equal(t, []string{"eruun-runner"}, names)
			}
		}
	}
	require.ElementsMatch(t, []string{"get", "list", "watch", "patch", "delete"}, verbsFor(runtimeRole, "", "pods"), "Controller must observe, label, and clean up completed Job Pods")
	require.Equal(t, []string{"get"}, verbsFor(runtimeRole, "", "pods/log"), "ResultDispatcher must collect completed Job logs")
	require.ElementsMatch(t, []string{"get", "list", "watch", "create", "update", "patch", "delete"}, verbsFor(runtimeRole, "batch", "jobs"), "runtime must dispatch, observe, adopt and clean up executions")
	require.ElementsMatch(t, []string{"get", "list", "create", "update", "patch", "delete"}, verbsFor(runtimeRole, "batch", "cronjobs"))
	require.ElementsMatch(t, []string{"get", "list", "update", "delete"}, verbsFor(runtimeRole, "apps", "replicasets"))
	require.ElementsMatch(t, []string{"get", "list", "create", "update", "patch", "delete"}, verbsFor(runtimeRole, "", "secrets"))
	require.Equal(t, []string{"create"}, verbsFor(runtimeRole, "", "pods/exec"))
	require.ElementsMatch(t, []string{"get", "list", "create", "update", "patch", "delete"}, verbsFor(runtimeRole, "apps", "deployments"))
	for _, resource := range []string{"roles", "clusterroles"} {
		require.ElementsMatch(t, []string{"get", "list", "create", "update", "patch", "delete", "bind", "escalate"}, verbsFor(runtimeRole, "rbac.authorization.k8s.io", resource))
	}
	require.ElementsMatch(t, []string{"get", "list", "watch", "create", "update", "patch", "delete"}, verbsFor(runtimeRole, "agents.kruise.io", "sandboxes"))
	require.ElementsMatch(t, []string{"get", "list", "watch", "create", "delete"}, verbsFor(runtimeRole, "agents.kruise.io", "checkpoints"))
}

func TestDockerBuildUsesDefaultDeploymentImageWithoutPublishing(t *testing.T) {
	makefile, err := os.ReadFile("../Makefile")
	require.NoError(t, err)

	require.Contains(t, string(makefile), "IMAGE           ?= ghcr.io/pixelcores/eruun:0.1.0")
	require.Contains(t, string(makefile), "$(DOCKER) tag $(IMAGE)-linux-amd64 $(IMAGE)")
	require.NotContains(t, string(makefile), "$(DOCKER) push")
}

func stackNumberValue(t *testing.T, object map[string]interface{}, fields ...string) int64 {
	t.Helper()
	value, found, err := unstructured.NestedFieldNoCopy(object, fields...)
	require.NoError(t, err)
	require.True(t, found)
	switch number := value.(type) {
	case int64:
		return number
	case float64:
		converted := int64(number)
		require.Equal(t, number, float64(converted))
		return converted
	case int:
		return int64(number)
	default:
		t.Fatalf("%s must be numeric, got %T", fields, value)
		return 0
	}
}

func stackSubjectNames(t *testing.T, binding map[string]interface{}) []string {
	t.Helper()
	subjects, found, err := unstructured.NestedSlice(binding, "subjects")
	require.NoError(t, err)
	require.True(t, found)
	names := make([]string, 0, len(subjects))
	for _, subject := range subjects {
		entry, ok := subject.(map[string]interface{})
		require.True(t, ok)
		name, ok := entry["name"].(string)
		require.True(t, ok)
		names = append(names, name)
	}
	return names
}
