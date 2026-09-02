package deploy_test

import (
	"errors"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/yaml"
	sigsyaml "sigs.k8s.io/yaml"
)

func TestEruunStackManifestPreservesRBACDocumentBoundary(t *testing.T) {
	manifest, err := os.Open("eruun-stack.yaml")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, manifest.Close())
	})

	decoder := yaml.NewYAMLOrJSONDecoder(manifest, 4096)
	clusterRoleBindings := 0
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

		if apiVersion == "rbac.authorization.k8s.io/v1" && kind == "ClusterRoleBinding" && name == "eruun-platform-cluster-admin" {
			clusterRoleBindings++
		}
		if apiVersion == "v1" && kind == "ConfigMap" && name == "eruun-flags" {
			runtimeFlagsConfigMaps++
			data, ok := object["data"].(map[string]interface{})
			require.True(t, ok, "runtime flags ConfigMap must contain data")
			require.Equal(t, "eruun-controller", data["ERUUN_CONTROLLER_LOCK_NAME"])
			require.Equal(t, "eruun-scheduler", data["ERUUN_SCHEDULER_LOCK_NAME"])
			require.NotContains(t, data, "ERUUN_LOCK_NAME")
		}
		if apiVersion == "v1" && kind == "Secret" {
			secrets++
		}
	}

	require.Equal(t, 1, clusterRoleBindings)
	require.Equal(t, 0, secrets)
	require.Equal(t, 1, runtimeFlagsConfigMaps)
}

func TestEruunStackUsesFixedDistributedRuntime(t *testing.T) {
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
	var clusterBinding map[string]interface{}

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
			if name == "eruun-api" || name == "eruun-controller" || name == "eruun-scheduler" || name == "eruun-worker" {
				serviceAccounts[name] = struct{}{}
			}
		case "PodDisruptionBudget":
			if name == "eruun-api" || name == "eruun-controller" || name == "eruun-scheduler" || name == "eruun-worker" {
				pdbs[name] = struct{}{}
			}
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
			if name == "eruun-platform-cluster-admin" {
				clusterBinding = object
			}
		}
	}

	roles := []string{"api", "controller", "scheduler", "worker"}
	require.Len(t, deployments, len(roles))
	require.Len(t, serviceAccounts, len(roles))
	require.Len(t, pdbs, len(roles))
	numberValue := func(object map[string]interface{}, fields ...string) int64 {
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
	for _, role := range roles {
		deployment, found := deployments[role]
		require.True(t, found, "missing %s Deployment", role)
		name, _, _ := unstructured.NestedString(deployment, "metadata", "name")
		require.Equal(t, "eruun-"+role, name)
		replicas := numberValue(deployment, "spec", "replicas")
		require.Greater(t, replicas, int64(0))
		serviceAccountName, found, err := unstructured.NestedString(deployment, "spec", "template", "spec", "serviceAccountName")
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "eruun-"+role, serviceAccountName)
		grace := numberValue(deployment, "spec", "template", "spec", "terminationGracePeriodSeconds")
		require.Equal(t, int64(90), grace)

		containers, found, err := unstructured.NestedSlice(deployment, "spec", "template", "spec", "containers")
		require.NoError(t, err)
		require.True(t, found)
		require.Len(t, containers, 1)
		container, ok := containers[0].(map[string]interface{})
		require.True(t, ok)
		env, found, err := unstructured.NestedSlice(container, "env")
		require.NoError(t, err)
		require.True(t, found)
		roleFound := false
		for _, item := range env {
			entry, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			if entry["name"] == "ERUUN_ROLE" && entry["value"] == role {
				roleFound = true
			}
		}
		require.True(t, roleFound, "missing ERUUN_ROLE=%s", role)
	}

	selector, found, err := unstructured.NestedString(service, "spec", "selector", "app.kubernetes.io/component")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "api", selector)

	data, found, err := unstructured.NestedStringMap(flags, "data")
	require.NoError(t, err)
	require.True(t, found)
	for _, key := range []string{
		"ERUUN_CONTROLLER_LOCK_NAME",
		"ERUUN_SCHEDULER_LOCK_NAME",
		"ERUUN_WORKFLOW_HEARTBEAT_INTERVAL",
		"ERUUN_WORKFLOW_LEASE_DURATION",
		"ERUUN_WORKFLOW_LEASE_REAPER_INTERVAL",
		"ERUUN_WORKFLOW_WORKER_DRAIN_TIMEOUT",
	} {
		require.NotEmpty(t, data[key], "missing %s", key)
	}
	require.NotContains(t, data, "ERUUN_WORKFLOW_LEASE_FENCING_ENABLED")
	require.NotContains(t, data, "ERUUN_LOCK_NAME")

	subjectNames := func(binding map[string]interface{}) []string {
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
	require.ElementsMatch(t, []string{"eruun-controller", "eruun-scheduler"}, subjectNames(leaderBinding))
	require.ElementsMatch(t, []string{"eruun-api", "eruun-controller", "eruun-worker"}, subjectNames(clusterBinding))
}

func TestHelmValuesUseTopLevelDistributedRuntime(t *testing.T) {
	rawValues, err := os.ReadFile("helm/eruun/values.yaml")
	require.NoError(t, err)

	var values map[string]interface{}
	require.NoError(t, sigsyaml.Unmarshal(rawValues, &values))

	runtimeValues, ok := values["runtime"].(map[string]interface{})
	require.True(t, ok, "runtime must be a top-level Helm values object")
	require.NotContains(t, runtimeValues, "mode")
	require.NotContains(t, runtimeValues, "split")
	require.NotContains(t, runtimeValues, "leaseFencingEnabled")

	roles, ok := runtimeValues["roles"].(map[string]interface{})
	require.True(t, ok, "runtime.roles must be an object")
	require.Len(t, roles, 4)
	for _, role := range []string{"api", "controller", "scheduler", "worker"} {
		require.Contains(t, roles, role)
	}

	require.NotContains(t, values, "replicaCount")
	serviceAccount, ok := values["serviceAccount"].(map[string]interface{})
	require.True(t, ok)
	require.NotContains(t, serviceAccount, "name")

	redis, ok := values["redis"].(map[string]interface{})
	require.True(t, ok)
	require.NotContains(t, redis, "roles")
	require.NotContains(t, redis, "controllerLockName")
}

func TestDockerBuildUsesDefaultDeploymentImageWithoutPublishing(t *testing.T) {
	makefile, err := os.ReadFile("../Makefile")
	require.NoError(t, err)

	require.Contains(t, string(makefile), "IMAGE           ?= ghcr.io/pixelcores/eruun:0.1.0")
	require.Contains(t, string(makefile), "$(DOCKER) tag $(IMAGE)-linux-amd64 $(IMAGE)")
	require.NotContains(t, string(makefile), "$(DOCKER) push")
}
