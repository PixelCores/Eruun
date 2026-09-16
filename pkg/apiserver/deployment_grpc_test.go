package apiserver

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func yamlMap(value any) map[string]any {
	result, _ := value.(map[string]any)
	return result
}
func yamlPath(document map[string]any, names ...string) any {
	var value any = document
	for _, name := range names {
		value = yamlMap(value)[name]
	}
	return value
}

func TestStaticDeploymentGRPCPortAndHTTPProbes(t *testing.T) {
	content, err := os.ReadFile(filepath.Join("..", "..", "deploy", "eruun-stack.yaml"))
	require.NoError(t, err)
	decoder := yaml.NewDecoder(bytes.NewReader(content))
	var service, flags, ingress map[string]any
	deployments := map[string]map[string]any{}
	for {
		var document map[string]any
		err := decoder.Decode(&document)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		name, _ := yamlPath(document, "metadata", "name").(string)
		switch document["kind"] {
		case "Service":
			if name == "eruun" {
				service = document
			}
		case "ConfigMap":
			if name == "eruun-flags" {
				flags = document
			}
		case "Ingress":
			if name == "eruun" {
				ingress = document
			}
		case "Deployment":
			if name == "eruun-api" || name == "eruun-controller" || name == "eruun-scheduler" || name == "eruun-worker" {
				deployments[name] = document
			}
		}
	}
	require.NotNil(t, service)
	require.NotNil(t, flags)
	require.NotNil(t, ingress)
	require.Len(t, deployments, 4)
	require.Equal(t, "0.0.0.0:9000", yamlPath(flags, "data", "ERUUN_GRPC_BIND_ADDR"))
	ports, ok := yamlPath(service, "spec", "ports").([]any)
	require.True(t, ok)
	require.Len(t, ports, 2)
	servicePorts := map[string]any{}
	for _, port := range ports {
		servicePorts[yamlMap(port)["name"].(string)] = yamlMap(port)["port"]
	}
	require.Equal(t, 8000, servicePorts["http"])
	require.Equal(t, 9000, servicePorts["grpc"])
	require.Equal(t, 8000, yamlPath(ingress, "spec", "rules").([]any)[0].(map[string]any)["http"].(map[string]any)["paths"].([]any)[0].(map[string]any)["backend"].(map[string]any)["service"].(map[string]any)["port"].(map[string]any)["number"])
	for name, deployment := range deployments {
		containers, ok := yamlPath(deployment, "spec", "template", "spec", "containers").([]any)
		require.True(t, ok)
		require.Len(t, containers, 1)
		container := yamlMap(containers[0])
		containerPorts, _ := container["ports"].([]any)
		hasGRPC := false
		for _, item := range containerPorts {
			if yamlMap(item)["name"] == "grpc" {
				hasGRPC = true
				require.Equal(t, 9000, yamlMap(item)["containerPort"])
			}
		}
		require.Equal(t, name == "eruun-api", hasGRPC, name)
		for _, probe := range []string{"startupProbe", "readinessProbe", "livenessProbe"} {
			require.Equal(t, "http", yamlPath(container, probe, "httpGet", "port"), name+" "+probe)
		}
	}
}
