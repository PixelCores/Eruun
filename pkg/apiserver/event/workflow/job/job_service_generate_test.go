package job

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

func TestGenerateServiceFromTrait_UsesTraitFields(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:      "mysql",
		AppID:     "app-1",
		Namespace: "nvg",
	}
	properties := &model.Properties{
		Labels: map[string]string{
			"app.kubernetes.io/part-of": "db",
		},
	}
	trait := spec.ServiceTraitSpec{
		Name:     "mysql-master",
		Type:     "internal",
		Headless: true,
		Selector: map[string]string{
			"mysql-pod-role": "master",
		},
		Labels: map[string]string{
			"layer": "db",
		},
		Ports: []spec.ServicePortTraitSpec{
			{
				Name:       "mysql",
				Port:       3306,
				TargetPort: 3306,
				Protocol:   "TCP",
			},
		},
	}

	svc := GenerateServiceFromTrait(component, properties, trait)
	if svc == nil {
		t.Fatalf("expected service, got nil")
	}
	if svc.Name == nil || *svc.Name != "mysql-master" {
		t.Fatalf("unexpected service name: %#v", svc.Name)
	}
	if svc.Spec == nil {
		t.Fatalf("expected service spec, got nil")
	}
	if svc.Spec.Type == nil || *svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Fatalf("unexpected service type: %#v", svc.Spec.Type)
	}
	if svc.Spec.ClusterIP == nil || *svc.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Fatalf("expected headless ClusterIP None, got %#v", svc.Spec.ClusterIP)
	}
	if got := svc.Spec.Selector["mysql-pod-role"]; got != "master" {
		t.Fatalf("unexpected selector mysql-pod-role=%q", got)
	}
	if _, exists := svc.Spec.Selector[config.LabelAppID]; exists {
		t.Fatalf("expected custom selector to remain unchanged, got %+v", svc.Spec.Selector)
	}
	if got := svc.Labels["layer"]; got != "db" {
		t.Fatalf("expected trait label layer=db, got %q", got)
	}
	if got := len(svc.Spec.Ports); got != 1 {
		t.Fatalf("expected 1 service port, got %d", got)
	}
	if svc.Spec.Ports[0].TargetPort == nil || svc.Spec.Ports[0].TargetPort.IntVal != 3306 {
		t.Fatalf("unexpected target port: %#v", svc.Spec.Ports[0].TargetPort)
	}
}

func TestBuildLabels_ManagedLabelsOverridePropertiesLabels(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:  "api",
		AppID: "app-1",
		ID:    7,
	}
	properties := &model.Properties{
		Labels: map[string]string{
			config.LabelManagedBy:     config.ManagedByEruun,
			config.LabelAppID:         "evil-app",
			config.LabelComponentID:   "99",
			config.LabelComponentName: "custom-api",
			"team":                    "Platform Team",
		},
	}

	labels := BuildLabels(component, properties)

	require.Equal(t, config.ManagedByEruun, labels[config.LabelManagedBy])
	require.Equal(t, "app-1", labels[config.LabelAppID])
	require.Equal(t, "7", labels[config.LabelComponentID])
	require.Equal(t, "api", labels[config.LabelComponentName])
	require.Equal(t, "platform-team", labels["team"])
}

func TestGenerateServiceFromTrait_ManagedLabelsOverrideTraitLabels(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:      "api",
		AppID:     "app-1",
		ID:        7,
		Namespace: "default",
	}
	properties := &model.Properties{
		Labels: map[string]string{
			"team": "Platform Team",
		},
	}
	trait := spec.ServiceTraitSpec{
		Name: "api-service",
		Type: "internal",
		Selector: map[string]string{
			"app": "api",
		},
		Labels: map[string]string{
			config.LabelManagedBy:     config.ManagedByEruun,
			config.LabelAppID:         "evil-app",
			config.LabelComponentID:   "99",
			config.LabelComponentName: "custom-api",
			"layer":                   "Edge Service",
		},
		Ports: []spec.ServicePortTraitSpec{
			{Port: 8080},
		},
	}

	svc := GenerateServiceFromTrait(component, properties, trait)

	require.NotNil(t, svc)
	require.Equal(t, config.ManagedByEruun, svc.Labels[config.LabelManagedBy])
	require.Equal(t, "app-1", svc.Labels[config.LabelAppID])
	require.Equal(t, "7", svc.Labels[config.LabelComponentID])
	require.Equal(t, "api", svc.Labels[config.LabelComponentName])
	require.Equal(t, "platform-team", svc.Labels["team"])
	require.Equal(t, "edge-service", svc.Labels["layer"])
}

func TestGenerateServiceFromTrait_RebindsManagedIdentitySelectors(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:      "api",
		AppID:     "target-app",
		ID:        7,
		Namespace: "default",
	}
	trait := spec.ServiceTraitSpec{
		Selector: map[string]string{
			config.LabelManagedBy:     "Helm",
			config.LabelAppID:         "source-app",
			config.LabelComponentID:   "41",
			config.LabelComponentName: "source-api",
			"role":                    "backend",
		},
		Ports: []spec.ServicePortTraitSpec{{Port: 8080}},
	}

	svc := GenerateServiceFromTrait(component, nil, trait)

	require.NotNil(t, svc)
	require.NotNil(t, svc.Spec)
	require.Equal(t, "Helm", svc.Spec.Selector[config.LabelManagedBy])
	require.Equal(t, "target-app", svc.Spec.Selector[config.LabelAppID])
	require.Equal(t, "7", svc.Spec.Selector[config.LabelComponentID])
	require.Equal(t, "api", svc.Spec.Selector[config.LabelComponentName])
	require.Equal(t, "backend", svc.Spec.Selector["role"])
}

func TestGenerateServiceFromTrait_PreservesAdoptedIdentitySelectors(t *testing.T) {
	sourceUID := "deployment-uid"
	component := &model.ApplicationComponent{
		Name:                     "api",
		AppID:                    "target-app",
		ID:                       7,
		Namespace:                "default",
		SourceWorkloadAPIVersion: "apps/v1",
		SourceWorkloadKind:       "Deployment",
		SourceWorkloadName:       "source-api",
		SourceWorkloadUID:        &sourceUID,
	}
	trait := spec.ServiceTraitSpec{
		Selector: map[string]string{
			config.LabelAppID:         "source-app",
			config.LabelComponentID:   "41",
			config.LabelComponentName: "source-api",
		},
		Ports: []spec.ServicePortTraitSpec{{Port: 8080}},
	}

	svc := GenerateServiceFromTrait(component, nil, trait)

	require.NotNil(t, svc)
	require.NotNil(t, svc.Spec)
	require.Equal(t, "source-app", svc.Spec.Selector[config.LabelAppID])
	require.Equal(t, "41", svc.Spec.Selector[config.LabelComponentID])
	require.Equal(t, "source-api", svc.Spec.Selector[config.LabelComponentName])
}

func TestGenerateServiceFromTrait_NormalizesGeneratedSelectorsAndPreservesValidExternalSelectors(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:      "API",
		AppID:     "app-1",
		Namespace: "default",
	}
	trait := spec.ServiceTraitSpec{
		Selector: map[string]string{
			config.LabelManagedBy:     "Helm",
			config.LabelComponentName: "API",
			"role":                    "API",
			"game":                    "Penalty Shootout 2026",
			"version":                 "v1.2.3",
			"track":                   "canary_A",
		},
		Ports: []spec.ServicePortTraitSpec{
			{Port: 8080},
		},
	}

	svc := GenerateServiceFromTrait(component, nil, trait)
	require.NotNil(t, svc)
	require.NotNil(t, svc.Spec)
	require.Equal(t, "Helm", svc.Spec.Selector[config.LabelManagedBy])
	require.Equal(t, "api", svc.Spec.Selector[config.LabelComponentName])
	require.Equal(t, "API", svc.Spec.Selector["role"])
	require.Equal(t, "penalty-shootout-2026", svc.Spec.Selector["game"])
	require.Equal(t, "v1.2.3", svc.Spec.Selector["version"])
	require.Equal(t, "canary_A", svc.Spec.Selector["track"])
}

func TestGenerateServiceFromTrait_NormalizesSelectorValuesForPropertyLabels(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:      "backend",
		AppID:     "app-1",
		Namespace: "default",
	}
	properties := &model.Properties{
		Labels: map[string]string{
			"version": "v1.2.3",
		},
	}
	trait := spec.ServiceTraitSpec{
		Selector: map[string]string{
			"version": "v1.2.3",
		},
		Ports: []spec.ServicePortTraitSpec{
			{Port: 8080},
		},
	}

	svc := GenerateServiceFromTrait(component, properties, trait)

	require.NotNil(t, svc)
	require.Equal(t, "v1-2-3", svc.Labels["version"])
	require.Equal(t, "v1-2-3", svc.Spec.Selector["version"])
}

func TestGenerateServiceFromTrait_NormalizesLabelValues(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:      "backend",
		AppID:     "app-1",
		ID:        7,
		Namespace: "default",
	}
	properties := &model.Properties{
		Labels: map[string]string{
			"name": "penalty shootout 2026-m2606241344ccufxh-backend",
		},
	}
	trait := spec.ServiceTraitSpec{
		Name: "backend-service",
		Type: "internal",
		Labels: map[string]string{
			"service-name": "Penalty Shootout 2026 Service",
		},
		Selector: map[string]string{
			"name": "penalty shootout 2026-m2606241344ccufxh-backend",
		},
		Ports: []spec.ServicePortTraitSpec{{Port: 8080}},
	}

	svc := GenerateServiceFromTrait(component, properties, trait)

	require.NotNil(t, svc)
	require.Equal(t, "penalty-shootout-2026-m2606241344ccufxh-backend", svc.Labels["name"])
	require.Equal(t, "penalty-shootout-2026-service", svc.Labels["service-name"])
	require.Equal(t, "penalty-shootout-2026-m2606241344ccufxh-backend", svc.Spec.Selector["name"])
}

func TestGenerateServiceFromTrait_Defaults(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:      "api",
		AppID:     "app-2",
		Namespace: "default",
	}
	trait := spec.ServiceTraitSpec{
		Selector: map[string]string{
			"app": "api",
		},
		Ports: []spec.ServicePortTraitSpec{
			{
				Port: 8080,
			},
		},
	}

	svc := GenerateServiceFromTrait(component, nil, trait)
	if svc == nil {
		t.Fatalf("expected service, got nil")
	}
	if svc.Name == nil || *svc.Name != buildServiceName(component.Name, component.ResourceAppNameOrID()) {
		t.Fatalf("unexpected default service name: %#v", svc.Name)
	}
	if svc.Spec == nil {
		t.Fatalf("expected service spec, got nil")
	}
	if svc.Spec.Type == nil || *svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Fatalf("expected default ClusterIP type, got %#v", svc.Spec.Type)
	}
	if len(svc.Spec.Ports) != 1 {
		t.Fatalf("expected one service port, got %d", len(svc.Spec.Ports))
	}
	if svc.Spec.Ports[0].Protocol == nil || *svc.Spec.Ports[0].Protocol != corev1.ProtocolTCP {
		t.Fatalf("expected default TCP protocol, got %#v", svc.Spec.Ports[0].Protocol)
	}
	if svc.Spec.Ports[0].TargetPort == nil || svc.Spec.Ports[0].TargetPort.IntVal != 8080 {
		t.Fatalf("expected default targetPort=8080, got %#v", svc.Spec.Ports[0].TargetPort)
	}
}

func TestGenerateServiceFromTrait_DefaultPortNameFallsBackWhenTooLong(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:      "very-long-component-name",
		AppID:     "app-6",
		Namespace: "default",
	}
	trait := spec.ServiceTraitSpec{
		Ports: []spec.ServicePortTraitSpec{
			{
				Port: 8080,
			},
		},
	}

	svc := GenerateServiceFromTrait(component, nil, trait)
	if svc == nil || svc.Spec == nil {
		t.Fatalf("expected service spec")
	}
	if len(svc.Spec.Ports) != 1 {
		t.Fatalf("expected one service port, got %d", len(svc.Spec.Ports))
	}
	if svc.Spec.Ports[0].Name == nil || *svc.Spec.Ports[0].Name != "p-8080" {
		t.Fatalf("expected fallback port name p-8080, got %#v", svc.Spec.Ports[0].Name)
	}
}

func TestGenerateServiceFromTrait_DefaultSelectorWhenEmpty(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:      "api",
		AppID:     "app-4",
		Namespace: "default",
	}
	trait := spec.ServiceTraitSpec{
		Ports: []spec.ServicePortTraitSpec{
			{
				Port: 8080,
			},
		},
	}

	svc := GenerateServiceFromTrait(component, nil, trait)
	if svc == nil || svc.Spec == nil {
		t.Fatalf("expected service spec")
	}
	if got := svc.Spec.Selector[config.LabelAppID]; got != "app-4" {
		t.Fatalf("expected default selector app label app-4, got %q", got)
	}
	if got := svc.Spec.Selector[config.LabelComponentName]; got != "api" {
		t.Fatalf("expected default selector component label api, got %q", got)
	}
}

func TestGenerateServiceFromTrait_ExternalName(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:      "api",
		AppID:     "app-3",
		Namespace: "default",
	}
	trait := spec.ServiceTraitSpec{
		Name:         "api-external",
		Type:         "external",
		ExternalName: "example.org",
		Ports: []spec.ServicePortTraitSpec{
			{Port: 443, TargetPort: 443, Protocol: "TCP"},
		},
	}

	svc := GenerateServiceFromTrait(component, nil, trait)
	if svc == nil || svc.Spec == nil {
		t.Fatalf("expected service spec")
	}
	if svc.Name == nil || *svc.Name != "api-external" {
		t.Fatalf("expected explicit service name api-external, got %#v", svc.Name)
	}
	if svc.Spec.Type == nil || *svc.Spec.Type != corev1.ServiceTypeExternalName {
		t.Fatalf("expected ExternalName type, got %#v", svc.Spec.Type)
	}
	if svc.Spec.ExternalName == nil || *svc.Spec.ExternalName != "example.org" {
		t.Fatalf("expected externalName example.org, got %#v", svc.Spec.ExternalName)
	}
	if svc.Spec.Selector != nil {
		t.Fatalf("expected nil selector for external service, got %#v", svc.Spec.Selector)
	}
}

func TestGenerateService_Default(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:      "very-long-component-name",
		AppID:     "app-5",
		Namespace: "default",
	}
	properties := &model.Properties{
		Ports: []model.Ports{
			{Port: 8080},
			{Port: 9090},
		},
	}

	svc := GenerateService(component, properties)
	if svc == nil || svc.Spec == nil {
		t.Fatalf("expected generated service")
	}
	if svc.Name == nil || *svc.Name != buildServiceName(component.Name, component.ResourceAppNameOrID()) {
		t.Fatalf("unexpected service name: %#v", svc.Name)
	}
	require.Equal(t, map[string]string{
		config.LabelAppID:         component.AppID,
		config.LabelComponentName: component.Name,
	}, svc.Spec.Selector)
	if svc.Spec.Type == nil || *svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Fatalf("expected ClusterIP service, got %#v", svc.Spec.Type)
	}
	if len(svc.Spec.Ports) != 2 {
		t.Fatalf("expected 2 ports, got %d", len(svc.Spec.Ports))
	}
	if svc.Spec.Ports[0].Name == nil || *svc.Spec.Ports[0].Name == "" {
		t.Fatalf("expected port name to be generated")
	}
	if len(*svc.Spec.Ports[0].Name) > 15 && (*svc.Spec.Ports[0].Name)[:2] != "p-" {
		t.Fatalf("expected long port name to fallback with p- prefix, got %s", *svc.Spec.Ports[0].Name)
	}
}
