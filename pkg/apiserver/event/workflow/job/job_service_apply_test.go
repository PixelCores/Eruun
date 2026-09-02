package job

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

func countClientActions(client *fake.Clientset, verb, resource string) int {
	count := 0
	for _, action := range client.Actions() {
		if action.GetVerb() == verb && action.GetResource().Resource == resource {
			count++
		}
	}
	return count
}

func TestApplyService_PreservesHeadlessClusterIP(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:      "mysql",
		AppID:     "app-headless",
		Namespace: "default",
	}
	trait := spec.ServiceTraitSpec{
		Name:     "mysql-headless",
		Type:     "internal",
		Headless: true,
		Selector: map[string]string{
			"app": "mysql",
		},
		Ports: []spec.ServicePortTraitSpec{
			{Port: 3306, TargetPort: 3306, Protocol: "TCP"},
		},
	}

	svcCfg := GenerateServiceFromTrait(component, nil, trait)
	if svcCfg == nil {
		t.Fatalf("expected service apply config, got nil")
	}

	clientset := fake.NewSimpleClientset()
	ctl := &DeployServiceJobCtl{
		deployNamespacedResourceJobBase: deployNamespacedResourceJobBase{client: clientset},
	}
	applied, err := ctl.ApplyService(context.Background(), svcCfg)
	if err != nil {
		t.Fatalf("ApplyService failed: %v", err)
	}
	if applied.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Fatalf("expected headless ClusterIP None, got %q", applied.Spec.ClusterIP)
	}
}

func TestApplyService_UpdatesExternalName(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:      "socket",
		AppID:     "app-external",
		Namespace: "default",
	}
	trait := spec.ServiceTraitSpec{
		Name:         "socket-external",
		Type:         "external",
		ExternalName: "new.example.org",
		Ports: []spec.ServicePortTraitSpec{
			{Port: 443, TargetPort: 443, Protocol: "TCP"},
		},
	}
	svcName := trait.Name

	existing := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName,
			Namespace: component.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Type:         corev1.ServiceTypeExternalName,
			ExternalName: "old.example.org",
		},
	}

	svcCfg := GenerateServiceFromTrait(component, nil, trait)
	if svcCfg == nil {
		t.Fatalf("expected service apply config, got nil")
	}

	clientset := fake.NewSimpleClientset(existing)
	ctl := &DeployServiceJobCtl{
		deployNamespacedResourceJobBase: deployNamespacedResourceJobBase{client: clientset},
	}
	applied, err := ctl.ApplyService(context.Background(), svcCfg)
	if err != nil {
		t.Fatalf("ApplyService failed: %v", err)
	}
	if applied.Spec.ExternalName != "new.example.org" {
		t.Fatalf("expected externalName to be updated, got %q", applied.Spec.ExternalName)
	}
	if updates := countClientActions(clientset, "update", "services"); updates != 1 {
		t.Fatalf("expected one service update, got %d", updates)
	}
}

func TestApplyService_SkipsUnchangedServiceUpdate(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:      "mysql",
		AppID:     "app-service-noop",
		Namespace: "default",
	}
	trait := spec.ServiceTraitSpec{
		Name: "mysql",
		Type: "internal",
		Selector: map[string]string{
			"app": "mysql",
		},
		Ports: []spec.ServicePortTraitSpec{
			{Port: 3306, TargetPort: 3306, Protocol: "TCP"},
		},
	}

	svcCfg := GenerateServiceFromTrait(component, nil, trait)
	if svcCfg == nil {
		t.Fatalf("expected service apply config, got nil")
	}
	existing := serviceFromApplyConfig(svcCfg)
	existing.ResourceVersion = "1"
	existing.Spec.ClusterIP = "10.0.0.10"
	existing.Spec.ClusterIPs = []string{"10.0.0.10"}
	existing.Spec.IPFamilies = []corev1.IPFamily{corev1.IPv4Protocol}
	ipFamilyPolicy := corev1.IPFamilyPolicySingleStack
	existing.Spec.IPFamilyPolicy = &ipFamilyPolicy

	clientset := fake.NewSimpleClientset(existing)
	ctl := &DeployServiceJobCtl{
		deployNamespacedResourceJobBase: deployNamespacedResourceJobBase{client: clientset},
	}
	applied, err := ctl.ApplyService(context.Background(), svcCfg)
	if err != nil {
		t.Fatalf("ApplyService failed: %v", err)
	}
	if applied.ResourceVersion != "1" {
		t.Fatalf("expected existing service to be returned, got resourceVersion %q", applied.ResourceVersion)
	}
	if updates := countClientActions(clientset, "update", "services"); updates != 0 {
		t.Fatalf("expected no service update, got %d", updates)
	}
}

func TestApplyService_SkipsUnchangedNodePortServiceUpdate(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:      "web",
		AppID:     "app-nodeport-noop",
		Namespace: "default",
	}
	trait := spec.ServiceTraitSpec{
		Name:     "web",
		Type:     "node",
		Selector: map[string]string{"app": "web"},
		Ports: []spec.ServicePortTraitSpec{
			{Name: "http", Port: 80, TargetPort: 8080, Protocol: "TCP"},
		},
	}

	svcCfg := GenerateServiceFromTrait(component, nil, trait)
	if svcCfg == nil {
		t.Fatalf("expected service apply config, got nil")
	}
	existing := serviceFromApplyConfig(svcCfg)
	existing.ResourceVersion = "1"
	existing.Spec.ClusterIP = "10.0.0.20"
	existing.Spec.ClusterIPs = []string{"10.0.0.20"}
	existing.Spec.IPFamilies = []corev1.IPFamily{corev1.IPv4Protocol}
	ipFamilyPolicy := corev1.IPFamilyPolicySingleStack
	existing.Spec.IPFamilyPolicy = &ipFamilyPolicy
	existing.Spec.Ports[0].NodePort = 30080

	clientset := fake.NewSimpleClientset(existing)
	ctl := &DeployServiceJobCtl{
		deployNamespacedResourceJobBase: deployNamespacedResourceJobBase{client: clientset},
	}
	applied, err := ctl.ApplyService(context.Background(), svcCfg)
	if err != nil {
		t.Fatalf("ApplyService failed: %v", err)
	}
	if applied.Spec.Ports[0].NodePort != 30080 {
		t.Fatalf("expected existing nodePort to be returned, got %d", applied.Spec.Ports[0].NodePort)
	}
	if updates := countClientActions(clientset, "update", "services"); updates != 0 {
		t.Fatalf("expected no service update, got %d", updates)
	}
}

func TestApplyService_SkipsUnchangedLoadBalancerServiceUpdate(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:      "web",
		AppID:     "app-loadbalancer-noop",
		Namespace: "default",
	}
	trait := spec.ServiceTraitSpec{
		Name:     "web",
		Type:     "public",
		Selector: map[string]string{"app": "web"},
		Ports: []spec.ServicePortTraitSpec{
			{Name: "http", Port: 80, TargetPort: 8080, Protocol: "TCP"},
		},
	}

	svcCfg := GenerateServiceFromTrait(component, nil, trait)
	if svcCfg == nil {
		t.Fatalf("expected service apply config, got nil")
	}
	existing := serviceFromApplyConfig(svcCfg)
	existing.ResourceVersion = "1"
	existing.Spec.ClusterIP = "10.0.0.30"
	existing.Spec.ClusterIPs = []string{"10.0.0.30"}
	existing.Spec.IPFamilies = []corev1.IPFamily{corev1.IPv4Protocol}
	ipFamilyPolicy := corev1.IPFamilyPolicySingleStack
	existing.Spec.IPFamilyPolicy = &ipFamilyPolicy
	existing.Spec.Ports[0].NodePort = 30080
	allocateNodePorts := true
	existing.Spec.AllocateLoadBalancerNodePorts = &allocateNodePorts

	clientset := fake.NewSimpleClientset(existing)
	ctl := &DeployServiceJobCtl{
		deployNamespacedResourceJobBase: deployNamespacedResourceJobBase{client: clientset},
	}
	applied, err := ctl.ApplyService(context.Background(), svcCfg)
	if err != nil {
		t.Fatalf("ApplyService failed: %v", err)
	}
	if applied.Spec.AllocateLoadBalancerNodePorts == nil || !*applied.Spec.AllocateLoadBalancerNodePorts {
		t.Fatalf("expected existing allocateLoadBalancerNodePorts to be returned, got %v", applied.Spec.AllocateLoadBalancerNodePorts)
	}
	if updates := countClientActions(clientset, "update", "services"); updates != 0 {
		t.Fatalf("expected no service update, got %d", updates)
	}
}

func TestServiceNeedsUpdate(t *testing.T) {
	base := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "mysql",
			Namespace:   "default",
			Labels:      map[string]string{"app": "mysql"},
			Annotations: map[string]string{"owner": "eruun"},
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: map[string]string{"app": "mysql"},
			Ports: []corev1.ServicePort{{
				Name:     "mysql",
				Port:     3306,
				Protocol: corev1.ProtocolTCP,
			}},
		},
	}

	if serviceNeedsUpdate(base, base.DeepCopy()) {
		t.Fatal("expected unchanged service to skip update")
	}

	desiredPort := base.DeepCopy()
	desiredPort.Spec.Ports[0].Port = 3307
	if !serviceNeedsUpdate(base, desiredPort) {
		t.Fatal("expected service port change to require update")
	}

	desiredSelector := base.DeepCopy()
	desiredSelector.Spec.Selector = map[string]string{"app": "redis"}
	if !serviceNeedsUpdate(base, desiredSelector) {
		t.Fatal("expected service selector change to require update")
	}

	desiredType := base.DeepCopy()
	desiredType.Spec.Type = corev1.ServiceTypeNodePort
	if !serviceNeedsUpdate(base, desiredType) {
		t.Fatal("expected service type change to require update")
	}

	desiredLabels := base.DeepCopy()
	desiredLabels.Labels = map[string]string{"app": "mysql", "tier": "store"}
	if !serviceNeedsUpdate(base, desiredLabels) {
		t.Fatal("expected service labels change to require update")
	}

	desiredAnnotations := base.DeepCopy()
	desiredAnnotations.Annotations = map[string]string{"owner": "eruun", "revision": "2"}
	if !serviceNeedsUpdate(base, desiredAnnotations) {
		t.Fatal("expected service annotations change to require update")
	}
}

func TestServiceNeedsUpdateDetectsLoadBalancerNodePortAllocationChange(t *testing.T) {
	allocateNodePorts := true
	current := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web",
			Namespace: "default",
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeLoadBalancer,
			Ports: []corev1.ServicePort{{
				Name:     "http",
				Port:     80,
				Protocol: corev1.ProtocolTCP,
				NodePort: 30080,
			}},
			AllocateLoadBalancerNodePorts: &allocateNodePorts,
		},
	}
	desired := current.DeepCopy()
	disableNodePortAllocation := false
	desired.Spec.AllocateLoadBalancerNodePorts = &disableNodePortAllocation

	if !serviceNeedsUpdate(current, desired) {
		t.Fatal("expected loadbalancer nodeport allocation change to require update")
	}
}
