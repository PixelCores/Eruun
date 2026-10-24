package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

func TestBuildServiceDeployInfo(t *testing.T) {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "nginx", Namespace: "default"},
		Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{
			{Name: "http", Port: 80, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromInt(8080)},
			{Name: "https", Port: 443, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromInt(8443)},
		}},
	}

	info := buildServiceDeployInfo(svc, "nginx", "default")
	require.Equal(t, "svc: nginx.default.svc:80,443; ports: http:80/TCP->8080, https:443/TCP->8443", info)
}

func TestBuildRouteDeployInfo(t *testing.T) {
	component := &model.ApplicationComponent{Name: "nginx", Namespace: "default"}
	gateway := spec.GatewayTraitSpec{
		Hosts:  []string{"nginx-default.apps.example.com", "web-default.apps.example.com"},
		Routes: []spec.GatewayRouteSpec{{}, {Path: "/api"}, {Path: "/api", Match: spec.GatewayMatchExact}},
	}
	cfg := spec.GatewayConfig{Provider: spec.GatewayProviderIngress, Domain: "apps.example.com"}
	require.Equal(t, "gateway: nginx-default.apps.example.com/, nginx-default.apps.example.com/api, web-default.apps.example.com/, web-default.apps.example.com/api",
		buildRouteDeployInfo("nginx", gateway, component, cfg))
}

func TestBuildServiceDeployInfoOmitsUnsetTargetPort(t *testing.T) {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "backend", Namespace: "ops"},
		Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{
			{Name: "http", Port: 80},
			{Name: "https", Port: 443, TargetPort: intstr.FromString("tls")},
		}},
	}
	require.Equal(t, "svc: backend.ops.svc:80,443; ports: http:80/TCP, https:443/TCP->tls", buildServiceDeployInfo(svc, "", ""))
}
