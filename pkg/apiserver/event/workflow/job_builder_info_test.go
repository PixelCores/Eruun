package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	applyv1 "k8s.io/client-go/applyconfigurations/core/v1"
)

func TestBuildServiceDeployInfo(t *testing.T) {
	svc := applyv1.Service("nginx", "default").
		WithSpec(applyv1.ServiceSpec().WithPorts(
			applyv1.ServicePort().WithName("http").WithPort(80).WithProtocol(corev1.ProtocolTCP).WithTargetPort(intstr.FromInt(8080)),
			applyv1.ServicePort().WithName("https").WithPort(443).WithProtocol(corev1.ProtocolTCP).WithTargetPort(intstr.FromInt(8443)),
		))

	info := buildServiceDeployInfo(svc, "nginx", "default")
	require.Equal(t, "svc: nginx.default.svc:80,443; ports: http:80/TCP->8080, https:443/TCP->8443", info)
}

func TestBuildIngressDeployInfo(t *testing.T) {
	ing := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "nginx",
			Namespace: "default",
		},
		Spec: networkingv1.IngressSpec{
			Rules: []networkingv1.IngressRule{
				{
					Host: "nginx.example.com",
					IngressRuleValue: networkingv1.IngressRuleValue{
						HTTP: &networkingv1.HTTPIngressRuleValue{
							Paths: []networkingv1.HTTPIngressPath{
								{Path: "/", Backend: networkingv1.IngressBackend{}},
								{Path: "/api", Backend: networkingv1.IngressBackend{}},
							},
						},
					},
				},
			},
		},
	}

	info := buildIngressDeployInfo(ing, "nginx", "default")
	require.Equal(t, "ingress: nginx.example.com/, nginx.example.com/api", info)
}
