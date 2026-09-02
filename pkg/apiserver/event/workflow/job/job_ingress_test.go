package job

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
)

func TestIngressNeedsUpdate(t *testing.T) {
	baseSpec := ingressSpecForTest("example.com", "/", networkingv1.PathTypePrefix)
	changedSpec := ingressSpecForTest("example.com", "/api", networkingv1.PathTypePrefix)

	cases := []struct {
		name    string
		current *networkingv1.Ingress
		desired *networkingv1.Ingress
		want    bool
	}{
		{
			name:    "same spec and metadata",
			current: newIngressForTest("ing", baseSpec, map[string]string{"app": "a"}, map[string]string{"k": "v"}),
			desired: newIngressForTest("ing", baseSpec, map[string]string{"app": "a"}, map[string]string{"k": "v"}),
			want:    false,
		},
		{
			name:    "spec changed",
			current: newIngressForTest("ing", baseSpec, map[string]string{"app": "a"}, nil),
			desired: newIngressForTest("ing", changedSpec, map[string]string{"app": "a"}, nil),
			want:    true,
		},
		{
			name:    "labels changed",
			current: newIngressForTest("ing", baseSpec, map[string]string{"app": "a"}, nil),
			desired: newIngressForTest("ing", baseSpec, map[string]string{"app": "b"}, nil),
			want:    true,
		},
		{
			name:    "extra labels removed triggers update",
			current: newIngressForTest("ing", baseSpec, map[string]string{"app": "a", "extra": "1"}, nil),
			desired: newIngressForTest("ing", baseSpec, map[string]string{"app": "a"}, nil),
			want:    true,
		},
		{
			name:    "system labels preserved",
			current: newIngressForTest("ing", baseSpec, map[string]string{"app": "a", config.LabelManagedBy: config.ManagedByEruun}, nil),
			desired: newIngressForTest("ing", baseSpec, map[string]string{"app": "a"}, nil),
			want:    false,
		},
		{
			name:    "annotations changed",
			current: newIngressForTest("ing", baseSpec, nil, map[string]string{"k": "v"}),
			desired: newIngressForTest("ing", baseSpec, nil, map[string]string{"k": "new"}),
			want:    true,
		},
		{
			name:    "annotations removed triggers update",
			current: newIngressForTest("ing", baseSpec, nil, map[string]string{"k": "v"}),
			desired: newIngressForTest("ing", baseSpec, nil, nil),
			want:    true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ingressNeedsUpdate(tc.current, tc.desired); got != tc.want {
				t.Fatalf("ingressNeedsUpdate() = %v, want %v", got, tc.want)
			}
		})
	}
}

func ingressSpecForTest(host, path string, pathType networkingv1.PathType) networkingv1.IngressSpec {
	pt := pathType
	return networkingv1.IngressSpec{
		Rules: []networkingv1.IngressRule{
			{
				Host: host,
				IngressRuleValue: networkingv1.IngressRuleValue{
					HTTP: &networkingv1.HTTPIngressRuleValue{
						Paths: []networkingv1.HTTPIngressPath{
							{
								Path:     path,
								PathType: &pt,
								Backend: networkingv1.IngressBackend{
									Service: &networkingv1.IngressServiceBackend{
										Name: "svc",
										Port: networkingv1.ServiceBackendPort{Number: 80},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func newIngressForTest(name string, spec networkingv1.IngressSpec, labels, annotations map[string]string) *networkingv1.Ingress {
	return &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   "default",
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: spec,
	}
}

func TestIngressSpecsOverlap(t *testing.T) {
	specA := ingressSpecForTest("example.com", "/api", networkingv1.PathTypePrefix)
	specB := ingressSpecForTest("example.com", "/api", networkingv1.PathTypePrefix)
	specC := ingressSpecForTest("example.com", "/ws", networkingv1.PathTypePrefix)

	require.True(t, ingressSpecsOverlap(specA, specB))
	require.False(t, ingressSpecsOverlap(specA, specC))
}

func TestReusableIngressScore(t *testing.T) {
	desired := newIngressForTest("ing-demo-newappid", ingressSpecForTest("example.com", "/", networkingv1.PathTypePrefix), map[string]string{
		config.LabelAppID:         "newappid",
		config.LabelComponentName: "demo",
	}, nil)

	sameApp := newIngressForTest("ing-demo-oldappid", desired.Spec, map[string]string{
		config.LabelAppID:         "newappid",
		config.LabelComponentName: "other",
	}, nil)
	sameComponent := newIngressForTest("ing-demo-oldappid", desired.Spec, map[string]string{
		config.LabelAppID:         "otherappid",
		config.LabelComponentName: "demo",
	}, nil)
	sameComponentSameApp := newIngressForTest("ing-demo-oldappid", desired.Spec, map[string]string{
		config.LabelAppID:         "newappid",
		config.LabelComponentName: "demo",
	}, nil)
	sameLogicalBase := newIngressForTest("ing-demo-newappid", desired.Spec, nil, nil)
	unknown := newIngressForTest("custom-ingress", desired.Spec, nil, nil)

	require.Equal(t, 3, reusableIngressScore(sameApp, desired))
	require.Equal(t, 3, reusableIngressScore(sameComponentSameApp, desired))
	require.Equal(t, 0, reusableIngressScore(sameComponent, desired))
	require.Equal(t, 1, reusableIngressScore(sameLogicalBase, desired))
	require.Equal(t, 0, reusableIngressScore(unknown, desired))
}

func TestFindReusableIngressName(t *testing.T) {
	desired := newIngressForTest("ing-game-socket-ingress-newid", ingressSpecForTest("socket.example.com", "/ws/abc/", networkingv1.PathTypePrefix), map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "game-socket",
	}, nil)

	existing := newIngressForTest("ing-game-socket-ingress-oldid", desired.Spec, map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "game-socket",
	}, nil)
	other := newIngressForTest("ing-other-app", ingressSpecForTest("example.com", "/", networkingv1.PathTypePrefix), nil, nil)

	clientset := fake.NewSimpleClientset(existing, other)
	ctl := &DeployIngressJobCtl{
		deployNamespacedResourceJobBase: deployNamespacedResourceJobBase{client: clientset},
	}

	name, err := ctl.findReusableIngressName(context.Background(), desired)
	require.NoError(t, err)
	require.Equal(t, existing.Name, name)
}
