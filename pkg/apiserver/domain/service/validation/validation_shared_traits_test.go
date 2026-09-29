package validation

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func TestTryApplicationTraitErrorsPreservePresentationAndOrder(t *testing.T) {
	zero := intstr.FromInt32(0)
	req := validCallbackTryApplicationRequest()
	req.Components[0].Traits = spec.Traits{
		Ingress: []spec.IngressTraitsSpec{{
			Name: "BAD_INGRESS",
			Label: map[string]string{
				config.LabelComponentName: "other-component",
				config.LabelAppID:         "other-app",
			},
			Hosts: []string{"192.0.2.1"},
			Routes: []spec.IngressRoutes{{
				Path: "/", Backend: spec.IngressRoute{ServiceName: "backend", ServicePort: 8080},
			}},
		}},
		Service: []spec.ServiceTraitSpec{{
			Name: "BAD_SERVICE", Type: "unknown", Headless: true,
			Labels: map[string]string{
				config.LabelComponentName: "other-component",
				config.LabelAppID:         "other-app",
			},
			Ports: []spec.ServicePortTraitSpec{{Port: 0, TargetPort: -1, Protocol: "INVALID"}},
		}},
		Rollout: &spec.RolloutTraitSpec{
			Type:          "RollingUpdate",
			RollingUpdate: &spec.RolloutRollingUpdateSpec{MaxSurge: &zero, MaxUnavailable: &zero},
		},
	}

	resp := (&validationServiceImpl{}).TryApplication(context.Background(), req)
	require.False(t, resp.Valid)
	// Keep the trait sequence separate from the later application-wide resource
	// name checks, which also reject the two invalid resource names.
	var traitErrors []apisv1.ValidationError
	for _, validationErr := range resp.Errors {
		if strings.HasPrefix(validationErr.Field, "component[0].traits.") {
			traitErrors = append(traitErrors, apisv1.ValidationError{
				Field: validationErr.Field, Code: validationErr.Code, Message: validationErr.Message,
			})
		}
	}
	require.Equal(t, []apisv1.ValidationError{
		{Field: "component[0].traits.ingress[0].label.eruun.io/app-id", Code: apisv1.ErrCodeInvalidTraitConfig, Message: `traits.ingress.label key "eruun.io/app-id" is reserved and cannot be overridden`},
		{Field: "component[0].traits.ingress[0].label.eruun.io/component-name", Code: apisv1.ErrCodeInvalidTraitConfig, Message: `traits.ingress.label key "eruun.io/component-name" is reserved and cannot be overridden`},
		{Field: "component[0].traits.ingress[0].name", Code: apisv1.ErrCodeInvalidNameFormat, Message: "component[0].traits.ingress[0].name must match DNS-1123 subdomain (lowercase alphanumeric, may contain hyphens, must start and end with alphanumeric)"},
		{Field: "component[0].traits.ingress[0].hosts[0]", Code: apisv1.ErrCodeInvalidTraitConfig, Message: "component[0].traits.ingress[0].hosts[0] must be a DNS name, not an IP address"},
		{Field: "component[0].traits.service[0].name", Code: apisv1.ErrCodeInvalidNameFormat, Message: "component[0].traits.service[0].name must match DNS-1123 subdomain (lowercase alphanumeric, may contain hyphens, must start and end with alphanumeric)"},
		{Field: "component[0].traits.service[0].labels.eruun.io/app-id", Code: apisv1.ErrCodeInvalidTraitConfig, Message: `traits.service.labels key "eruun.io/app-id" is reserved and cannot be overridden`},
		{Field: "component[0].traits.service[0].labels.eruun.io/component-name", Code: apisv1.ErrCodeInvalidTraitConfig, Message: `traits.service.labels key "eruun.io/component-name" is reserved and cannot be overridden`},
		{Field: "component[0].traits.service[0].type", Code: apisv1.ErrCodeInvalidTraitConfig, Message: "invalid service type: unknown, must be one of: internal, node, public, external"},
		{Field: "component[0].traits.service[0].selector", Code: apisv1.ErrCodeMissingRequiredField, Message: "selector is required for non-external service"},
		{Field: "component[0].traits.service[0].ports[0].port", Code: apisv1.ErrCodeMissingRequiredField, Message: "service port is required and must be positive"},
		{Field: "component[0].traits.service[0].ports[0].targetPort", Code: apisv1.ErrCodeInvalidTraitConfig, Message: "targetPort must be greater than or equal to zero"},
		{Field: "component[0].traits.service[0].ports[0].protocol", Code: apisv1.ErrCodeInvalidTraitConfig, Message: "invalid service protocol: INVALID, must be one of: TCP, UDP, SCTP"},
		{Field: "component[0].traits.rollout.rollingUpdate", Code: apisv1.ErrCodeInvalidTraitConfig, Message: "deployment rollout maxSurge and maxUnavailable cannot both be 0"},
	}, traitErrors)

	// Presentation adapters must not remove fields from the resubmittable spec.
	require.NotNil(t, resp.NormalizedSpec)
	traits := resp.NormalizedSpec.Components[0].Traits
	require.Equal(t, "BAD_INGRESS", traits.Ingress[0].Name)
	require.Equal(t, "BAD_SERVICE", traits.Service[0].Name)
	labels := map[string]string{config.LabelAppID: "other-app", config.LabelComponentName: "other-component"}
	require.Equal(t, labels, traits.Ingress[0].Label)
	require.Equal(t, labels, traits.Service[0].Labels)
}
