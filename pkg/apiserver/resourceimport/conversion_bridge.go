package resourceimport

import (
	"strings"

	conversionservice "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/conversion"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func convertKubeObjectsToComponents(objects []*unstructured.Unstructured) ([]apisv1.CreateComponentRequest, []string, error) {
	return conversionservice.ConvertKubeObjectsToComponents(objects)
}

func ensureShareLabels(labels map[string]string, name string) map[string]string {
	return conversionservice.EnsureShareLabels(labels, name)
}

func isSystemSecret(secret corev1.Secret) bool {
	if secret.Type == corev1.SecretTypeServiceAccountToken {
		return true
	}
	return strings.HasPrefix(secret.Name, "default-token-")
}
