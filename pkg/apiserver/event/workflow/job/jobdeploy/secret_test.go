package jobdeploy

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
)

func TestGenerateSecret(t *testing.T) {
	// Test case 1: Basic secret generation
	t.Run("BasicSecret", func(t *testing.T) {
		component := &model.ApplicationComponent{
			Name:      "my-secret",
			Namespace: "default",
			AppID:     "test-app",
			ID:        1,
		}
		properties := &model.Properties{
			Secret: map[string]string{
				"username": "admin",
				"password": "password123",
			},
		}
		expected := &jobinfo.SecretInput{
			Name:      "my-secret",
			Namespace: "default",
			Labels:    map[string]string{spec.LabelManagedBy: config.ManagedByEruun, spec.LabelAppID: "test-app", spec.LabelComponentID: "1", spec.LabelComponentName: "my-secret"},
			Data: map[string]string{
				"username": "admin",
				"password": "password123",
			},
		}
		actual := GenerateSecret(component, properties)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expected %v, but got %v", expected, actual)
		}
	})

	// Test case 2: Secret generation from URL
	t.Run("SecretFromURL", func(t *testing.T) {
		component := &model.ApplicationComponent{
			Name:      "my-secret-from-url",
			Namespace: "kube-system",
			AppID:     "test-app",
			ID:        2,
		}
		properties := &model.Properties{
			Labels: map[string]string{
				spec.LabelComponentName: "custom-secret",
				"team":                  "security",
			},
			Secret: map[string]string{},
			Conf: map[string]string{
				"config.url":      "http://example.com/config",
				"config.fileName": "my-config-file",
			},
		}
		expected := &jobinfo.SecretInput{
			Name:      "my-secret-from-url",
			Namespace: "kube-system",
			URL:       "http://example.com/config",
			FileName:  "my-config-file",
			Labels: map[string]string{
				spec.LabelManagedBy:     config.ManagedByEruun,
				spec.LabelAppID:         "test-app",
				spec.LabelComponentID:   "2",
				spec.LabelComponentName: "my-secret-from-url",
				"team":                  "security",
			},
		}
		actual := GenerateSecret(component, properties)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expected %v, but got %v", expected, actual)
		}
	})

	// Test case 3: Nil properties
	t.Run("NilProperties", func(t *testing.T) {
		component := &model.ApplicationComponent{
			Name:      "nil-props-secret",
			Namespace: "default",
			AppID:     "test-app",
			ID:        3,
		}
		expected := &jobinfo.SecretInput{
			Name:      "nil-props-secret",
			Namespace: "default",
			Labels:    map[string]string{spec.LabelManagedBy: config.ManagedByEruun, spec.LabelAppID: "test-app", spec.LabelComponentID: "3", spec.LabelComponentName: "nil-props-secret"},
			Data:      nil,
		}
		actual := GenerateSecret(component, nil)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expected %v, but got %v", expected, actual)
		}
	})

	// Test case 4: Empty secret data
	t.Run("EmptySecretData", func(t *testing.T) {
		component := &model.ApplicationComponent{
			Name:      "empty-secret",
			Namespace: "default",
			AppID:     "test-app",
			ID:        4,
		}
		properties := &model.Properties{
			Secret: map[string]string{},
		}
		expected := &jobinfo.SecretInput{
			Name:      "empty-secret",
			Namespace: "default",
			Labels:    map[string]string{spec.LabelManagedBy: config.ManagedByEruun, spec.LabelAppID: "test-app", spec.LabelComponentID: "4", spec.LabelComponentName: "empty-secret"},
			Data:      map[string]string{},
		}
		actual := GenerateSecret(component, properties)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expected %v, but got %v", expected, actual)
		}
	})
}

func TestEqualSecretPayload(t *testing.T) {
	t.Run("same-data", func(t *testing.T) {
		current := &corev1.Secret{
			Type: corev1.SecretTypeOpaque,
			Data: map[string][]byte{
				"token": []byte("abc"),
			},
		}
		desired := &corev1.Secret{
			Type: corev1.SecretTypeOpaque,
			Data: map[string][]byte{
				"token": []byte("abc"),
			},
		}
		if !equalSecretPayload(current, desired) {
			t.Fatal("expected payloads to be equal")
		}
	})

	t.Run("same-length-different-content", func(t *testing.T) {
		current := &corev1.Secret{
			Type: corev1.SecretTypeOpaque,
			Data: map[string][]byte{
				"token": []byte("abc"),
			},
		}
		desired := &corev1.Secret{
			Type: corev1.SecretTypeOpaque,
			Data: map[string][]byte{
				"token": []byte("xyz"),
			},
		}
		if equalSecretPayload(current, desired) {
			t.Fatal("expected payloads to differ when bytes differ")
		}
	})

	t.Run("string-data-equals-existing-data", func(t *testing.T) {
		current := &corev1.Secret{
			Type: corev1.SecretTypeOpaque,
			Data: map[string][]byte{
				"config": []byte("value"),
			},
		}
		desired := &corev1.Secret{
			Type: corev1.SecretTypeOpaque,
			StringData: map[string]string{
				"config": "value",
			},
		}
		if !equalSecretPayload(current, desired) {
			t.Fatal("expected payloads to be equal when stringData matches data")
		}
	})

	t.Run("string-data-override-differs", func(t *testing.T) {
		current := &corev1.Secret{
			Type: corev1.SecretTypeOpaque,
			Data: map[string][]byte{
				"config": []byte("value"),
			},
		}
		desired := &corev1.Secret{
			Type: corev1.SecretTypeOpaque,
			Data: map[string][]byte{
				"config": []byte("value"),
			},
			StringData: map[string]string{
				"config": "override",
			},
		}
		if equalSecretPayload(current, desired) {
			t.Fatal("expected payloads to differ when stringData overrides data")
		}
	})
}
