package job

import (
	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
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
		expected := &model.SecretInput{
			Name:      "my-secret",
			Namespace: "default",
			Labels:    map[string]string{config.LabelManagedBy: config.ManagedByEruun, config.LabelAppID: "test-app", config.LabelComponentID: "1", config.LabelComponentName: "my-secret"},
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
				config.LabelComponentName: "custom-secret",
				"team":                    "security",
			},
			Secret: map[string]string{},
			Conf: map[string]string{
				"config.url":      "http://example.com/config",
				"config.fileName": "my-config-file",
			},
		}
		expected := &model.SecretInput{
			Name:      "my-secret-from-url",
			Namespace: "kube-system",
			URL:       "http://example.com/config",
			FileName:  "my-config-file",
			Labels: map[string]string{
				config.LabelManagedBy:     config.ManagedByEruun,
				config.LabelAppID:         "test-app",
				config.LabelComponentID:   "2",
				config.LabelComponentName: "my-secret-from-url",
				"team":                    "security",
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
		expected := &model.SecretInput{
			Name:      "nil-props-secret",
			Namespace: "default",
			Labels:    map[string]string{config.LabelManagedBy: config.ManagedByEruun, config.LabelAppID: "test-app", config.LabelComponentID: "3", config.LabelComponentName: "nil-props-secret"},
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
		expected := &model.SecretInput{
			Name:      "empty-secret",
			Namespace: "default",
			Labels:    map[string]string{config.LabelManagedBy: config.ManagedByEruun, config.LabelAppID: "test-app", config.LabelComponentID: "4", config.LabelComponentName: "empty-secret"},
			Data:      map[string]string{},
		}
		actual := GenerateSecret(component, properties)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expected %v, but got %v", expected, actual)
		}
	})

	// Test case 5: No namespace provided
	t.Run("NoNamespace", func(t *testing.T) {
		component := &model.ApplicationComponent{
			Name:  "no-namespace-secret",
			AppID: "test-app",
			ID:    5,
		}
		properties := &model.Properties{
			Secret: map[string]string{"key": "value"},
		}
		expected := &model.SecretInput{
			Name:      "no-namespace-secret",
			Namespace: config.DefaultNamespace,
			Labels:    map[string]string{config.LabelManagedBy: config.ManagedByEruun, config.LabelAppID: "test-app", config.LabelComponentID: "5", config.LabelComponentName: "no-namespace-secret"},
			Data:      map[string]string{"key": "value"},
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
