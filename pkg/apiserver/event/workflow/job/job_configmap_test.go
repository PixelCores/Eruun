package job

import (
	"context"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

func TestGenerateConfigMap(t *testing.T) {
	// Test case 1: Basic ConfigMap generation
	t.Run("BasicConfigMap", func(t *testing.T) {
		component := &model.ApplicationComponent{
			Name:      "my-configmap",
			Namespace: "default",
			AppID:     "test-app",
			ID:        1,
		}
		properties := &model.Properties{
			Conf: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
		}
		expected := &model.ConfigMapInput{
			Name:      "my-configmap",
			Namespace: "default",
			Labels:    map[string]string{config.LabelManagedBy: config.ManagedByEruun, config.LabelAppID: "test-app", config.LabelComponentID: "1", config.LabelComponentName: "my-configmap"},
			Data: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
		}
		actual := GenerateConfigMap(component, properties)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expected %v, but got %v", expected, actual)
		}
	})

	// Test case 2: ConfigMap generation from URL
	t.Run("ConfigMapFromURL", func(t *testing.T) {
		component := &model.ApplicationComponent{
			Name:      "my-configmap-from-url",
			Namespace: "kube-system",
			AppID:     "test-app",
			ID:        2,
		}
		properties := &model.Properties{
			Labels: map[string]string{
				config.LabelAppID: "evil-app",
				"team":            "platform",
			},
			Conf: map[string]string{
				"config.url":      "http://example.com/config.txt",
				"config.fileName": "my-config-file.txt",
			},
		}
		expected := &model.ConfigMapInput{
			Name:      "my-configmap-from-url",
			Namespace: "kube-system",
			URL:       "http://example.com/config.txt",
			FileName:  "my-config-file.txt",
			Labels: map[string]string{
				config.LabelManagedBy:     config.ManagedByEruun,
				config.LabelAppID:         "test-app",
				config.LabelComponentID:   "2",
				config.LabelComponentName: "my-configmap-from-url",
				"team":                    "platform",
			},
		}
		actual := GenerateConfigMap(component, properties)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expected %v, but got %v", expected, actual)
		}
	})

	// Test case 3: Nil properties
	t.Run("NilProperties", func(t *testing.T) {
		component := &model.ApplicationComponent{
			Name:      "nil-props-configmap",
			Namespace: "default",
			AppID:     "test-app",
			ID:        3,
		}
		expected := &model.ConfigMapInput{
			Name:      "nil-props-configmap",
			Namespace: "default",
			Labels:    map[string]string{config.LabelManagedBy: config.ManagedByEruun, config.LabelAppID: "test-app", config.LabelComponentID: "3", config.LabelComponentName: "nil-props-configmap"},
			Data:      nil,
		}
		actual := GenerateConfigMap(component, nil)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expected %v, but got %v", expected, actual)
		}
	})

	// Test case 4: Empty ConfigMap data
	t.Run("EmptyConfigMapData", func(t *testing.T) {
		component := &model.ApplicationComponent{
			Name:      "empty-configmap",
			Namespace: "default",
			AppID:     "test-app",
			ID:        4,
		}
		properties := &model.Properties{
			Conf: map[string]string{},
		}
		expected := &model.ConfigMapInput{
			Name:      "empty-configmap",
			Namespace: "default",
			Labels:    map[string]string{config.LabelManagedBy: config.ManagedByEruun, config.LabelAppID: "test-app", config.LabelComponentID: "4", config.LabelComponentName: "empty-configmap"},
			Data:      map[string]string{},
		}
		actual := GenerateConfigMap(component, properties)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expected %v, but got %v", expected, actual)
		}
	})

	// Test case 5: No namespace provided
	t.Run("NoNamespace", func(t *testing.T) {
		component := &model.ApplicationComponent{
			Name:  "no-namespace-configmap",
			AppID: "test-app",
			ID:    5,
		}
		properties := &model.Properties{
			Conf: map[string]string{"key": "value"},
		}
		expected := &model.ConfigMapInput{
			Name:      "no-namespace-configmap",
			Namespace: config.DefaultNamespace,
			Labels:    map[string]string{config.LabelManagedBy: config.ManagedByEruun, config.LabelAppID: "test-app", config.LabelComponentID: "5", config.LabelComponentName: "no-namespace-configmap"},
			Data:      map[string]string{"key": "value"},
		}
		actual := GenerateConfigMap(component, properties)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expected %v, but got %v", expected, actual)
		}
	})
}

func TestDeployConfigMapJobCtl_CreateAlreadyExists(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.Fake.PrependReactor("create", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
		createAction, ok := action.(k8stesting.CreateAction)
		if !ok {
			return false, nil, nil
		}
		obj, ok := createAction.GetObject().(*corev1.ConfigMap)
		if !ok {
			return false, nil, nil
		}
		_ = client.Tracker().Add(obj)
		return true, obj, k8serrors.NewAlreadyExists(corev1.Resource("configmaps"), obj.Name)
	})

	jobTask := &model.JobTask{
		Name:      "app-config",
		Namespace: "ops",
		AppID:     "app-1",
		JobType:   string(config.JobDeployConfigMap),
		JobInfo: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "app-config",
				Namespace: "ops",
			},
			Data: map[string]string{"foo": "bar"},
		},
	}
	ctl := NewDeployConfigMapJobCtl(jobTask, client, &noopStore{}, func() {}, locker.NewNoopLocker(shareLockerPrefix), nil)
	ctx := WithCleanupTracker(context.Background())

	if err := ctl.Run(ctx); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	created, err := client.CoreV1().ConfigMaps("ops").Get(context.Background(), "app-config", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected configmap to exist: %v", err)
	}
	if created.Data["foo"] != "bar" {
		t.Fatalf("expected configmap data to be preserved, got %v", created.Data)
	}
}
