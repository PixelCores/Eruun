package application

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

const namespaceAutoCreatedValue = "true"

func (c *applicationsServiceImpl) ensureApplicationNamespace(ctx context.Context, app *model.Applications) error {
	if app == nil {
		return nil
	}
	namespace := pickNamespace(app.Namespace, config.DefaultNamespace)
	app.Namespace = namespace
	if namespace == config.DefaultNamespace {
		return nil
	}
	if c.KubeClient == nil {
		return fmt.Errorf("kubernetes client is not initialized for namespace %q", namespace)
	}

	cli := c.KubeClient.CoreV1().Namespaces()
	if _, err := cli.Get(ctx, namespace, metav1.GetOptions{}); err == nil {
		return nil
	} else if !k8serrors.IsNotFound(err) {
		return fmt.Errorf("get namespace %q: %w", namespace, err)
	}

	annotations := map[string]string{
		config.AnnotationNamespaceAutoCreated: namespaceAutoCreatedValue,
		config.AnnotationNamespaceOwnerAppID:  strings.TrimSpace(app.ID),
	}
	labels := map[string]string{
		config.LabelManagedBy: config.ManagedByEruun,
	}
	if _, err := cli.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:        namespace,
			Labels:      labels,
			Annotations: annotations,
		},
	}, metav1.CreateOptions{}); err != nil && !k8serrors.IsAlreadyExists(err) {
		return fmt.Errorf("create namespace %q: %w", namespace, err)
	}
	klog.Infof("namespace %q created automatically for app %s", namespace, app.ID)
	return nil
}
