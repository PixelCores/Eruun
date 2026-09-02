package application

import (
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

func applicationResourceNameKey(app *model.Applications) string {
	if app == nil {
		return ""
	}
	// Runtime Kubernetes resource names are derived from the application name
	// only. Template version is catalog identity, not workload identity.
	return naming.ApplicationResourceKey(app.Name, "", false)
}

func setResourceAppNameForComponents(components []*model.ApplicationComponent, resourceAppName string) {
	for _, component := range components {
		setResourceAppNameForComponent(component, resourceAppName)
	}
}

func setResourceAppNameForComponent(component *model.ApplicationComponent, resourceAppName string) {
	if component == nil {
		return
	}
	component.ResourceAppName = resourceAppName
}

func serviceNamespaceOrDefault(namespace string) string {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return config.DefaultNamespace
	}
	return namespace
}
