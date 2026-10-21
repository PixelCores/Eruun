package application

import (
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

func applicationResourceNameKey(app *model.Applications) string {
	if app == nil {
		return ""
	}
	// Runtime Kubernetes resource names are derived from the application name
	// only. Template version is catalog identity, not workload identity.
	return naming.ApplicationResourceKey(app.Name)
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
