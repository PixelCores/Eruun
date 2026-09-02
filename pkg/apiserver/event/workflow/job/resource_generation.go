package job

import (
	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

func generatedResourceIdentity(component *model.ApplicationComponent) (string, string) {
	name := component.Name
	namespace := component.Namespace
	if namespace == "" {
		namespace = config.DefaultNamespace
	}
	return name, namespace
}

func externalConfigFileInput(properties *model.Properties, enabled bool) (string, string, bool) {
	if !enabled || properties == nil || properties.Conf == nil {
		return "", "", false
	}
	url := properties.Conf["config.url"]
	if url == "" {
		return "", "", false
	}
	fileName := properties.Conf["config.fileName"]
	if fileName == "" {
		fileName = "config"
	}
	return url, fileName, true
}

func keyValueDataOrNil(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	return values
}
