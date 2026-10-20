package jobdeploy

import (
	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
)

var eruunSystemLabelKeys = []string{
	spec.LabelManagedBy,
	spec.LabelAppID,
	spec.LabelComponentID,
	spec.LabelComponentName,
	spec.LabelShareName,
	spec.LabelShareStrategy,
}

var eruunWorkloadTemplateAnnotationKeys = []string{
	config.AnnotationJobTaskID,
	config.AnnotationWorkloadRestartAt,
}

func preserveStringMapKeys(current, desired map[string]string, keys []string) map[string]string {
	values := utils.CopyStringMap(desired)
	for _, key := range keys {
		val, ok := current[key]
		if !ok {
			continue
		}
		if values == nil {
			values = make(map[string]string, 1)
		}
		if _, exists := values[key]; !exists {
			values[key] = val
		}
	}
	return values
}
