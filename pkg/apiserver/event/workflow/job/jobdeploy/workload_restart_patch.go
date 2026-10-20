package jobdeploy

import (
	"encoding/json"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
)

func buildWorkloadRestartPatch(restartedAt string) ([]byte, error) {
	patch := map[string]interface{}{
		"spec": map[string]interface{}{
			"template": map[string]interface{}{
				"metadata": map[string]interface{}{
					"annotations": map[string]string{
						config.AnnotationWorkloadRestartAt: restartedAt,
					},
				},
			},
		},
	}
	return json.Marshal(patch)
}
