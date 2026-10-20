package jobtest

import (
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

func ComponentMatchesConditions(component *model.ApplicationComponent, conditions map[string]interface{}) bool {
	if component == nil {
		return false
	}
	if appID, ok := conditions["app_id"].(string); ok && component.AppID != appID {
		return false
	}
	if name, ok := conditions["name"].(string); ok && component.Name != name {
		return false
	}
	if id, ok := conditions["id"].(int); ok && component.ID != id {
		return false
	}
	return true
}

func ApplyComponentRuntimeUpdateMap(component *model.ApplicationComponent, updates map[string]interface{}) {
	if component == nil {
		return
	}
	for key, value := range updates {
		switch key {
		case "status":
			if status, ok := value.(string); ok {
				component.Status = status
			}
		case "ready_replicas":
			if readyReplicas, ok := value.(int32); ok {
				component.ReadyReplicas = readyReplicas
			}
		case "last_abnormal":
			if lastAbnormal, ok := value.(string); ok {
				component.LastAbnormal = lastAbnormal
			}
		}
	}
}
