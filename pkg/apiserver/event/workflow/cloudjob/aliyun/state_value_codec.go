package aliyun

import (
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/contracts"
)

func cloudResultString(result *contracts.CloudJobResult, key string) string {
	if result == nil {
		return ""
	}
	return cloudMapString(result.Output, key)
}

func cloudResultValue(result *contracts.CloudJobResult, key string) (interface{}, bool) {
	if result == nil {
		return nil, false
	}
	return cloudMapValue(result.Output, key)
}

func cloudMapString(values map[string]interface{}, key string) string {
	raw, ok := cloudMapValue(values, key)
	if !ok {
		return ""
	}
	if raw == nil {
		return ""
	}
	switch v := raw.(type) {
	case string:
		return strings.TrimSpace(v)
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", v))
	}
}

func cloudMapValue(values map[string]interface{}, key string) (interface{}, bool) {
	if len(values) == 0 {
		return nil, false
	}
	raw, ok := values[key]
	if !ok {
		return nil, false
	}
	return raw, true
}

func normalizeStatus(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func cloudValuePresent(value interface{}) bool {
	if value == nil {
		return false
	}
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v) != ""
	case bool:
		return v
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", v)) != ""
	}
}
