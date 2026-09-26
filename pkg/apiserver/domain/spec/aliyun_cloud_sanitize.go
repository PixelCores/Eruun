package spec

import (
	"encoding/json"
	"strings"
)

// SanitizeAliyunCloudSettingValue removes secrets from stored settings responses.
func SanitizeAliyunCloudSettingValue(value json.RawMessage) json.RawMessage {
	parsed, err := ParseAliyunCloudSetting(value)
	if err == nil {
		parsed.AccessKeySecret = AliyunCloudSecretMaskedValue
		sanitized, marshalErr := json.Marshal(parsed)
		if marshalErr == nil {
			return json.RawMessage(sanitized)
		}
	}

	var obj map[string]interface{}
	if err := json.Unmarshal(value, &obj); err != nil {
		return json.RawMessage(`{}`)
	}

	maskAliyunCloudSecret(obj)

	sanitized, err := json.Marshal(obj)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return json.RawMessage(sanitized)
}

func maskAliyunCloudSecret(obj map[string]interface{}) {
	if obj == nil {
		return
	}
	for key := range obj {
		if strings.EqualFold(strings.TrimSpace(key), "accessKeySecret") {
			obj[key] = AliyunCloudSecretMaskedValue
		}
	}
}
