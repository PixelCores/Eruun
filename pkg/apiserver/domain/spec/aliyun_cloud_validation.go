package spec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

type aliyunCloudField struct {
	canonical string
	aliases   []string
	apply     func(*AliyunCloudSettingSpec, string)
}

var aliyunCloudFields = []aliyunCloudField{
	{
		canonical: "accessKeyId",
		aliases:   []string{"access_key_id"},
		apply: func(setting *AliyunCloudSettingSpec, value string) {
			setting.AccessKeyID = value
		},
	},
	{
		canonical: "accessKeySecret",
		aliases:   []string{"access_key_secret"},
		apply: func(setting *AliyunCloudSettingSpec, value string) {
			setting.AccessKeySecret = value
		},
	},
	{
		canonical: "endpoint",
		apply: func(setting *AliyunCloudSettingSpec, value string) {
			setting.Endpoint = value
		},
	},
	{
		canonical: "regionId",
		aliases:   []string{"region_id"},
		apply: func(setting *AliyunCloudSettingSpec, value string) {
			setting.RegionID = value
		},
	},
	{
		canonical: "zoneId",
		aliases:   []string{"zone_id"},
		apply: func(setting *AliyunCloudSettingSpec, value string) {
			setting.ZoneID = value
		},
	},
	{
		canonical: "vpcId",
		aliases:   []string{"vpc_id"},
		apply: func(setting *AliyunCloudSettingSpec, value string) {
			setting.VpcID = value
		},
	},
	{
		canonical: "vswId",
		aliases:   []string{"vsw_id"},
		apply: func(setting *AliyunCloudSettingSpec, value string) {
			setting.VSwitchID = value
		},
	},
}

// NormalizeAliyunCloudSetting trims all scalar fields.
func NormalizeAliyunCloudSetting(setting AliyunCloudSettingSpec) AliyunCloudSettingSpec {
	setting.AccessKeyID = strings.TrimSpace(setting.AccessKeyID)
	setting.AccessKeySecret = strings.TrimSpace(setting.AccessKeySecret)
	setting.Endpoint = strings.TrimSpace(setting.Endpoint)
	setting.RegionID = strings.TrimSpace(setting.RegionID)
	setting.ZoneID = strings.TrimSpace(setting.ZoneID)
	setting.VpcID = strings.TrimSpace(setting.VpcID)
	setting.VSwitchID = strings.TrimSpace(setting.VSwitchID)
	return setting
}

// NormalizeAliyunCloudSettingValue canonicalizes aliases and stores only camelCase fields.
func NormalizeAliyunCloudSettingValue(value json.RawMessage) (json.RawMessage, error) {
	trimmed := bytes.TrimSpace(value)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("aliyunCloud value is empty")
	}

	var raw map[string]interface{}
	if err := json.Unmarshal(trimmed, &raw); err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, fmt.Errorf("aliyunCloud must be a JSON object")
	}

	allowedKeys := make(map[string]struct{}, len(aliyunCloudFields)*2)
	for _, field := range aliyunCloudFields {
		allowedKeys[field.canonical] = struct{}{}
		for _, alias := range field.aliases {
			allowedKeys[alias] = struct{}{}
		}
	}
	for key := range raw {
		switch strings.TrimSpace(key) {
		case "region_endpoint", "regionEndpoint":
			return nil, fmt.Errorf("aliyunCloud field %q is not supported; use endpoint", key)
		}
		if _, ok := allowedKeys[key]; !ok {
			return nil, fmt.Errorf("unsupported aliyunCloud field %q", key)
		}
	}

	var setting AliyunCloudSettingSpec
	for _, field := range aliyunCloudFields {
		value, err := resolveAliyunCloudField(raw, field.canonical, field.aliases...)
		if err != nil {
			return nil, err
		}
		field.apply(&setting, value)
	}

	setting = NormalizeAliyunCloudSetting(setting)
	if err := ValidateAliyunCloudSetting(setting); err != nil {
		return nil, err
	}

	normalized, err := json.Marshal(setting)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(normalized), nil
}

// ParseAliyunCloudSetting loads and validates aliyunCloud config from raw JSON.
func ParseAliyunCloudSetting(value json.RawMessage) (AliyunCloudSettingSpec, error) {
	normalized, err := NormalizeAliyunCloudSettingValue(value)
	if err != nil {
		return AliyunCloudSettingSpec{}, err
	}
	var setting AliyunCloudSettingSpec
	if err := json.Unmarshal(normalized, &setting); err != nil {
		return AliyunCloudSettingSpec{}, err
	}
	return NormalizeAliyunCloudSetting(setting), nil
}

// ValidateAliyunCloudSetting validates required aliyunCloud fields.
func ValidateAliyunCloudSetting(setting AliyunCloudSettingSpec) error {
	setting = NormalizeAliyunCloudSetting(setting)
	if setting.AccessKeyID == "" {
		return fmt.Errorf("accessKeyId is required")
	}
	if setting.AccessKeySecret == "" {
		return fmt.Errorf("accessKeySecret is required")
	}
	if setting.AccessKeySecret == AliyunCloudSecretMaskedValue {
		return fmt.Errorf("accessKeySecret cannot use redacted placeholder")
	}
	if setting.RegionID == "" {
		return fmt.Errorf("regionId is required")
	}
	return nil
}

func resolveAliyunCloudField(raw map[string]interface{}, canonical string, aliases ...string) (string, error) {
	canonicalValue, hasCanonical, err := lookupAliyunCloudString(raw, canonical)
	if err != nil {
		return "", err
	}
	if !hasCanonical {
		for _, alias := range aliases {
			aliasValue, hasAlias, lookupErr := lookupAliyunCloudString(raw, alias)
			if lookupErr != nil {
				return "", lookupErr
			}
			if hasAlias {
				return aliasValue, nil
			}
		}
		return "", nil
	}
	for _, alias := range aliases {
		aliasValue, hasAlias, lookupErr := lookupAliyunCloudString(raw, alias)
		if lookupErr != nil {
			return "", lookupErr
		}
		if hasAlias && aliasValue != canonicalValue {
			return "", fmt.Errorf("aliyunCloud field %q conflicts with alias %q", canonical, alias)
		}
	}
	return canonicalValue, nil
}

func lookupAliyunCloudString(raw map[string]interface{}, key string) (string, bool, error) {
	value, ok := raw[key]
	if !ok {
		return "", false, nil
	}
	if value == nil {
		return "", true, fmt.Errorf("aliyunCloud field %q must be a string", key)
	}
	str, ok := value.(string)
	if !ok {
		return "", true, fmt.Errorf("aliyunCloud field %q must be a string", key)
	}
	return strings.TrimSpace(str), true, nil
}
