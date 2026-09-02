package spec

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeAliyunCloudSettingValueCanonicalizesAliases(t *testing.T) {
	normalized, err := NormalizeAliyunCloudSettingValue(json.RawMessage(`{
		"access_key_id": " test-ak ",
		"access_key_secret": " test-sk ",
		"endpoint": " nas.cn-hangzhou.aliyuncs.com ",
		"region_id": " cn-hangzhou ",
		"zone_id": " cn-hangzhou-i ",
		"vpc_id": " vpc-001 ",
		"vsw_id": " vsw-001 "
	}`))
	require.NoError(t, err)
	require.JSONEq(t, `{
		"accessKeyId": "test-ak",
		"accessKeySecret": "test-sk",
		"endpoint": "nas.cn-hangzhou.aliyuncs.com",
		"regionId": "cn-hangzhou",
		"zoneId": "cn-hangzhou-i",
		"vpcId": "vpc-001",
		"vswId": "vsw-001"
	}`, string(normalized))
}

func TestNormalizeAliyunCloudSettingValueRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		message string
	}{
		{
			name: "region endpoint alias",
			raw: `{
				"accessKeyId": "test-ak",
				"accessKeySecret": "test-sk",
				"regionId": "cn-hangzhou",
				"region_endpoint": "nas.cn-hangzhou.aliyuncs.com"
			}`,
			message: "not supported",
		},
		{
			name: "canonical alias conflict",
			raw: `{
				"accessKeyId": "test-ak",
				"access_key_id": "other-ak",
				"accessKeySecret": "test-sk",
				"regionId": "cn-hangzhou"
			}`,
			message: "conflicts",
		},
		{
			name: "masked secret",
			raw: `{
				"accessKeyId": "test-ak",
				"accessKeySecret": "******",
				"regionId": "cn-hangzhou"
			}`,
			message: "redacted placeholder",
		},
		{
			name: "unknown field",
			raw: `{
				"accessKeyId": "test-ak",
				"accessKeySecret": "test-sk",
				"regionId": "cn-hangzhou",
				"foo": "bar"
			}`,
			message: "unsupported aliyunCloud field",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NormalizeAliyunCloudSettingValue(json.RawMessage(tc.raw))
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.message)
		})
	}
}
