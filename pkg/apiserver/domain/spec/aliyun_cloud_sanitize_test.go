package spec

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSanitizeAliyunCloudSettingValue(t *testing.T) {
	for _, tc := range []struct{ name, input, want string }{
		{"valid", `{"accessKeyId":"test-ak","accessKeySecret":"test-sk","regionId":"cn-hangzhou"}`, `{"accessKeyId":"test-ak","accessKeySecret":"******","regionId":"cn-hangzhou"}`},
		{"invalid stored setting", `{"AccessKeySecret":"test-sk","unsupported":true}`, `{"AccessKeySecret":"******","unsupported":true}`},
		{"malformed", `{`, `{}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.JSONEq(t, tc.want, string(SanitizeAliyunCloudSettingValue(json.RawMessage(tc.input))))
		})
	}
}
