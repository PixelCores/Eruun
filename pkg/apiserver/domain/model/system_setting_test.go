package model

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSystemSetting_EntityContract(t *testing.T) {
	setting := &SystemSetting{Type: SystemSettingTypeURLSecurityPolicy}

	require.Equal(t, "eruun_system_setting", setting.TableName())
	require.Equal(t, SystemSettingTypeURLSecurityPolicy, setting.PrimaryKey())

	index := setting.Index()
	require.Equal(t, SystemSettingTypeURLSecurityPolicy, index["type"])

	require.True(t, builtinModelExists(t, setting.TableName()), "expected system setting model in auto-migration snapshot")
}
