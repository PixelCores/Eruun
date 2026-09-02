package model

// SystemInfo systemInfo model
type SystemInfo struct {
	BaseModel
	InstallID string `json:"installID" gorm:"primaryKey;type:varchar(64);column:install_id"` //安装ID，主键
}

// TableName return custom table name
func (u *SystemInfo) TableName() string {
	return tableNamePrefix + "system_info"
}

// ShortTableName is the compressed version of table name for kubeapi storage and others
func (u *SystemInfo) ShortTableName() string {
	return "sys"
}

// PrimaryKey return custom primary key
func (u *SystemInfo) PrimaryKey() string {
	return u.InstallID
}

// Index return custom index
func (u *SystemInfo) Index() map[string]interface{} {
	index := make(map[string]interface{})
	if u.InstallID != "" {
		index["install_id"] = u.InstallID
	}
	return index
}
