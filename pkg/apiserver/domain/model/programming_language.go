package model

// ProgrammingLanguage stores administrator-managed programming language options.
type ProgrammingLanguage struct {
	ID      string `json:"id" gorm:"primaryKey;type:varchar(64);column:id"`
	Code    string `json:"code" gorm:"type:varchar(64);not null;uniqueIndex:idx_programming_language_code_version,priority:1;column:code"`
	Name    string `json:"name" gorm:"type:varchar(128);not null;column:name"`
	Version string `json:"version" gorm:"type:varchar(64);not null;uniqueIndex:idx_programming_language_code_version,priority:2;column:version"`
	Enabled *bool  `json:"enabled" gorm:"not null;column:enabled"`
	CPUReq  string `json:"cpuReq" gorm:"type:varchar(32);column:cpu_req"`
	MemReq  string `json:"memReq" gorm:"type:varchar(32);column:mem_req"`
	BaseModel
}

func (p *ProgrammingLanguage) PrimaryKey() string {
	return p.ID
}

func (p *ProgrammingLanguage) TableName() string {
	return tableNamePrefix + "programming_languages"
}

func (p *ProgrammingLanguage) ShortTableName() string {
	return "programming_language"
}

func (p *ProgrammingLanguage) Index() map[string]interface{} {
	index := make(map[string]interface{})
	if p.ID != "" {
		index["id"] = p.ID
	}
	if p.Code != "" {
		index["code"] = p.Code
	}
	if p.Version != "" {
		index["version"] = p.Version
	}
	return index
}
