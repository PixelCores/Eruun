package v1

import "time"

// ProgrammingLanguage is an administrator-managed programming language option.
type ProgrammingLanguage struct {
	ID         string    `json:"id"`
	Code       string    `json:"code"`
	Name       string    `json:"name"`
	Version    string    `json:"version"`
	Enabled    bool      `json:"enabled"`
	CPUReq     string    `json:"cpuReq"`
	MemReq     string    `json:"memReq"`
	CreateTime time.Time `json:"createTime"`
	UpdateTime time.Time `json:"updateTime"`
}

// CreateProgrammingLanguageRequest creates a programming language.
type CreateProgrammingLanguageRequest struct {
	Name    string `json:"name" validate:"required"`
	Version string `json:"version" validate:"required"`
	Enabled *bool  `json:"enabled" validate:"required"`
	CPUReq  string `json:"cpuReq,omitempty"`
	MemReq  string `json:"memReq,omitempty"`
}

// UpdateProgrammingLanguageRequest updates mutable programming language fields.
type UpdateProgrammingLanguageRequest struct {
	Name    *string `json:"name,omitempty"`
	Version *string `json:"version,omitempty"`
	Enabled *bool   `json:"enabled,omitempty"`
	CPUReq  *string `json:"cpuReq,omitempty"`
	MemReq  *string `json:"memReq,omitempty"`
}

// ListProgrammingLanguagesResponse lists programming languages.
type ListProgrammingLanguagesResponse struct {
	Languages []*ProgrammingLanguage `json:"languages"`
}

// DeleteProgrammingLanguageResponse reports the deleted programming language id.
type DeleteProgrammingLanguageResponse struct {
	ID string `json:"id"`
}
