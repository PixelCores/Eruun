package model

import "time"

// User is the local account. Provider subjects are stored separately in Identity.
type User struct {
	ID                 string `json:"id" gorm:"primaryKey;type:varchar(36)"`
	Name               string `json:"name" gorm:"type:varchar(128)"`
	PasswordHash       string `json:"-" gorm:"type:varchar(255)"`
	SystemAdmin        bool   `json:"systemAdmin"`
	Disabled           bool   `json:"disabled"`
	MustChangePassword bool   `json:"mustChangePassword"`
	SecurityVersion    uint64 `json:"-"`
	BaseModel
}

type Identity struct {
	ID       string `json:"id" gorm:"primaryKey;type:varchar(36)"`
	UserID   string `json:"-" gorm:"type:varchar(36);not null;uniqueIndex:uidx_identity_user_provider;index"`
	Provider string `json:"provider" gorm:"type:varchar(16);not null;uniqueIndex:uidx_identity_subject;uniqueIndex:uidx_identity_user_provider"`
	Subject  string `json:"subject" gorm:"type:bytes;size:255;not null;uniqueIndex:uidx_identity_subject"`
	BaseModel
}

type Session struct {
	ID              string    `json:"id" gorm:"primaryKey;type:varchar(36)"`
	UserID          string    `json:"-" gorm:"type:varchar(36);not null;index"`
	AccessHash      string    `json:"-" gorm:"type:char(64);not null;uniqueIndex"`
	RefreshHash     string    `json:"-" gorm:"type:char(64);not null;uniqueIndex"`
	AccessExpiresAt time.Time `json:"-"`
	ExpiresAt       time.Time `json:"expiresAt" gorm:"index"`
	AuthenticatedAt time.Time `json:"-"`
	SecurityVersion uint64    `json:"-"`
	BaseModel
}

// OwnerID is the sole source of ownership; owner is an effective membership role.
type Workspace struct {
	ID        string `json:"id" gorm:"primaryKey;type:varchar(36)"`
	Name      string `json:"name" gorm:"type:varchar(128);not null"`
	Kind      string `json:"kind" gorm:"type:varchar(16);not null"`
	OwnerID   string `json:"ownerId" gorm:"type:varchar(36);not null;index"`
	Namespace string `json:"namespace" gorm:"type:varchar(63);not null;uniqueIndex"`
	// PersonalUserID is NULL for teams, providing one personal space per user.
	PersonalUserID *string `json:"-" gorm:"type:varchar(36);uniqueIndex"`
	BaseModel
}

type WorkspaceMember struct {
	ID          string `json:"id" gorm:"primaryKey;type:varchar(36)"`
	WorkspaceID string `json:"workspaceId" gorm:"type:varchar(36);not null;uniqueIndex:uidx_workspace_member"`
	UserID      string `json:"userId" gorm:"type:varchar(36);not null;uniqueIndex:uidx_workspace_member;index"`
	Role        string `json:"role" gorm:"type:varchar(16);not null"`
	BaseModel
}

type WorkspaceInvitation struct {
	ID          string    `json:"id" gorm:"primaryKey;type:varchar(36)"`
	WorkspaceID string    `json:"workspaceId" gorm:"type:varchar(36);not null;uniqueIndex:uidx_workspace_invitation"`
	Email       string    `json:"email" gorm:"type:bytes;size:254;not null;uniqueIndex:uidx_workspace_invitation"`
	Role        string    `json:"role" gorm:"type:varchar(16);not null"`
	TokenHash   string    `json:"-" gorm:"type:char(64);not null;uniqueIndex"`
	ExpiresAt   time.Time `json:"expiresAt"`
	AcceptedBy  string    `json:"acceptedBy,omitempty" gorm:"type:varchar(36)"`
	BaseModel
}

func (m *User) PrimaryKey() string   { return m.ID }
func (*User) TableName() string      { return tableNamePrefix + "users" }
func (*User) ShortTableName() string { return "users" }
func (m *User) Index() map[string]interface{} {
	out := map[string]interface{}{}
	if m.ID != "" {
		out["id"] = m.ID
	}
	return out
}

func (m *Identity) PrimaryKey() string   { return m.ID }
func (*Identity) TableName() string      { return tableNamePrefix + "identities" }
func (*Identity) ShortTableName() string { return "identities" }
func (m *Identity) Index() map[string]interface{} {
	out := map[string]interface{}{}
	if m.ID != "" {
		out["id"] = m.ID
	}
	if m.UserID != "" {
		out["userid"] = m.UserID
	}
	if m.Provider != "" {
		out["provider"] = m.Provider
	}
	if m.Subject != "" {
		out["subject"] = m.Subject
	}
	return out
}

func (m *Session) PrimaryKey() string   { return m.ID }
func (*Session) TableName() string      { return tableNamePrefix + "sessions" }
func (*Session) ShortTableName() string { return "sessions" }
func (m *Session) Index() map[string]interface{} {
	out := map[string]interface{}{}
	if m.ID != "" {
		out["id"] = m.ID
	}
	if m.UserID != "" {
		out["userid"] = m.UserID
	}
	if m.AccessHash != "" {
		out["accesshash"] = m.AccessHash
	}
	if m.RefreshHash != "" {
		out["refreshhash"] = m.RefreshHash
	}
	return out
}

func (m *Workspace) PrimaryKey() string   { return m.ID }
func (*Workspace) TableName() string      { return tableNamePrefix + "workspaces" }
func (*Workspace) ShortTableName() string { return "workspaces" }
func (m *Workspace) Index() map[string]interface{} {
	out := map[string]interface{}{}
	if m.ID != "" {
		out["id"] = m.ID
	}
	if m.OwnerID != "" {
		out["ownerid"] = m.OwnerID
	}
	if m.Kind != "" {
		out["kind"] = m.Kind
	}
	if m.Namespace != "" {
		out["namespace"] = m.Namespace
	}
	return out
}

func (m *WorkspaceMember) PrimaryKey() string   { return m.ID }
func (*WorkspaceMember) TableName() string      { return tableNamePrefix + "workspace_members" }
func (*WorkspaceMember) ShortTableName() string { return "workspace_members" }
func (m *WorkspaceMember) Index() map[string]interface{} {
	out := map[string]interface{}{}
	if m.ID != "" {
		out["id"] = m.ID
	}
	if m.WorkspaceID != "" {
		out["workspaceid"] = m.WorkspaceID
	}
	if m.UserID != "" {
		out["userid"] = m.UserID
	}
	return out
}

func (m *WorkspaceInvitation) PrimaryKey() string   { return m.ID }
func (*WorkspaceInvitation) TableName() string      { return tableNamePrefix + "workspace_invitations" }
func (*WorkspaceInvitation) ShortTableName() string { return "workspace_invitations" }
func (m *WorkspaceInvitation) Index() map[string]interface{} {
	out := map[string]interface{}{}
	if m.ID != "" {
		out["id"] = m.ID
	}
	if m.WorkspaceID != "" {
		out["workspaceid"] = m.WorkspaceID
	}
	if m.Email != "" {
		out["email"] = m.Email
	}
	if m.TokenHash != "" {
		out["tokenhash"] = m.TokenHash
	}
	return out
}
