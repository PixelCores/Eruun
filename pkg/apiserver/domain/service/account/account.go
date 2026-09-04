package account

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Delivery interface {
	SendCode(context.Context, string, string, string) error
	SendInvitation(context.Context, string, string) error
}

type Service struct {
	Repo       repository.Accounts
	Config     *spec.AccountConfig
	Redis      *redis.Client
	Delivery   Delivery
	HTTPClient *http.Client
	Now        func() time.Time
}

func New(store datastore.DataStore, cfg *spec.AccountConfig, r *redis.Client, delivery Delivery) *Service {
	return &Service{Repo: repository.Accounts{Store: store}, Config: cfg, Redis: r, Delivery: delivery, HTTPClient: &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, Now: time.Now}
}

type Login struct {
	AccessToken  string      `json:"accessToken"`
	TokenType    string      `json:"tokenType"`
	ExpiresIn    int64       `json:"expiresIn"`
	User         *model.User `json:"user"`
	RefreshToken string      `json:"-"`
}

type Principal struct {
	User    *model.User
	Session *model.Session
}

func (p *Principal) Recent(now time.Time) bool {
	return p != nil && p.Session != nil && now.Sub(p.Session.AuthenticatedAt) <= recentAuthTTL
}

func (s *Service) Bootstrap(ctx context.Context) error {
	if s.Config == nil || s.Redis == nil || s.Delivery == nil {
		return fmt.Errorf("authentication requires config, Redis and delivery adapter")
	}
	if err := s.Redis.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("authentication Redis: %w", err)
	}
	count, err := s.Repo.Store.Count(ctx, &model.User{}, nil)
	if err != nil {
		return err
	}
	if count > 0 {
		return nil
	}
	id, err := NormalizeIdentity("email", s.Config.BootstrapAdmin.Email)
	if err != nil {
		return fmt.Errorf("empty account database requires bootstrap administrator")
	}
	hash, err := hashPassword(s.Config.BootstrapAdmin.Password)
	if err != nil {
		return err
	}
	return s.Repo.Transaction(ctx, func(r repository.Accounts) error {
		// A fixed marker serializes concurrent first boots without electing a public user.
		marker := &model.SystemSetting{Type: "accounts.bootstrap.v1", Value: []byte(`{"completed":true}`)}
		if err := r.Store.Add(ctx, marker); err != nil {
			if errors.Is(err, datastore.ErrRecordExist) {
				return nil
			}
			return err
		}
		_, err := s.createUser(ctx, r, "email", id, hash, "Administrator", true)
		return err
	})
}

func (s *Service) createUser(ctx context.Context, r repository.Accounts, provider, subject, password, name string, admin bool) (*model.User, error) {
	if len([]rune(name)) > 128 {
		return nil, bcode.ErrAccountInput
	}
	u := &model.User{ID: uuid.NewString(), Name: strings.TrimSpace(name), PasswordHash: password, SystemAdmin: admin, MustChangePassword: admin}
	space := &model.Workspace{ID: uuid.NewString(), Name: "Personal", Kind: "personal", OwnerID: u.ID, PersonalUserID: &u.ID}
	space.Namespace = "eruun-ws-" + strings.ReplaceAll(space.ID, "-", "")
	for _, e := range []datastore.Entity{u, &model.Identity{ID: uuid.NewString(), UserID: u.ID, Provider: provider, Subject: subject}, space, &model.WorkspaceMember{ID: uuid.NewString(), WorkspaceID: space.ID, UserID: u.ID, Role: "admin"}} {
		if err := r.Store.Add(ctx, e); err != nil {
			return nil, accountConflict(err)
		}
	}
	return u, nil
}

func (s *Service) Register(ctx context.Context, provider, subject, code, password, name string) (*Login, error) {
	id, err := NormalizeIdentity(provider, subject)
	if err != nil {
		return nil, err
	}
	hash, err := hashPassword(password)
	if err != nil {
		return nil, err
	}
	if err = s.consumeCode(ctx, "register", provider, id, code); err != nil {
		return nil, err
	}
	var result *Login
	err = s.Repo.Transaction(ctx, func(r repository.Accounts) error {
		u, e := s.createUser(ctx, r, provider, id, hash, name, false)
		if e != nil {
			return e
		}
		result, e = s.issueSession(ctx, r, u)
		return e
	})
	return result, err
}

func (s *Service) Login(ctx context.Context, provider, subject, password, code string) (*Login, error) {
	id, err := NormalizeIdentity(provider, subject)
	if err != nil {
		return nil, bcode.ErrUnauthorized
	}
	if (password == "") == (code == "") {
		return nil, bcode.ErrAccountInput
	}
	if code != "" {
		if err = s.consumeCode(ctx, "login", provider, id, code); err != nil {
			return nil, err
		}
	}
	identity := &model.Identity{Provider: provider, Subject: id}
	if err = s.Repo.One(ctx, identity); err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrUnauthorized
		}
		return nil, err
	}
	var result *Login
	err = s.Repo.Transaction(ctx, func(r repository.Accounts) error {
		u := &model.User{ID: identity.UserID}
		if e := r.Lock(ctx, u); e != nil {
			return e
		}
		if u.Disabled {
			return bcode.ErrUnauthorized
		}
		// Re-read the identity under the same user lock used by unlink.
		current := &model.Identity{ID: identity.ID}
		if e := r.One(ctx, current); e != nil {
			return bcode.ErrUnauthorized
		}
		if password != "" && !verifyPassword(u.PasswordHash, password) {
			return bcode.ErrUnauthorized
		}
		var e error
		result, e = s.issueSession(ctx, r, u)
		return e
	})
	return result, err
}

func (s *Service) issueSession(ctx context.Context, r repository.Accounts, u *model.User) (*Login, error) {
	a, err := randomToken()
	if err != nil {
		return nil, err
	}
	refresh, err := randomToken()
	if err != nil {
		return nil, err
	}
	now := s.Now().UTC()
	session := &model.Session{ID: uuid.NewString(), UserID: u.ID, AccessHash: tokenHash(a), RefreshHash: tokenHash(refresh), AccessExpiresAt: now.Add(accessTTL), ExpiresAt: now.Add(sessionTTL), AuthenticatedAt: now, SecurityVersion: u.SecurityVersion}
	if err = r.Store.Add(ctx, session); err != nil {
		return nil, fmt.Errorf("create session: %w", err)
	}
	return &Login{AccessToken: a, TokenType: "Bearer", ExpiresIn: int64(accessTTL.Seconds()), User: u, RefreshToken: refresh}, nil
}

func (s *Service) Authenticate(ctx context.Context, token string) (*Principal, error) {
	if len(token) != 43 {
		return nil, bcode.ErrUnauthorized
	}
	session := &model.Session{AccessHash: tokenHash(token)}
	if err := s.Repo.One(ctx, session); err != nil {
		return nil, authLookupError(err)
	}
	u := &model.User{ID: session.UserID}
	if err := s.Repo.Store.Get(ctx, u); err != nil {
		return nil, authLookupError(err)
	}
	now := s.Now()
	if u.Disabled || u.SecurityVersion != session.SecurityVersion || !session.AccessExpiresAt.After(now) || !session.ExpiresAt.After(now) {
		return nil, bcode.ErrUnauthorized
	}
	return &Principal{User: u, Session: session}, nil
}

func (s *Service) Refresh(ctx context.Context, token string) (*Login, error) {
	if len(token) != 43 {
		return nil, bcode.ErrUnauthorized
	}
	session := &model.Session{RefreshHash: tokenHash(token)}
	if err := s.Repo.One(ctx, session); err != nil {
		return nil, authLookupError(err)
	}
	var result *Login
	err := s.Repo.Transaction(ctx, func(r repository.Accounts) error {
		u := &model.User{ID: session.UserID}
		if e := r.Lock(ctx, u); e != nil {
			return e
		}
		if u.Disabled || u.SecurityVersion != session.SecurityVersion || !session.ExpiresAt.After(s.Now()) {
			return bcode.ErrUnauthorized
		}
		a, e := randomToken()
		if e != nil {
			return e
		}
		refresh, e := randomToken()
		if e != nil {
			return e
		}
		ok, e := r.Store.CompareAndSwap(ctx, session, "refreshhash", tokenHash(token), map[string]interface{}{"accesshash": tokenHash(a), "refreshhash": tokenHash(refresh), "accessexpiresat": s.Now().UTC().Add(accessTTL)})
		if e != nil {
			return e
		}
		if !ok {
			return bcode.ErrUnauthorized
		}
		result = &Login{AccessToken: a, TokenType: "Bearer", ExpiresIn: int64(accessTTL.Seconds()), User: u, RefreshToken: refresh}
		return nil
	})
	return result, err
}

func (s *Service) Logout(ctx context.Context, p *Principal) error {
	return s.Repo.Store.Delete(ctx, p.Session)
}

func (s *Service) ChangePassword(ctx context.Context, p *Principal, password string) error {
	if !p.Recent(s.Now()) {
		return bcode.ErrAccountRecentAuth
	}
	if p.User.MustChangePassword && verifyPassword(p.User.PasswordHash, password) {
		return bcode.ErrAccountInput
	}
	hash, err := hashPassword(password)
	if err != nil {
		return err
	}
	return s.setPassword(ctx, p.User.ID, hash, p.User.SecurityVersion, nil)
}

func (s *Service) ResetPassword(ctx context.Context, provider, subject, code, password string) error {
	id, err := NormalizeIdentity(provider, subject)
	if err != nil {
		return err
	}
	hash, err := hashPassword(password)
	if err != nil {
		return err
	}
	if err = s.consumeCode(ctx, "reset", provider, id, code); err != nil {
		return err
	}
	i := &model.Identity{Provider: provider, Subject: id}
	if err = s.Repo.One(ctx, i); err != nil {
		return authLookupError(err)
	}
	return s.setPassword(ctx, i.UserID, hash, ^uint64(0), i)
}

func (s *Service) setPassword(ctx context.Context, userID, hash string, expectedVersion uint64, identity *model.Identity) error {
	return s.Repo.Transaction(ctx, func(r repository.Accounts) error {
		u := &model.User{ID: userID}
		if e := r.Lock(ctx, u); e != nil {
			return e
		}
		if u.Disabled || (expectedVersion != ^uint64(0) && u.SecurityVersion != expectedVersion) {
			return bcode.ErrUnauthorized
		}
		if identity != nil {
			current := &model.Identity{ID: identity.ID, UserID: u.ID, Provider: identity.Provider, Subject: identity.Subject}
			if e := r.One(ctx, current); e != nil {
				return bcode.ErrUnauthorized
			}
		}
		if e := r.Update(ctx, u, map[string]interface{}{"passwordhash": hash, "mustchangepassword": false, "securityversion": u.SecurityVersion + 1}); e != nil {
			return e
		}
		return r.Store.DeleteByFilter(ctx, &model.Session{UserID: u.ID}, nil)
	})
}

func (s *Service) Identities(ctx context.Context, p *Principal) ([]datastore.Entity, error) {
	return s.Repo.Store.List(ctx, &model.Identity{UserID: p.User.ID}, &datastore.ListOptions{})
}

func (s *Service) Bind(ctx context.Context, p *Principal, provider, subject, code string) error {
	if !p.Recent(s.Now()) {
		return bcode.ErrAccountRecentAuth
	}
	id, err := NormalizeIdentity(provider, subject)
	if err != nil {
		return err
	}
	if err = s.consumeCode(ctx, "bind", provider, id, code); err != nil {
		return err
	}
	return s.Repo.Transaction(ctx, func(r repository.Accounts) error {
		u := &model.User{ID: p.User.ID}
		if e := r.Lock(ctx, u); e != nil {
			return e
		}
		if u.Disabled || u.SecurityVersion != p.User.SecurityVersion {
			return bcode.ErrUnauthorized
		}
		return accountConflict(r.Store.Add(ctx, &model.Identity{ID: uuid.NewString(), UserID: u.ID, Provider: provider, Subject: id}))
	})
}

func (s *Service) Unbind(ctx context.Context, p *Principal, id string) error {
	if !p.Recent(s.Now()) {
		return bcode.ErrAccountRecentAuth
	}
	return s.Repo.Transaction(ctx, func(r repository.Accounts) error {
		u := &model.User{ID: p.User.ID}
		if e := r.Lock(ctx, u); e != nil {
			return e
		}
		if u.Disabled || u.SecurityVersion != p.User.SecurityVersion {
			return bcode.ErrUnauthorized
		}
		i := &model.Identity{ID: id, UserID: u.ID}
		if e := r.One(ctx, i); e != nil {
			return e
		}
		identities, e := r.Store.List(ctx, &model.Identity{UserID: u.ID}, &datastore.ListOptions{})
		if e != nil {
			return e
		}
		available := false
		for _, row := range identities {
			other := row.(*model.Identity)
			if other.ID == i.ID {
				continue
			}
			switch other.Provider {
			case "email":
				available = available || u.PasswordHash != "" || s.Config.SMTP.Host != ""
			case "phone":
				available = available || u.PasswordHash != "" || s.Config.SMS.AccessKeyID != ""
			case "google":
				available = available || s.Config.Google.Enabled
			case "github":
				available = available || s.Config.GitHub.Enabled
			}
		}
		if !available {
			return bcode.ErrAccountConflict
		}
		return r.Store.Delete(ctx, i)
	})
}

func (s *Service) SetDisabled(ctx context.Context, p *Principal, userID string, disabled bool) error {
	if !p.User.SystemAdmin || p.User.ID == userID {
		return bcode.ErrForbidden
	}
	return s.Repo.Transaction(ctx, func(r repository.Accounts) error {
		u := &model.User{ID: userID}
		if e := r.Lock(ctx, u); e != nil {
			return e
		}
		if u.SystemAdmin {
			return bcode.ErrForbidden
		}
		if e := r.Update(ctx, u, map[string]interface{}{"disabled": disabled, "securityversion": u.SecurityVersion + 1}); e != nil {
			return e
		}
		return r.Store.DeleteByFilter(ctx, &model.Session{UserID: userID}, nil)
	})
}

func accountConflict(err error) error {
	if errors.Is(err, datastore.ErrRecordExist) {
		return bcode.ErrAccountConflict
	}
	return err
}
func authLookupError(err error) error {
	if errors.Is(err, datastore.ErrRecordNotExist) {
		return bcode.ErrUnauthorized
	}
	return err
}
